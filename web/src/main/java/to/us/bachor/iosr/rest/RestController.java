package to.us.bachor.iosr.rest;

import static to.us.bachor.iosr.TopologyNames.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.apache.thrift7.TException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import to.us.bachor.iosr.db.dao.DocumentDao;
import to.us.bachor.iosr.db.dao.TermDao;
import to.us.bachor.iosr.db.model.Document;
import to.us.bachor.iosr.db.model.Term;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.utils.DRPCClient;
import edu.washington.cs.knowitall.morpha.MorphaStemmer;

@Controller
@RequestMapping("/")
public class RestController {

	private DocumentDao documentDao;
	private TermDao termDao;

	@PostConstruct
	private void startToplogy() throws AlreadyAliveException, InvalidTopologyException {
		// drpc = TfIdfRunner.runTopology();
		ApplicationContext springContext = new ClassPathXmlApplicationContext("mongoConfiguration.xml");
		documentDao = springContext.getBean(DocumentDao.class);
		termDao = springContext.getBean(TermDao.class);
	}

	private static final Logger logger = Logger.getLogger(RestController.class);

	private <T> List<List<T>> splitListEvenly(List<T> list, int parts) {
		List<List<T>> result = new ArrayList<>();
		for (int i = 0; i < parts; ++i) {
			result.add(new ArrayList<T>());
		}
		for (int i = 0; i < list.size(); ++i) {
			result.get(i % parts).add(list.get(i));
		}
		return result;
	}

	private class DRPCQueriesExecutor implements Callable<List<String>> {

		private List<String> queries;
		private String function;

		public DRPCQueriesExecutor(String function, List<String> queries) {
			this.queries = queries;
			this.function = function;
		}

		@Override
		public List<String> call() throws Exception {
			DRPCClient client = new DRPCClient("127.0.0.1", 3772);
			List<String> result = new ArrayList<>();
			for (String query : queries) {
				result.add(client.execute(function, query));
			}
			return result;
		}
	}

	private List<String> executeDRPCQueriesInParallel(String function, List<String> queries)
			throws InterruptedException {
		List<String> result = new ArrayList<>();

		int threads = Runtime.getRuntime().availableProcessors();
		List<List<String>> parts = splitListEvenly(queries, threads);
		List<Callable<List<String>>> tasks = new ArrayList<>();
		for (List<String> part : parts) {
			tasks.add(new DRPCQueriesExecutor(function, part));
		}
		ExecutorService executorService = Executors.newFixedThreadPool(threads);
		List<Future<List<String>>> futures = executorService.invokeAll(tasks);
		for (Future<List<String>> future : futures) {
			try {
				result.addAll(future.get());
			} catch (ExecutionException e) {
				logger.error(e);
			}
		}
		return result;
	}

	@RequestMapping(value = "frequencies/{term}", method = RequestMethod.GET)
	public @ResponseBody
	ResponseEntity<List<String>> getFrequencies(@PathVariable String term, HttpServletRequest request)
			throws TException, DRPCExecutionException, InterruptedException {
		String stemmedTerm = MorphaStemmer.stemToken(term);
		Collection<Document> documentsToQuery = termDao.getDocumentsContainingTerm(stemmedTerm);
		Set<Document> uniqueDocumentsToQuery = new HashSet<>(documentsToQuery);
		List<String> queries = new ArrayList<>();
		for (Document document : uniqueDocumentsToQuery) {
			queries.add(document.getUrl() + " " + stemmedTerm);
		}
		List<String> result = executeDRPCQueriesInParallel(TF_IDF_QUERY, queries);
		return new ResponseEntity<List<String>>(result, HttpStatus.OK);
	}

	@RequestMapping(value = "frequencies/document", method = RequestMethod.GET)
	public @ResponseBody
	ResponseEntity<List<String>> getFrequencies(@RequestParam("url") String url) throws TException,
			DRPCExecutionException, InterruptedException {
		List<String> result = new ArrayList<>();
		Document documentByUrl = documentDao.getDocumentByUrl(url);
		if (documentByUrl == null) {
			logger.info("Cannot find document with url: " + url);
		} else {
			Collection<Term> termsFromDocument = termDao.getTermsFromDocument(documentByUrl);
			List<String> queries = new ArrayList<>();
			for (Term term : termsFromDocument) {
				queries.add(documentByUrl.getUrl() + " " + term.getTerm());
			}
			result = executeDRPCQueriesInParallel(TF_IDF_QUERY, queries);
		}
		return new ResponseEntity<List<String>>(result, HttpStatus.OK);
	}
}
