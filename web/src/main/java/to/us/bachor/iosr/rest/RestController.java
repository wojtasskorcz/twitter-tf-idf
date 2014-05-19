package to.us.bachor.iosr.rest;

import static to.us.bachor.iosr.TopologyNames.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

	@RequestMapping(value = "frequencies/{term}", method = RequestMethod.GET)
	public @ResponseBody
	ResponseEntity<List<String>> getFrequencies(@PathVariable String term, HttpServletRequest request)
			throws TException, DRPCExecutionException {
		String stemmedTerm = MorphaStemmer.stemToken(term);
		Collection<Document> documentsToQuery = termDao.getDocumentsContainingTerm(stemmedTerm);
		Set<Document> uniqueDocumentsToQuery = new HashSet<>(documentsToQuery);
		List<String> result = new ArrayList<>();
		DRPCClient client = new DRPCClient("127.0.0.1", 3772);
		for (Document document : uniqueDocumentsToQuery) {
			result.add(client.execute(TF_IDF_QUERY, document.getUrl() + " " + stemmedTerm));
		}
		return new ResponseEntity<List<String>>(result, HttpStatus.OK);
	}

	@RequestMapping(value = "frequencies/document", method = RequestMethod.GET)
	public @ResponseBody
	ResponseEntity<List<String>> getFrequencies(@RequestParam("url") String url) throws TException,
			DRPCExecutionException {
		Document documentByUrl = documentDao.getDocumentByUrl(url);
		List<String> result = new ArrayList<>();
		if (documentByUrl == null) {
			logger.info("Cannot find document with url: " + url);
		} else {
			Collection<Term> termsFromDocument = termDao.getTermsFromDocument(documentByUrl);
			DRPCClient client = new DRPCClient("127.0.0.1", 3772);
			for (Term term : termsFromDocument) {
				result.add(client.execute(TF_IDF_QUERY, documentByUrl.getUrl() + " " + term.getTerm()));
			}
		}
		return new ResponseEntity<List<String>>(result, HttpStatus.OK);
	}
}
