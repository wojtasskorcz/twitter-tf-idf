package to.us.bachor.iosr.rest;

import static to.us.bachor.iosr.TopologyNames.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

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
import org.springframework.web.bind.annotation.ResponseBody;

import to.us.bachor.iosr.db.dao.DocumentDao;
import to.us.bachor.iosr.db.model.Document;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.utils.DRPCClient;

@Controller
@RequestMapping("/")
public class RestController {

	private DocumentDao documentDao;

	@PostConstruct
	private void startToplogy() throws AlreadyAliveException, InvalidTopologyException {
		// drpc = TfIdfRunner.runTopology();
		ApplicationContext springContext = new ClassPathXmlApplicationContext("mongoConfiguration.xml");
		documentDao = springContext.getBean(DocumentDao.class);
	}

	private static final Logger logger = Logger.getLogger(RestController.class);

	@RequestMapping(value = "frequencies/{term}", method = RequestMethod.GET)
	public @ResponseBody
	ResponseEntity<List<String>> getFrequencies(@PathVariable String term, HttpServletRequest request)
			throws TException, DRPCExecutionException {
		Collection<Document> documentsToQuery = documentDao.getAllProcessedDocumentsAfterDate(new Date(1991, 1, 1));
		List<String> result = new ArrayList<>();
		DRPCClient client = new DRPCClient("127.0.0.1", 3772);
		for (Document document : documentsToQuery) {
			result.add(client.execute(TF_IDF_QUERY, document.getUrl() + " " + term));
		}
		return new ResponseEntity<List<String>>(result, HttpStatus.OK);
	}

}
