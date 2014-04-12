package to.us.bachor.iosr;

import static to.us.bachor.iosr.TopologyNames.*;

import java.util.Collection;
import java.util.Date;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import storm.trident.TridentTopology;
import to.us.bachor.iosr.db.dao.DocumentDao;
import to.us.bachor.iosr.db.model.Document;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

public class TfIdfLocalRunner {

	private static final Logger logger = Logger.getLogger(TfIdfRunner.class);

	public static LocalDRPC runTopology() throws AlreadyAliveException, InvalidTopologyException {
		Config conf = new Config();
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		TridentTopology tfIdfTopology = new TfIdfToplogyCreator().createTfIdfTopology(drpc);

		cluster.submitTopology(MOCK_DOCUMENT_TOPOLOGY, conf, tfIdfTopology.build());
		cluster.submitTopology(TWITTER_STREAM_TOPOLOGY, conf,
				new TwitterScraperTopologyCreator().createTwitterScraperToplogy());
		return drpc;
	}

	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
		LocalDRPC drpc = runTopology();
		ApplicationContext springContext = new ClassPathXmlApplicationContext("mongoConfiguration.xml");
		DocumentDao documentDao = springContext.getBean(DocumentDao.class);
		String word = "have";
		// watch out, so far Mongo support is disabled and a mock FixedBatchSpout is used
		while (true) {
			Thread.sleep(5000);
			logger.debug("---Querying for the word '" + word + "' in all processed documents.");
			Collection<Document> documentsToQuery = documentDao.getAllProcessedDocumentsAfterDate(new Date(1991, 1, 1));
			for (Document document : documentsToQuery) {
				logger.debug(drpc.execute(TF_IDF_QUERY, document.getUrl() + " " + word));
			}
			logger.debug("---Querying end---");
		}
	}

}
