package to.us.bachor.iosr;

import static to.us.bachor.iosr.TopologyNames.*;

import java.sql.Date;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import storm.trident.TridentTopology;
import to.us.bachor.iosr.db.dao.DocumentDao;
import to.us.bachor.iosr.db.model.Document;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;

/**
 * Builds a Topology that receives a stream of links to documents, fetches content of those documents, stems and filters
 * dictionary words from those documents and then computes D, DF and TF factors of tf-idf. Then submits this Topology to
 * Storm.
 * 
 * Not yet finished: Should use Redis values gathered by TwitterScraperRunner. Uses
 * 
 * a mock FixedBatchSpout so far. Computing of D, DF and TF not yet implemented.
 */

public class TfIdfRunner {

	private static final Logger logger = Logger.getLogger(TfIdfRunner.class);

	public static LocalDRPC runTopology() {
		Config conf = new Config();
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		TridentTopology tfIdfToplogy = new TfIdfToplogyCreator().createTfIdfToplogy(drpc);
		cluster.submitTopology(MOCK_DOCUMENT_TOPOLOGY, conf, tfIdfToplogy.build());
		cluster.submitTopology(TWITTER_STREAM_TOPOLOGY, conf,
				new TwitterScraperTopologyCreator().createTwitterScraperToplogy());
		return drpc;
	}

	public static void main(String[] args) throws InterruptedException {

		LocalDRPC drpc = runTopology();
		ApplicationContext springContext = new ClassPathXmlApplicationContext("mongoConfiguration.xml");
		DocumentDao documentDao = springContext.getBean(DocumentDao.class);
		String word = "have";
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
