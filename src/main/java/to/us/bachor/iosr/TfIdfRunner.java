package to.us.bachor.iosr;

import static to.us.bachor.iosr.TopologyNames.*;
import storm.trident.TridentTopology;
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

	public static void main(String[] args) {
		Config conf = new Config();
		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			TridentTopology tfIdfToplogy = new TfIdfToplogyCreator().createTfIdfToplogy(drpc);
			cluster.submitTopology(MOCK_DOCUMENT_TOPOLOGY, conf, tfIdfToplogy.build());
			cluster.submitTopology(TWITTER_STREAM_TOPOLOGY, conf,
					new TwitterScraperTopologyCreator().createTwitterScraperToplogy());
		}
	}

}
