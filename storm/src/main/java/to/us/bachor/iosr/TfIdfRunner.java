package to.us.bachor.iosr;

import static to.us.bachor.iosr.TopologyNames.*;

import org.apache.log4j.Logger;

import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

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

	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
		TridentTopology tfIdfTopology = new TfIdfToplogyCreator().createTfIdfTopology();
		StormSubmitter.submitTopology(MOCK_DOCUMENT_TOPOLOGY, new Config(), tfIdfTopology.build());
	}
}
