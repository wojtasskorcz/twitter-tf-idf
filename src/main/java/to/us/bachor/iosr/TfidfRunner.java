package to.us.bachor.iosr;

import static to.us.bachor.iosr.TopologyNames.ARGS;
import static to.us.bachor.iosr.TopologyNames.DF_TERM;
import static to.us.bachor.iosr.TopologyNames.DIRTY_TERM;
import static to.us.bachor.iosr.TopologyNames.DOCUMENT;
import static to.us.bachor.iosr.TopologyNames.DOCUMENT_ID;
import static to.us.bachor.iosr.TopologyNames.D_TERM;
import static to.us.bachor.iosr.TopologyNames.MOCK_DOCUMENT_TOPOLOGY;
import static to.us.bachor.iosr.TopologyNames.SOURCE;
import static to.us.bachor.iosr.TopologyNames.TERM;
import static to.us.bachor.iosr.TopologyNames.TF_IDF_QUERY;
import static to.us.bachor.iosr.TopologyNames.TF_IDF_RESULT;
import static to.us.bachor.iosr.TopologyNames.TF_TERM;
import static to.us.bachor.iosr.TopologyNames.TWITTER_SOURCE;
import static to.us.bachor.iosr.TopologyNames.URL;
import static to.us.bachor.iosr.TopologyNames.URL_SPOUT;

import java.util.ArrayList;
import java.util.Arrays;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import to.us.bachor.iosr.function.AddSourceField;
import to.us.bachor.iosr.function.DocumentFetchFunction;
import to.us.bachor.iosr.function.DocumentTokenizer;
import to.us.bachor.iosr.function.MapGetNoNulls;
import to.us.bachor.iosr.function.SplitAndProjectToFields;
import to.us.bachor.iosr.function.TermFilter;
import to.us.bachor.iosr.function.TfidfExpression;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;

/**
 * Builds a Topology that receives a stream of links to documents, fetches content of those documents, stems and filters
 * dictionary words from those documents and then computes D, DF and TF factors of tf-idf. Then submits this Topology to
 * Storm.
 * 
 * Not yet finished: Should use Redis values gathered by TwitterScraperRunner. Uses
 * 
 * a mock FixedBatchSpout so far. Computing of D, DF and TF not yet implemented.
 */

@SuppressWarnings("unchecked" /* Storm has no generic types */)
public class TfidfRunner {

	private static String[] mimeTypes = new String[] { "text/html", "text/plain" };
	private static String[] urls = new String[] { "http://t.co/hP5PM6fm"/* , "http://t.co/xSFteG23" */};

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			TridentTopology topology = buildMockDocumentTopology(drpc);
			cluster.submitTopology(MOCK_DOCUMENT_TOPOLOGY, conf, topology.build());
			for (int i = 0; i < 100; i++) {
				System.out.println(TF_IDF_QUERY + " " + drpc.execute(TF_IDF_QUERY, urls[0] + " have"));
				Thread.sleep(1000);
			}
		}
	}

	private static TridentTopology buildMockDocumentTopology(LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();

		// emits: url
		FixedBatchSpout testSpout = new FixedBatchSpout(new Fields(URL), 1, new ArrayList<Object>(Arrays.asList(urls)));

		/* ================================ streams ================================ */

		// gets: url
		// emits: url, document (actual content), documentId (document's url), source (here: "twitter")
		Stream documentStream = topology //
				.newStream(URL_SPOUT, testSpout) //
				.parallelismHint(20) //
				.each(new Fields(URL), new DocumentFetchFunction(mimeTypes), new Fields(DOCUMENT, DOCUMENT_ID, SOURCE));

		// gets: url, document (actual content), documentId (document's url), source (here: "twitter")
		// emits: term (lemmatized), documentId (document's url), source (here: "twitter")
		Stream termStream = documentStream //
				.parallelismHint(20) //
				.each(new Fields(DOCUMENT), new DocumentTokenizer(), new Fields(DIRTY_TERM)) //
				.each(new Fields(DIRTY_TERM), new TermFilter(), new Fields(TERM)) //
				.project(new Fields(TERM, DOCUMENT_ID, SOURCE));

		/* ================================ states ================================ */

		// gets: url, document (actual content), documentId (document's url), source (here: "twitter")
		// contains: d (total number of documents from this source)
		TridentState dState = documentStream //
				.groupBy(new Fields(SOURCE)) //
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields(D_TERM));

		// gets: term (lemmatized), documentId (document's url), source (here: "twitter")
		// contains: df (number of appearances of the term in all documents)
		TridentState dfState = termStream //
				.groupBy(new Fields(TERM)) //
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields(DF_TERM));

		// gets: documentId (document's url), term (lemmatized)
		// contains: tf (number of appearances of the term in the document)
		TridentState tfState = termStream //
				.groupBy(new Fields(DOCUMENT_ID, TERM)) //
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields(TF_TERM));

		/* ================================ DRPC streams ================================ */

		// gets: args (a string in form <documentId><space><term>)
		// returns: documentId (document's url), term, tfidf
		topology.newDRPCStream(TF_IDF_QUERY, drpc)
				.each(new Fields(ARGS), new SplitAndProjectToFields(), new Fields(DOCUMENT_ID, TERM))
				.each(new Fields(), new AddSourceField(TWITTER_SOURCE), new Fields(SOURCE))
				.stateQuery(dState, new Fields(SOURCE), new MapGetNoNulls(), new Fields(D_TERM))
				.stateQuery(dfState, new Fields(TERM), new MapGetNoNulls(), new Fields(DF_TERM))
				.stateQuery(tfState, new Fields(DOCUMENT_ID, TERM), new MapGetNoNulls(), new Fields(TF_TERM))
				.each(new Fields(TERM, DOCUMENT_ID, D_TERM, DF_TERM, TF_TERM), new TfidfExpression(),
						new Fields(TF_IDF_RESULT)) //
				.project(new Fields(DOCUMENT_ID, TERM, TF_IDF_RESULT));

		return topology;
	}
}
