package to.us.bachor.iosr;

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
public class TfidfRunner {

	private static String[] mimeTypes = new String[] { "text/html", "text/plain" };
	private static String[] urls = new String[] { "http://t.co/hP5PM6fm"/* , "http://t.co/xSFteG23" */};

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			TridentTopology topology = buildMockDocumentTopology(drpc);
			cluster.submitTopology("mockDocumentTopology", conf, topology.build());
			for (int i = 0; i < 100; i++) {
				System.out.println("tfidfQuery " + drpc.execute("tfidfQuery", urls[0] + " have"));
				Thread.sleep(1000);
			}
		}
	}

	private static TridentTopology buildMockDocumentTopology(LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();

		// emits: url
		FixedBatchSpout testSpout = new FixedBatchSpout(new Fields("url"), 1,
				new ArrayList<Object>(Arrays.asList(urls)));
		testSpout.setCycle(true);

		/* ================================ streams ================================ */

		// gets: url
		// emits: url, document (actual content), documentId (document's url), source (here: "twitter")
		Stream documentStream = topology //
				.newStream("tweetSpout", testSpout) //
				.parallelismHint(20) //
				.each(new Fields("url"), new DocumentFetchFunction(mimeTypes),
						new Fields("document", "documentId", "source"));

		// gets: url, document (actual content), documentId (document's url), source (here: "twitter")
		// emits: term (lemmatized), documentId (document's url), source (here: "twitter")
		Stream termStream = documentStream //
				.parallelismHint(20) //
				.each(new Fields("document"), new DocumentTokenizer(), new Fields("dirtyTerm")) //
				.each(new Fields("dirtyTerm"), new TermFilter(), new Fields("term")) //
				.project(new Fields("term", "documentId", "source"));

		/* ================================ states ================================ */

		// gets: url, document (actual content), documentId (document's url), source (here: "twitter")
		// contains: d (total number of documents from this source)
		TridentState dState = documentStream //
				.groupBy(new Fields("source")) //
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("d"));

		// gets: term (lemmatized), documentId (document's url), source (here: "twitter")
		// contains: df (number of appearances of the term in all documents)
		TridentState dfState = termStream //
				.groupBy(new Fields("term")) //
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("df"));

		// gets: documentId (document's url), term (lemmatized)
		// contains: tf (number of appearances of the term in the document)
		TridentState tfState = termStream //
				.groupBy(new Fields("documentId", "term")) //
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("tf"));

		/* ================================ DRPC streams ================================ */

		// gets: args (a string in form <documentId><space><term>)
		// returns: documentId (document's url), term, tfidf
		topology.newDRPCStream("tfidfQuery", drpc)
				.each(new Fields("args"), new SplitAndProjectToFields(), new Fields("documentId", "term"))
				.each(new Fields(), new AddSourceField("twitter"), new Fields("source"))
				.stateQuery(dState, new Fields("source"), new MapGetNoNulls(), new Fields("d"))
				.stateQuery(dfState, new Fields("term"), new MapGetNoNulls(), new Fields("df"))
				.stateQuery(tfState, new Fields("documentId", "term"), new MapGetNoNulls(), new Fields("tf"))
				.each(new Fields("term", "documentId", "d", "df", "tf"), new TfidfExpression(), new Fields("tfidf")) //
				.project(new Fields("documentId", "term", "tfidf"));

		return topology;
	}
}
