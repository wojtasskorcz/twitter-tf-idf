package to.us.bachor.iosr;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import to.us.bachor.iosr.function.DocumentFetchFunction;
import to.us.bachor.iosr.function.DocumentTokenizer;
import to.us.bachor.iosr.function.NullToZeroFunction;
import to.us.bachor.iosr.function.TermFilter;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Builds a Topology that receives a stream of links to documents, fetches content of those documents, stems and filters
 * dictionary words from those documents and then computes D, DF and TF factors of tf-idf. Then submits this Topology to
 * Storm.
 * 
 * Not yet finished: Should use Redis values gathered by TwitterScraperRunner. Uses a mock FixedBatchSpout so far.
 * Computing of D, DF and TF not yet implemented.
 */
public class TfidfRunner {

	private static String[] mimeTypes = new String[] { "text/html", "text/plain" };

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			TridentTopology topology = buildMockDocumentTopology(drpc);
			cluster.submitTopology("mockDocumentTopology", conf, topology.build());
			for (int i = 0; i < 100; i++) {
				System.out.println("dQuery(twitter): " + drpc.execute("dQuery", "twitter"));
				System.out.println("dfQuery" + drpc.execute("dfQuery", "have your dupa"));
				Thread.sleep(1000);
			}
		}
	}

	private static TridentTopology buildMockDocumentTopology(LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();
		// FixedBatchSpout testSpout = new FixedBatchSpout(new Fields("url"), 1, new Values("http://t.co/hP5PM6fm"),
		// new Values("http://t.co/xSFteG23"));

		// emits: url
		FixedBatchSpout testSpout = new FixedBatchSpout(new Fields("url"), 1, new Values("http://t.co/hP5PM6fm"));

		// gets: url
		// emits: url, document (actual content), documentId (trimmed url), source (here: "twitter")
		Stream documentStream = topology //
				.newStream("tweetSpout", testSpout) //
				.parallelismHint(20) //
				.each(new Fields("url"), new DocumentFetchFunction(mimeTypes),
						new Fields("document", "documentId", "source"));

		// gets: url, document (actual content), documentId (trimmed url), source (here: "twitter")
		// emits: term (lemmatized), documentId (trimmed url), source (here: "twitter")
		Stream termStream = documentStream //
				.parallelismHint(20) //
				.each(new Fields("document"), new DocumentTokenizer(), new Fields("dirtyTerm")) //
				.each(new Fields("dirtyTerm"), new TermFilter(), new Fields("term")) //
				.project(new Fields("term", "documentId", "source"));

		// gets: url, document (actual content), documentId (trimmed url), source (here: "twitter")
		TridentState dState = documentStream //
				.groupBy(new Fields("source")) //
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("d"));

		// gets: term (lemmatized), documentId (trimmed url), source (here: "twitter")
		TridentState dfState = termStream //
				.groupBy(new Fields("term")) //
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("df"));

		// gets: args (space-separated list of sources)
		// returns: source, d (the D factor of tf-idf for this source)
		topology.newDRPCStream("dQuery", drpc) //
				.each(new Fields("args"), new Split(), new Fields("source")) //
				.stateQuery(dState, new Fields("source"), new MapGet(), new Fields("tmpD")) //
				.each(new Fields("tmpD"), new NullToZeroFunction(), new Fields("d")) //
				.project(new Fields("source", "d"));

		// gets: args (space-separated list of terms)
		// returns: term, df (the DF factor of tf-idf for this term)
		topology.newDRPCStream("dfQuery", drpc) //
				.each(new Fields("args"), new Split(), new Fields("term")) //
				.stateQuery(dfState, new Fields("term"), new MapGet(), new Fields("tmpDf")) //
				.each(new Fields("tmpDf"), new NullToZeroFunction(), new Fields("df")) //
				.project(new Fields("term", "df"));

		// Stream tfidfStream = termStream.groupBy(new Fields("documentId", "term"))
		// .aggregate(new Count(), new Fields("tf"))
		// .each(new Fields("term", "documentId", "tf"), new TfidfExpression(), new Fields("tfidf"));

		return topology;
	}
}
