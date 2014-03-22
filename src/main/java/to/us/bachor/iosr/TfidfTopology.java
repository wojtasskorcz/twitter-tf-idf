package to.us.bachor.iosr;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import to.us.bachor.iosr.function.DocumentFetchFunction;
import to.us.bachor.iosr.function.DocumentTokenizer;
import to.us.bachor.iosr.function.TermFilter;
import to.us.bachor.iosr.spout.TwitterSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TfidfTopology {

	private static String[] mimeTypes = new String[] { "text/html", "text/plain" };

	public static void main(String[] args) throws Exception {
		// please uncomment and run only one of the methods specified below
		// runTwitterStreamTopology(args);
		runMockDocumentTopology(args);
	}

	private static void runTwitterStreamTopology(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("iphoneTweetsSpout", new TwitterSpout(new String[] { "iphone" }, 1000));
		Config conf = new Config();
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("twitterStreamTopology", conf, builder.createTopology());
		}
	}

	private static void runMockDocumentTopology(String[] args) throws Exception {
		Config conf = new Config();
		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			TridentTopology topology = buildMockDocumentTopology(drpc);
			cluster.submitTopology("mockDocumentTopology", conf, topology.build());
		}
	}

	private static TridentTopology buildMockDocumentTopology(LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();
		FixedBatchSpout testSpout = new FixedBatchSpout(new Fields("url"), 1, new Values("http://t.co/hP5PM6fm"),
				new Values("http://t.co/xSFteG23"));

		Stream documentStream = topology
				.newStream("tweetSpout", testSpout)
				.parallelismHint(20)
				.each(new Fields("url"), new DocumentFetchFunction(mimeTypes),
						new Fields("document", "documentId", "source"));

		// the commented-out code is not yet propertly implemented; probably will have to replace persistentAggregate
		// with some transient aggregate to avoid using Cassandra for now

		// TridentState dState = documentStream.groupBy(new Fields("source")).persistentAggregate(getStateFactory("d"),
		// new Count(), new Fields("d"));

		Stream termStream = documentStream.parallelismHint(20)
				.each(new Fields("document"), new DocumentTokenizer(), new Fields("dirtyTerm"))
				.each(new Fields("dirtyTerm"), new TermFilter(), new Fields("term"))
				.project(new Fields("term", "documentId", "source"));

		// TridentState dfState = termStream.groupBy(new Fields("term")).persistentAggregate(getStateFactory("df"),
		// new Count(), new Fields("df"));
		//
		// Stream tfidfStream = termStream.groupBy(new Fields("documentId", "term"))
		// .aggregate(new Count(), new Fields("tf"))
		// .each(new Fields("term", "documentId", "tf"), new TfidfExpression(), new Fields("tfidf"));

		// topology.newDRPCStream("dQuery", drpc).each(new Fields("args"), new Split(), new Fields("source"))
		// .stateQuery(dState, new Fields("source"), new MapGet(), new Fields("d_term", "currentD"));
		//
		// topology.newDRPCStream("dfQuery", drpc).each(new Fields("args"), new Split(), new Fields("term"))
		// .stateQuery(dfState, new Fields("term"), new MapGet(), new Fields("currentDf"));

		return topology;
	}
}
