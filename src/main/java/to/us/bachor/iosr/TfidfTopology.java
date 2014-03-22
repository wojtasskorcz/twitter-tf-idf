package to.us.bachor.iosr;

import to.us.bachor.iosr.spout.TwitterSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class TfidfTopology {

	public static void main(String[] args) throws Exception {
		// please uncomment and run only one of the methods specified below
		runTwitterStreamTopology(args);
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

}
