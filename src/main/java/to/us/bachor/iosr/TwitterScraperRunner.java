package to.us.bachor.iosr;

import to.us.bachor.iosr.bolt.PublishURLBolt;
import to.us.bachor.iosr.spout.TwitterSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * Builds a Topology that connects to Twiter Streaming API, receives tweets with the specified keywords, extracts links
 * to documents from them and saves those links in Redis under the key "urls". Then submits this Topology to Storm.
 * 
 * The extracted links are not filtered out in regard to duplicates. Perhaps TODO.
 */
public class TwitterScraperRunner {

	private static String[] keywords = { "iphone" };

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitterSpout", new TwitterSpout(keywords, 1000));
		builder.setBolt("publishBolt", new PublishURLBolt(), 2).shuffleGrouping("twitterSpout");
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
