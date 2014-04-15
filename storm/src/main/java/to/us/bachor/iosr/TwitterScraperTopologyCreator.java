package to.us.bachor.iosr;

import static to.us.bachor.iosr.TopologyNames.*;
import to.us.bachor.iosr.bolt.PublishURLBolt;
import to.us.bachor.iosr.spout.TwitterSpout;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * Builds a Topology that connects to Twiter Streaming API, receives tweets with the specified keywords, extracts links
 * to documents from them and saves those links in Redis under the key "urls". Then submits this Topology to Storm.
 * 
 * The extracted links are not filtered out in regard to duplicates. Perhaps TODO.
 */
public class TwitterScraperTopologyCreator {

	private static String[] keywords = { "reddit" };

	public StormTopology createTwitterScraperToplogy() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(TWITTER_SPOUT, new TwitterSpout(keywords, 1000));
		builder.setBolt(PUBLISH_URL_BOLT, new PublishURLBolt(), 2).shuffleGrouping(TWITTER_SPOUT);
		return builder.createTopology();
	}

}
