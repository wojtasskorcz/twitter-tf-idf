package to.us.bachor.iosr;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class RandomCountingTopology {

	public void runTopologyLocally() {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("randomNumber", new RandomNumberSpout(), 5);
		builder.setBolt("counting", new CountingBolt(), 5).fieldsGrouping("randomNumber",
				new Fields(Constants.RANDOM_NUMBER));

		Config conf = new Config();
		conf.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(Constants.TOPOLOGY_RUN_TIME);
		cluster.killTopology("test");
		cluster.shutdown();
	}
}
