package to.us.bachor.iosr;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomNumberSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private Random random;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.random = new Random();
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		collector.emit(new Values(random.nextInt(Constants.MAX_RANDOM)));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Constants.RANDOM_NUMBER));
	}

}
