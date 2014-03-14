package to.us.bachor.iosr;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CountingBolt extends BaseRichBolt {

	private Map<Integer, Integer> counts;
	private OutputCollector collector;
	private int taskId;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		counts = new HashMap<Integer, Integer>();
		this.collector = collector;
		taskId = context.getThisTaskId();
	}

	@Override
	public void execute(Tuple input) {
		Integer randomNumber = (Integer) input.getValueByField(Constants.RANDOM_NUMBER);
		Integer currentCount = counts.get(randomNumber);
		if (currentCount == null) {
			currentCount = 0;
		}
		currentCount++;
		counts.put(randomNumber, currentCount);
		Values tuple = new Values(randomNumber, currentCount);
		collector.emit(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Constants.NUMBER, Constants.COUNT));
	}

}
