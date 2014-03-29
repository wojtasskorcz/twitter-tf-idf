package to.us.bachor.iosr.bolt;

import java.util.Map;

import redis.clients.jedis.Jedis;
import to.us.bachor.iosr.Config;
import twitter4j.Status;
import twitter4j.URLEntity;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PublishURLBolt extends BaseRichBolt {

	private Jedis jedis;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		jedis = new Jedis("localhost");
	}

	@Override
	public void execute(Tuple input) {
		Status ret = (Status) input.getValue(0);
		URLEntity[] urls = ret.getURLEntities();
		for (int i = 0; i < urls.length; i++) {
			System.out.println("saving: " + urls[i].getURL().trim());
			jedis.rpush(Config.REDIS_URLS_KEY, urls[i].getURL().trim());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
