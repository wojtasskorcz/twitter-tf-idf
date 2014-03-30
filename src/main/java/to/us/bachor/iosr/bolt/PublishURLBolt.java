package to.us.bachor.iosr.bolt;

import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import to.us.bachor.iosr.Settings;
import twitter4j.Status;
import twitter4j.URLEntity;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("rawtypes" /* Storm has no generic types */)
public class PublishURLBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(PublishURLBolt.class);

	private Jedis jedis;
	private Settings settings = Settings.getSettings();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		jedis = new Jedis(settings.getProperty(Settings.Key.REDIS_HOST),
				settings.getIntegerProperty(Settings.Key.REDIS_PORT));
	}

	@Override
	public void execute(Tuple input) {
		Status ret = (Status) input.getValue(0);
		URLEntity[] urls = ret.getURLEntities();
		for (int i = 0; i < urls.length; i++) {
			logger.debug("saving: " + urls[i].getURL().trim());
			jedis.rpush(Settings.REDIS_URLS_KEY, urls[i].getURL().trim());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
