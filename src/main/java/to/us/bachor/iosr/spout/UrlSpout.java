package to.us.bachor.iosr.spout;

import static to.us.bachor.iosr.TopologyNames.*;

import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import to.us.bachor.iosr.Settings;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class UrlSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(UrlSpout.class);

	private Jedis jedis;
	private Settings settings = Settings.getSettings();
	private SpoutOutputCollector collector;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		jedis = new Jedis(settings.getProperty(Settings.Key.REDIS_HOST),
				settings.getIntegerProperty(Settings.Key.REDIS_PORT));
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		String url = jedis.lpop(REDIS_URLS_KEY);
		if (url == null) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
			}
		} else {
			collector.emit(new Values(url));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(URL));
	}

}
