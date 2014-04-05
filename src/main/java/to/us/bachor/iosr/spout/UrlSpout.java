package to.us.bachor.iosr.spout;

import static to.us.bachor.iosr.TopologyNames.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import to.us.bachor.iosr.Settings;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

public class UrlSpout implements IBatchSpout {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(UrlSpout.class);

	private Jedis jedis;
	private Settings settings = Settings.getSettings();
	HashMap<Long, List<String>> batches = new HashMap<>();

	@Override
	public void open(Map conf, TopologyContext context) {
		jedis = new Jedis(settings.getProperty(Settings.Key.REDIS_HOST),
				settings.getIntegerProperty(Settings.Key.REDIS_PORT));
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		String url = jedis.lpop(REDIS_URLS_KEY);
		logger.info("next url = " + url);
		if (url == null) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
			}
		} else {
			List<String> batch = new ArrayList<>();
			batch.add(url);
			batches.put(batchId, batch);
			collector.emit(new ArrayList<Object>(batch));
		}

		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
		}
	}

	@Override
	public void ack(long batchId) {
		batches.remove(batchId);
	}

	@Override
	public void close() {
	}

	@Override
	public Map getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		return conf;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(URL);
	}

}
