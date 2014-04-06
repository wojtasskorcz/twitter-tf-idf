package to.us.bachor.iosr.spout;

import static to.us.bachor.iosr.TopologyNames.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import to.us.bachor.iosr.db.dao.DocumentDao;
import to.us.bachor.iosr.db.model.Document;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

@SuppressWarnings("rawtypes" /* Storm has no generic types */)
public class UrlSpout implements IBatchSpout {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(UrlSpout.class);

	private DocumentDao documentDao;
	HashMap<Long, List<String>> batches = new HashMap<>();

	@Override
	public void open(Map conf, TopologyContext context) {
		ApplicationContext springContext = new ClassPathXmlApplicationContext("mongoConfiguration.xml");
		documentDao = springContext.getBean(DocumentDao.class);
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		Document document = documentDao.getOldestUnprocessedAndMarkAsProcessed();
		logger.info("next url = " + document);
		if (document == null) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
			}
		} else {
			List<String> batch = new ArrayList<>();
			batch.add(document.getUrl());
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
