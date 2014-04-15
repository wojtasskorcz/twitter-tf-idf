package to.us.bachor.iosr.bolt;

import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import to.us.bachor.iosr.db.dao.DocumentDao;
import to.us.bachor.iosr.db.model.Document;
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

	private DocumentDao documentDao;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		ApplicationContext springContext = new ClassPathXmlApplicationContext("mongoConfiguration.xml");
		documentDao = springContext.getBean(DocumentDao.class);
	}

	@Override
	public void execute(Tuple input) {
		Status ret = (Status) input.getValue(0);
		URLEntity[] urls = ret.getURLEntities();
		for (int i = 0; i < urls.length; i++) {
			logger.debug("saving: " + urls[i].getURL().trim());
			documentDao.saveDocument(new Document(urls[i].getURL().trim(), new Date()));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
