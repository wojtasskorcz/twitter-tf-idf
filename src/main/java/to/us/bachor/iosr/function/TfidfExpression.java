package to.us.bachor.iosr.function;

import org.apache.log4j.Logger;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class TfidfExpression extends BaseFunction {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(TfidfExpression.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			double d = (double) tuple.getLongByField("d");
			double df = (double) tuple.getLongByField("df");
			double tf = (double) tuple.getLongByField("tf");
			String term = (String) tuple.getStringByField("term");
			logger.info(String.format("d=%s, df=%s, tf=%s, term=%s", d, df, tf, term));
			double tfidf = tf * Math.log(d / (1 + df));
			collector.emit(new Values(tfidf));
		} catch (Exception e) {
			logger.error("Error when calculating tf-idf", e);
		}

	}

}
