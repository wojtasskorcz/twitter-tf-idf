package to.us.bachor.iosr.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class TfidfExpression extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			double d = (double) tuple.getLongByField("d");
			double df = (double) tuple.getLongByField("df");
			double tf = (double) tuple.getLongByField("tf");
			String term = (String) tuple.getStringByField("term");
			System.out.println(String.format("d=%s, df=%s, tf=%s, term=%s", d, df, tf, term));
			double tfidf = tf * Math.log(d / (1 + df));
			collector.emit(new Values(tfidf));
		} catch (Exception e) {
			System.err.println(e);
		}

	}

}
