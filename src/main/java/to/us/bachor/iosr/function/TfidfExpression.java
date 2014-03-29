package to.us.bachor.iosr.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TfidfExpression extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			// double d = (double) tuple.getLongByField("d");
			// double df = (double) tuple.getLongByField("df");
			double tf = (double) tuple.getLongByField("tf");
			String term = (String) tuple.getStringByField("term");
			System.out.println(String.format("%s\ttf=%s", term, tf));
			// System.out.println("d=" + d + " df=" + df + " tf=" + tf);
			// double tfidf = tf * Math.log(d / (1 + df));
			// System.out.println("Emitting new TFIDF(term,Document): (" + tuple.getStringByField("term") + ","
			// + tuple.getStringByField("documentId") + ") = " + tfidf);
			// collector.emit(new Values(tfidf));
		} catch (Exception e) {
			System.err.println(e);
		}

	}

}
