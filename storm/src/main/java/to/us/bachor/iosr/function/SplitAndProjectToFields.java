package to.us.bachor.iosr.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class SplitAndProjectToFields extends BaseFunction {

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Values vals = new Values();
		for (String word : tuple.getString(0).split(" ")) {
			if (word.length() > 0) {
				vals.add(word);
			}
		}
		collector.emit(vals);
	}

}
