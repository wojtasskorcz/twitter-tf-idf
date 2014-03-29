package to.us.bachor.iosr.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class NullToZeroFunction extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Long val = tuple.getLong(0);
		if (val == null) {
			collector.emit(new Values(0));
		} else {
			collector.emit(new Values(val));
		}
	}

}
