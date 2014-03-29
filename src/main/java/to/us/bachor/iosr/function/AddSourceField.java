package to.us.bachor.iosr.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class AddSourceField extends BaseFunction {
	private String source;

	public AddSourceField(String source) {
		this.source = source;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		collector.emit(new Values(source));
	}

}