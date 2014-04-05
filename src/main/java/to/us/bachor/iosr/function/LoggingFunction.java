package to.us.bachor.iosr.function;

import org.apache.log4j.Logger;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class LoggingFunction extends BaseFunction {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(LoggingFunction.class);

	private String tag;

	public LoggingFunction(String tag) {
		this.tag = tag;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		logger.debug("[" + tag + "] " + tuple);
		collector.emit(new Values());
	}

}