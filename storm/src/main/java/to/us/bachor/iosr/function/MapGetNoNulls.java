package to.us.bachor.iosr.function;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

@SuppressWarnings({ "unchecked", "rawtypes" } /* Storm has no generic types */)
public class MapGetNoNulls extends BaseQueryFunction<ReadOnlyMapState, Object> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<Object> batchRetrieve(ReadOnlyMapState map, List<TridentTuple> keys) {
		return map.multiGet((List) keys);
	}

	@Override
	public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
		if (result == null) {
			collector.emit(new Values(0L));
		} else {
			collector.emit(new Values(result));
		}
	}

}
