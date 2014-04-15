package to.us.bachor.iosr.function;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import to.us.bachor.iosr.TopologyNames;

public final class RemoveDuplicatesFilter implements Filter {
	private static final long serialVersionUID = 1L;
	private Set<Integer> hashes;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		hashes = new HashSet<>();
	}

	@Override
	public void cleanup() {
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		String documentId = tuple.getStringByField(TopologyNames.DOCUMENT_ID);
		String term = tuple.getStringByField(TopologyNames.TERM);
		Integer hash = 31 * documentId.hashCode() + term.hashCode();
		if (hashes.contains(hash)) {
			return false;
		} else {
			hashes.add(hash);
			return true;
		}

	}
}