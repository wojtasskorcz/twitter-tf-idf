package to.us.bachor.iosr.function;

import org.junit.Test;
import org.mockito.Mockito;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class DocumentTokenizerTest {

	@Test
	public void oneWordDocument() {
		String word = "word";
		TridentTuple tuple = Mockito.mock(TridentTuple.class);
		Mockito.when(tuple.getStringByField("document")).thenReturn(word);
		TridentCollector collector = Mockito.mock(TridentCollector.class);
		new DocumentTokenizer().execute(tuple, collector);
		Mockito.verify(collector, Mockito.times(1)).emit(new Values(word));
	}
}
