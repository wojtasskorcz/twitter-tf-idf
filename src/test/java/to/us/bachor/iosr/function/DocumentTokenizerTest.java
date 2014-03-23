package to.us.bachor.iosr.function;

import org.junit.Test;
import org.mockito.Mockito;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class DocumentTokenizerTest {
	private static final String DOCUMENT_FIELD_NAME = "document";
	private static final String SPACE = " ";

	private TridentCollector getTridentCollectorMock() {
		TridentCollector collector = Mockito.mock(TridentCollector.class);
		return collector;
	}

	private void tokenize(TridentCollector collector, String documentContent) {
		TridentTuple tuple = Mockito.mock(TridentTuple.class);
		Mockito.when(tuple.getStringByField(DOCUMENT_FIELD_NAME)).thenReturn(documentContent);
		new DocumentTokenizer().execute(tuple, collector);
	}

	@Test
	public void oneWordDocument() {
		String word = "word";
		TridentCollector collector = Mockito.mock(TridentCollector.class);
		tokenize(collector, word);
		Mockito.verify(collector, Mockito.times(1)).emit(new Values(word));
	}

	@Test
	public void threeWordsWithDuplicateDocument() {
		String word = "word";
		String bicycle = "bicycle";
		String document = word + SPACE + bicycle + SPACE + word;
		TridentCollector collector = Mockito.mock(TridentCollector.class);
		tokenize(collector, document);
		Mockito.verify(collector, Mockito.times(2)).emit(new Values(word));
		Mockito.verify(collector, Mockito.times(1)).emit(new Values(bicycle));
	}

	@Test
	public void stopwordsInDocument() {
		String word = "word";
		String the = "the";
		String document = word + SPACE + the + SPACE + word + SPACE + the;
		TridentCollector collector = Mockito.mock(TridentCollector.class);
		tokenize(collector, document);
		Mockito.verify(collector, Mockito.times(2)).emit(new Values(word));
		Mockito.verify(collector, Mockito.never()).emit(new Values(the));
	}

	@Test
	public void numbersInDocument() {
		String word = "word";
		String number = "15";
		String document = word + SPACE + number;
		TridentCollector collector = Mockito.mock(TridentCollector.class);
		tokenize(collector, document);
		Mockito.verify(collector, Mockito.times(1)).emit(new Values(word));
		Mockito.verify(collector, Mockito.times(1)).emit(new Values(number));
	}
}
