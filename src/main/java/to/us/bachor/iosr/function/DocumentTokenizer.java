package to.us.bachor.iosr.function;

import static to.us.bachor.iosr.TopologyNames.*;

import java.io.IOException;
import java.io.StringReader;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import edu.washington.cs.knowitall.morpha.MorphaStemmer;

public class DocumentTokenizer extends BaseFunction {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(DocumentTokenizer.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String documentContents = tuple.getStringByField(DOCUMENT);
		logger.info("Tokenizing: " + tuple.getStringByField(URL));
		TokenStream ts = null;
		try {
			ts = new StopFilter(Version.LUCENE_30, new StandardTokenizer(Version.LUCENE_30, new StringReader(
					documentContents)), StopAnalyzer.ENGLISH_STOP_WORDS_SET);
			CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
			while (ts.incrementToken()) {
				String lemma = MorphaStemmer.stemToken(termAtt.toString());
				lemma = lemma.trim().replaceAll("\n", "").replaceAll("\r", "");
				collector.emit(new Values(lemma));
			}
			ts.close();
		} catch (IOException e) {
			logger.error("Error when tokenizing a document", e);
		} finally {
			if (ts != null) {
				try {
					ts.close();
				} catch (IOException e) {
				}
			}
		}
		logger.debug("tokenizer ends");
	}
}
