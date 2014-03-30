package to.us.bachor.iosr.function;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.spell.PlainTextDictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

@SuppressWarnings("rawtypes" /* Storm has no generic types */)
public class TermFilter extends BaseFunction {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(TermFilter.class);

	private SpellChecker spellchecker;
	private List<String> filterTerms = Arrays.asList(new String[] { "http" });

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		File dir = new File(System.getProperty("user.home") + "/dictionaries");
		Directory directory;
		try {
			directory = FSDirectory.open(dir);
			spellchecker = new SpellChecker(directory);
			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
			URL dictionaryFile = TermFilter.class.getResource("/fulldictionary00.txt");
			spellchecker.indexDictionary(new PlainTextDictionary(new File(dictionaryFile.toURI())), config, true);
		} catch (Exception e) {
			System.err.println(e.toString());
		}
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		logger.debug("filtering: " + tuple);
		if (isKeep(tuple)) {
			logger.debug("Accepted tuple: " + tuple);
			collector.emit(tuple);
		}
	}

	private boolean shouldKeep(String stem) {
		if (stem == null)
			return false;
		if (stem.equals(""))
			return false;
		if (filterTerms.contains(stem))
			return false;
		// we don't want integers
		try {
			Integer.parseInt(stem);
			return false;
		} catch (Exception e) {
		}
		// or floating point numbers
		try {
			Double.parseDouble(stem);
			return false;
		} catch (Exception e) {
		}
		try {
			return spellchecker.exist(stem);
		} catch (Exception e) {
			logger.error("Exception when running spellchecker on " + stem, e);
			return false;
		}
	}

	private boolean isKeep(TridentTuple tuple) {
		return shouldKeep(tuple.getString(0));
	}

}
