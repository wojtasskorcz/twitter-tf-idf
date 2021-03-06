package to.us.bachor.iosr.function;

import static to.us.bachor.iosr.TopologyNames.*;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class DocumentFetchFunction extends BaseFunction {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(DocumentFetchFunction.class);

	private List<String> mimeTypes;

	public DocumentFetchFunction(String[] supportedMimeTypes) {
		mimeTypes = Arrays.asList(supportedMimeTypes);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String url = tuple.getStringByField(URL);
		logger.debug("fetching document: " + url);
		try {
			Parser parser = new AutoDetectParser();
			Metadata metadata = new Metadata();
			ParseContext parseContext = new ParseContext();
			URL urlObject = new URL(url);
			ContentHandler handler = new BodyContentHandler(10 * 1024 * 1024);
			parser.parse((InputStream) urlObject.getContent(), handler, metadata, parseContext);
			String[] mimeDetails = metadata.get("Content-Type").split(";");
			if ((mimeDetails.length > 0) && (mimeTypes.contains(mimeDetails[0]))) {
				logger.debug("emitting fetched document contents: " + url);
				collector.emit(new Values(handler.toString(), url.trim(), TWITTER_SOURCE));
			}
		} catch (Exception e) {
			logger.error("Cannot fetch document: " + url, e);
		}
	}

}
