package to.us.bachor.iosr;

import static to.us.bachor.iosr.TopologyNames.*;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;
import to.us.bachor.iosr.function.DocumentFetchFunction;
import to.us.bachor.iosr.function.DocumentTokenizer;
import to.us.bachor.iosr.function.LoggingFunction;
import to.us.bachor.iosr.function.MapGetNoNulls;
import to.us.bachor.iosr.function.TermFilter;
import to.us.bachor.iosr.function.TfidfExpression;
import to.us.bachor.iosr.spout.UrlSpout;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;

public class TfIdfToplogyCreator {
	private static String[] mimeTypes = new String[] { "text/html", "text/plain" };

	public TridentTopology createTfIdfToplogy(final LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();

		// emits: url
		// MOCK:
		// private static String[] urls = new String[] { "http://t.co/hP5PM6fm"/* , "http://t.co/xSFteG23" */};
		// FixedBatchSpout urlSpout = new FixedBatchSpout(new Fields(URL), 1, new
		// ArrayList<Object>(Arrays.asList(urls)));
		// REDIS:
		UrlSpout urlSpout = new UrlSpout();
		/* ================================ streams ================================ */

		// gets: url
		// emits: url, document (actual content), documentId (document's url), source (here: "twitter")
		Stream documentStream = topology //
				.newStream(URL_SPOUT, urlSpout) //
				.parallelismHint(1) //
				.each(new Fields(URL), new DocumentFetchFunction(mimeTypes), new Fields(DOCUMENT, DOCUMENT_ID, SOURCE));

		// gets: url, document (actual content), documentId (document's url), source (here: "twitter")
		// emits: term (lemmatized), documentId (document's url), source (here: "twitter")
		Stream termStream = documentStream //
				.parallelismHint(1) //
				.each(new Fields(DOCUMENT, URL), new DocumentTokenizer(), new Fields(DIRTY_TERM)) //
				.each(new Fields(DIRTY_TERM), new TermFilter(), new Fields(TERM)) //
				.project(new Fields(TERM, DOCUMENT_ID, SOURCE));

		/* ================================ states ================================ */

		// gets: url, document (actual content), documentId (document's url), source (here: "twitter")
		// contains: d (total number of documents from this source)
		TridentState dState = documentStream //
				.each(new Fields(URL, SOURCE), new LoggingFunction("dstate"), new Fields())//
				.groupBy(new Fields(SOURCE)) //
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields(D_TERM));

		// gets: term (lemmatized), documentId (document's url), source (here: "twitter")
		// contains: df (number of appearances of the term in all documents)
		TridentState dfState = termStream //
				.groupBy(new Fields(TERM)) //
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields(DF_TERM));

		// gets: documentId (document's url), term (lemmatized)
		// contains: tf (number of appearances of the term in the document)
		TridentState tfState = termStream //
				.groupBy(new Fields(DOCUMENT_ID, TERM)) //
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields(TF_TERM));

		/* ================================ DRPC streams ================================ */

		// gets: args (a string in form <documentId><space><term>)
		// returns: documentId (document's url), term, tfidf
		termStream
				.stateQuery(dState, new Fields(SOURCE), new MapGetNoNulls(), new Fields(D_TERM))
				.stateQuery(dfState, new Fields(TERM), new MapGetNoNulls(), new Fields(DF_TERM))
				.stateQuery(tfState, new Fields(DOCUMENT_ID, TERM), new MapGetNoNulls(), new Fields(TF_TERM))
				.each(new Fields(TERM, DOCUMENT_ID, D_TERM, DF_TERM, TF_TERM), new TfidfExpression(),
						new Fields(TF_IDF_RESULT))//
				.project(new Fields(DOCUMENT_ID, TERM, TF_IDF_RESULT))//
				.each(new Fields(DOCUMENT_ID, TERM, TF_IDF_RESULT), new LoggingFunction("tfidf"), new Fields()); //

		return topology;
	}
}
