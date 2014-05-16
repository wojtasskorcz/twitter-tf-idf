package to.us.bachor.iosr;

import static to.us.bachor.iosr.TopologyNames.*;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;
import to.us.bachor.iosr.function.AddSourceField;
import to.us.bachor.iosr.function.DocumentFetchFunction;
import to.us.bachor.iosr.function.DocumentTokenizer;
import to.us.bachor.iosr.function.LoggingFunction;
import to.us.bachor.iosr.function.MapGetNoNulls;
import to.us.bachor.iosr.function.RemoveDuplicatesFilter;
import to.us.bachor.iosr.function.SplitAndProjectToFields;
import to.us.bachor.iosr.function.TermFilter;
import to.us.bachor.iosr.function.TfidfExpression;
import to.us.bachor.iosr.spout.UrlSpout;
import trident.cassandra.CassandraState;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;

public class TfIdfToplogyCreator {
	private static String[] mimeTypes = new String[] { "text/html", "text/plain" };

	public TridentTopology createTfIdfTopology() {
		return createTfIdfTopology(null);
	}

	public TridentTopology createTfIdfTopology(final LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();

		// emits: url
		// MOCK:
		// String[] urls = new String[] { MOCK_URL };
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
				.persistentAggregate(getStateFactory("d"), new Count(), new Fields(D_TERM));

		// gets: term (lemmatized), documentId (document's url), source (here: "twitter")
		// contains: df (number of appearances of the term in all documents)
		TridentState dfState = termStream //
				.each(new Fields(TERM, DOCUMENT_ID), new RemoveDuplicatesFilter())//
				.groupBy(new Fields(TERM)) //
				.persistentAggregate(getStateFactory("df"), new Count(), new Fields(DF_TERM));

		// gets: documentId (document's url), term (lemmatized)
		// contains: tf (number of appearances of the term in the document)
		TridentState tfState = termStream //
				.groupBy(new Fields(DOCUMENT_ID, TERM)) //
				.persistentAggregate(getStateFactory("tf"), new Count(), new Fields(TF_TERM));

		/* ================================ DRPC streams ================================ */

		// gets: args (a string in form <documentId><space><term>)
		// returns: documentId (document's url), term, tfidf
		Stream drpcStream = drpc == null ? topology.newDRPCStream(TF_IDF_QUERY) : topology.newDRPCStream(TF_IDF_QUERY,
				drpc);
		drpcStream
				.each(new Fields(ARGS), new SplitAndProjectToFields(), new Fields(DOCUMENT_ID, TERM))
				.each(new Fields(), new AddSourceField(TWITTER_SOURCE), new Fields(SOURCE))
				.stateQuery(dState, new Fields(SOURCE), new MapGetNoNulls(), new Fields(D_TERM))
				.stateQuery(dfState, new Fields(TERM), new MapGetNoNulls(), new Fields(DF_TERM))
				.stateQuery(tfState, new Fields(DOCUMENT_ID, TERM), new MapGetNoNulls(), new Fields(TF_TERM))
				.each(new Fields(TERM, DOCUMENT_ID, D_TERM, DF_TERM, TF_TERM), new TfidfExpression(),
						new Fields(TF_IDF_RESULT))//
				.project(new Fields(DOCUMENT_ID, TERM, TF_IDF_RESULT));

		return topology;
	}

	private StateFactory getStateFactory(String rowKey) {
		CassandraState.Options options = new CassandraState.Options();
		options.keyspace = "storm";
		options.columnFamily = "tfidf";
		options.rowKey = rowKey;
		return CassandraState.nonTransactional("localhost", options);
	}
}
