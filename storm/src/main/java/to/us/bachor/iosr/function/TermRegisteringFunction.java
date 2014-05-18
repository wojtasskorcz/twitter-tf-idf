package to.us.bachor.iosr.function;

import static to.us.bachor.iosr.TopologyNames.*;

import java.util.Map;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import to.us.bachor.iosr.db.dao.DocumentDao;
import to.us.bachor.iosr.db.dao.TermDao;
import to.us.bachor.iosr.db.model.Document;

@SuppressWarnings("rawtypes" /* Storm has no generic types */)
public class TermRegisteringFunction extends BaseFunction {

	private static final long serialVersionUID = 1L;

	private TermDao termDao;
	private DocumentDao documentDao;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		ApplicationContext springContext = new ClassPathXmlApplicationContext("mongoConfiguration.xml");
		termDao = springContext.getBean(TermDao.class);
		documentDao = springContext.getBean(DocumentDao.class);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String term = tuple.getString(0);
		String url = tuple.getStringByField(DOCUMENT_ID);
		Document document = documentDao.getDocumentByUrl(url);
		termDao.addDocumentToTerm(document, term);
		collector.emit(tuple);
	}

}
