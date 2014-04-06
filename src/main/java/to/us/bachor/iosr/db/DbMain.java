package to.us.bachor.iosr.db;

import java.util.Date;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;

import to.us.bachor.iosr.db.dao.DocumentDao;
import to.us.bachor.iosr.db.model.Document;

import com.mongodb.WriteConcern;

public class DbMain {

	private static final Logger logger = Logger.getLogger(DbMain.class);

	public static void main(String[] args) {
		@SuppressWarnings("resource")
		ApplicationContext context = new ClassPathXmlApplicationContext("mongoConfiguration.xml");

		MongoTemplate mongoTemplate = context.getBean(MongoTemplate.class);
		mongoTemplate.setWriteConcern(WriteConcern.ERRORS_IGNORED);
		String url = "http://testUrl";
		Date date = new Date();
		DocumentDao documentDao = context.getBean(DocumentDao.class);
		Document document = new Document(url, date);
		logger.debug("Saving document: " + document);
		documentDao.saveDocument(document);
		logger.debug("Searching for document with url: " + url);
		Document retrievedDocument = documentDao.getDocumentByUrl(url);
		logger.debug("Found document: " + retrievedDocument);

	}
}
