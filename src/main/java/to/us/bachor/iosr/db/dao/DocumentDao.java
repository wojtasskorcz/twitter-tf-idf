package to.us.bachor.iosr.db.dao;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import to.us.bachor.iosr.db.model.Document;
import to.us.bachor.iosr.db.repository.DocumentRepository;

@Component
public class DocumentDao {

	@Autowired
	private MongoTemplate mongoTemplate;
	@Autowired
	private DocumentRepository documentRepository;

	public void saveDocument(Document document) {
		documentRepository.save(Arrays.asList(document));
	}

	public Document getDocumentByUrl(String url) {
		return documentRepository.findOne(url);
	}

	public Document getOldestUnprocessedAndMarkAsProcessed() {
		Query query = new Query(Criteria.where("processed").is(false));
		query.with(new Sort(Sort.Direction.ASC, "tweetDate"));
		Update update = new Update().set("processed", true);
		return mongoTemplate.findAndModify(query, update, Document.class);
	}

}
