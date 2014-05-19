package to.us.bachor.iosr.db.dao;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import to.us.bachor.iosr.db.model.Document;
import to.us.bachor.iosr.db.model.Term;
import to.us.bachor.iosr.db.repository.TermRepository;

@Component
public class TermDao {
	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private TermRepository termRepository;

	public void addDocumentToTerm(Document document, String term) {
		Term dbTerm = new Term();
		dbTerm.setTerm(term);
		try {
			mongoTemplate.insert(dbTerm);
		} catch (Exception ex) {

		}

		mongoTemplate.updateFirst(Query.query(Criteria.where("_id").is(term)),
				new Update().push("documents", document), Term.class);
	}

	public Collection<Document> getDocumentsContainingTerm(String term) {
		Term dbTerm = termRepository.findOne(term);
		if (dbTerm == null) {
			return new ArrayList<>();
		}
		return dbTerm.getDocuments();
	}

	public Collection<Term> getTermsFromDocument(Document document) {
		Query filter = new Query(Criteria.where("documents").is(document));
		return mongoTemplate.find(filter, Term.class);
	}
}
