package to.us.bachor.iosr.db.model;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.DBRef;

public class Term {

	@Id
	private String term;

	@DBRef
	private Collection<Document> documents = new ArrayList<>();

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public Collection<Document> getDocuments() {
		return documents;
	}

	public void setDocuments(Collection<Document> documents) {
		this.documents = documents;
	}

	public void addDocument(Document document) {
		documents.add(document);
	}

}
