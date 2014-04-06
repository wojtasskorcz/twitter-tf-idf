package to.us.bachor.iosr.db.dao;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import to.us.bachor.iosr.db.model.Document;
import to.us.bachor.iosr.db.repository.DocumentRepository;

@Component
public class DocumentDao {

	@Autowired
	private DocumentRepository documentRepository;

	public void saveDocument(Document document) {
		documentRepository.save(Arrays.asList(document));
	}

	public Document getDocumentByUrl(String url) {
		return documentRepository.findOne(url);
	}

}
