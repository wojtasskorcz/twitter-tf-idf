package to.us.bachor.iosr.db.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import to.us.bachor.iosr.db.model.Document;

public interface DocumentRepository extends MongoRepository<Document, String> {

}
