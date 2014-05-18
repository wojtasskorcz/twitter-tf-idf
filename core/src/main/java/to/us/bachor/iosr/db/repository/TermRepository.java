package to.us.bachor.iosr.db.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import to.us.bachor.iosr.db.model.Term;

public interface TermRepository extends MongoRepository<Term, String> {

}
