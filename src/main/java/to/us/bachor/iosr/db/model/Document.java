package to.us.bachor.iosr.db.model;

import java.util.Date;

import org.springframework.data.annotation.Id;

@org.springframework.data.mongodb.core.mapping.Document
public class Document {

	@Id
	private String url;

	private Date tweetDate;

	public Document(String url, Date tweetDate) {
		super();
		this.url = url;
		this.tweetDate = tweetDate;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Date getTweetDate() {
		return tweetDate;
	}

	public void setTweetDate(Date tweetDate) {
		this.tweetDate = tweetDate;
	}

	@Override
	public String toString() {
		return "Document [url=" + url + ", tweetDate=" + tweetDate + "]";
	}

}
