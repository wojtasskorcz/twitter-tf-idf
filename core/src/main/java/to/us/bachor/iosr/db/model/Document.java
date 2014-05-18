package to.us.bachor.iosr.db.model;

import java.util.Date;

import org.springframework.data.annotation.Id;

@org.springframework.data.mongodb.core.mapping.Document
public class Document {

	@Id
	private String url;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Document other = (Document) obj;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}

	private Date tweetDate;

	private boolean processed = false;

	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
	}

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
