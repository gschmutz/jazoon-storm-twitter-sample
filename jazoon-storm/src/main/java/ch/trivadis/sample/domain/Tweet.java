package ch.trivadis.sample.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class Tweet implements Serializable {

	private Long id;
	private Date createdAt;
	private String screenName;
	private String message;
	
	private List<String> hashtags;
	
	public Tweet(Long id, Date createdAt, String screenName, String message, List<String> hashtags) {
		this.id = id;
		this.createdAt = createdAt;
		this.screenName = screenName;
		this.message = message;
		this.hashtags = hashtags;
	}

	public Tweet(Long id, Date createdAt, String screenName, String message, String hashtags) {
		this.id = id;
		this.createdAt = createdAt;
		this.screenName = screenName;
		this.message = message;
		this.hashtags = new ArrayList<String>();
		String[] ht = StringUtils.split(hashtags, ",");
		for (int i = 0; i<ht.length; i++) {
			this.hashtags.add(ht[i]);
		}
	}

	public Long getId() {
		return id;
	}

	public String getScreenName() {
		return screenName;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public String getMessage() {
		return message;
	}

	public List<String> getHashtags() {
		return hashtags;
	}
	
}
