package ch.trivadis.sample.domain;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.utils.UUIDs;

public class TweetMock extends Tweet {

	private static List<String> convertToList(String values) {
		List<String> hashtags = new ArrayList<String>();
		String[] ht = StringUtils.split(values, ",");
		for (int i = 0; i<ht.length; i++) {
			hashtags.add(ht[i]);
		}
		return hashtags;
	}
	
	public TweetMock(String screenName, String message, String values) {
		super(null, null, screenName, message, convertToList(values));
	}

	public Long getId() {
		Long id = super.getId();
		if (id == null) {
			id = UUIDs.timeBased().timestamp();
		}
		return id;
	}

	public Date getCreatedAt() {
		Date createdAt = super.getCreatedAt();
		if (createdAt == null) {
			createdAt = new Date();
		}
		return createdAt;
	}
}
