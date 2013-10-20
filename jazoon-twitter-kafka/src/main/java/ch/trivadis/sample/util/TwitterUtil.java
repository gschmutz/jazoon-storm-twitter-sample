package ch.trivadis.sample.util;

import java.util.ArrayList;
import java.util.List;

import twitter4j.HashtagEntity;

public class TwitterUtil {
	public static List<String> convertToList(HashtagEntity[] entities) {
		List<String> hashtags = new ArrayList<String>();
		for (int i=0; i<entities.length; i++) {
			hashtags.add(entities[i].getText());
		}
		return hashtags;
	}

}
