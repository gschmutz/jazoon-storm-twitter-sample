package ch.trivadis.sample.storm.trident;

import org.apache.commons.lang.StringUtils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.tuple.Values;

public class HashtagNormalizer extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String hashtag = tuple.getStringByField("hashtag");
		hashtag = StringUtils.lowerCase(hashtag);
		collector.emit(new Values(hashtag));
	}


}
