package ch.trivadis.sample.storm.trident;

import ch.trivadis.sample.domain.Tweet;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.tuple.Values;

public class HashtagSplitter extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Tweet tweet = (Tweet)tuple.getValueByField("tweet");
		for (String hashTag : tweet.getHashtags()) {
			collector.emit(new Values(hashTag));
		}
	}
}
