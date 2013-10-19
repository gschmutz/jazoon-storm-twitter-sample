package ch.trivadis.sample.storm.spout;

import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import twitter4j.StatusListener;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import ch.trivadis.sample.domain.Tweet;
import ch.trivadis.sample.domain.TweetMock;

public class TwitterStreamingSpoutMock extends BaseRichSpout {

	SpoutOutputCollector collector;
	
	Tweet t1 = new TweetMock("user1", "this is a tweet for #JFS2013", "JFS2013"); 
	Tweet t2 = new TweetMock("user1", "this is a tweet for #JSF2013 about #storm", "JFS2013,storm"); 
	Tweet t3 = new TweetMock("user1", "this is another tweet for #JFS2013 with #OtherHashtag", "JFS2013,OtherHashtag"); 
	private Tweet[] tweets = new Tweet[] { t1, t2, t3 };
	
	public TwitterStreamingSpoutMock() {
	}
    
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
        this.collector = collector;
	}

	@Override
	public void nextTuple() {
		final Random rand = new Random();
		Utils.sleep(rand.nextInt(3000));
		Tweet tweet = tweets[rand.nextInt(tweets.length)];
        if (tweet != null) {
            collector.emit(new Values(tweet));
        }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
