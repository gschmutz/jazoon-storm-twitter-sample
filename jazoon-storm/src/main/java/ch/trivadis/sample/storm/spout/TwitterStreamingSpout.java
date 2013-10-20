package ch.trivadis.sample.storm.spout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import ch.trivadis.sample.domain.Tweet;
import ch.trivadis.sample.util.TwitterUtil;

public class TwitterStreamingSpout extends BaseRichSpout implements StatusListener {

	SpoutOutputCollector collector;
	TwitterStream twitterStream;
    private transient BlockingQueue<Tweet> queue;
    private Configuration twitterConfiguration;
    private String[] filterWords = null;

	public TwitterStreamingSpout(String consumerKey, String consumerSecret, String token, 
									String secret, String[] filterWords) {
		twitterConfiguration = new ConfigurationBuilder()
										.setOAuthConsumerKey(consumerKey)
										.setOAuthConsumerSecret(consumerSecret)
										.setOAuthAccessToken(token)
										.setOAuthAccessTokenSecret(secret)
										.build();
		this.filterWords = filterWords;
	}
    
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
        this.queue = new ArrayBlockingQueue<Tweet>(1000);
        this.collector = collector;

        twitterStream = new TwitterStreamFactory(twitterConfiguration).getInstance();
        twitterStream.addListener(this);
        twitterStream.filter(new FilterQuery().track(filterWords));
	}

	@Override
	public void nextTuple() {
        Tweet tweet = queue.poll();
        if (tweet != null) {
            collector.emit(new Values(tweet));
        }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	// StatusListener interface
	
	@Override
	public void onException(Exception arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onScrubGeo(long arg0, long arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStatus(Status status) {
		// TODO Auto-generated method stub
		List<String> hashtags = TwitterUtil.convertToList(status.getHashtagEntities()); 
		Tweet tweet = new Tweet(status.getId(), status.getCreatedAt(), status.getUser().getName(), status.getText(), hashtags);
		queue.offer(tweet);
	}

	@Override
	public void onTrackLimitationNotice(int arg0) {
		// TODO Auto-generated method stub
		
	}

}
