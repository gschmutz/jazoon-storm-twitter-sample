package ch.trivadis.sample.storm.bolt;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;
import twitter4j.Status;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import ch.trivadis.sample.domain.Tweet;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

public class TweetStorageCassandra extends CassandraBaseBolt {
	transient Jedis jedis; 
	protected String redisHost;
	protected int redisPort;
	
	private List<String> convertToList(Status tweet) {
		List<String> hashtags = new ArrayList<String>();
		for (int i=0; i<tweet.getHashtagEntities().length; i++) {
			hashtags.add(tweet.getHashtagEntities()[i].getText());
		}
		return hashtags;
	}

	public TweetStorageCassandra(String cassandraHost, int cassandraPort) {
		this.cassandraHost = cassandraHost;
		this.cassandraPort = cassandraPort;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Tweet tweet = (Tweet)input.getValueByField("tweet");

		PreparedStatement insertTweetCount = 
				session.prepare("INSERT INTO tweet (tweet_id, username, message, hashtags) VALUES (?, ?, ?, ?);");
		BoundStatement boundStatement = new BoundStatement(insertTweetCount);
		session.execute(boundStatement.bind(tweet.getId(), tweet.getScreenName(), tweet.getMessage(), tweet.getHashtags()));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	

}
