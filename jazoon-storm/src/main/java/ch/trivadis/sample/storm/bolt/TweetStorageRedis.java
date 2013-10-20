package ch.trivadis.sample.storm.bolt;

import java.util.Map;

import redis.clients.jedis.Jedis;
import twitter4j.Status;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class TweetStorageRedis extends BaseBasicBolt {
	transient Jedis jedis; 
	private String redisHost;
	private int redisPort;

	public TweetStorageRedis(String redisHost, int redisPort) {
		this.redisHost = redisHost;
		this.redisPort = redisPort;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Status tweet = (Status)input.getValueByField("tweet");
		jedis.hset("tweet:hashtags", String.valueOf(tweet.getId()), tweet.getText());
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		jedis = new Jedis(redisHost, redisPort, 1800);
		jedis.connect();
		super.prepare(stormConf, context);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	

}
