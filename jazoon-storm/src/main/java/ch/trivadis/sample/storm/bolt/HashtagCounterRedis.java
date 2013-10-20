package ch.trivadis.sample.storm.bolt;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class HashtagCounterRedis extends BaseBasicBolt {
	transient Jedis jedis; 
	private String redisHost;
	private int redisPort;

	public HashtagCounterRedis(String redisHost, int redisPort) {
		this.redisHost = redisHost;
		this.redisPort = redisPort;
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String hashtag = input.getStringByField("hashtag");
		System.out.println("hashtag : " + hashtag);
		jedis.hincrBy("jfs2013:hashtags", hashtag, 1);
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
