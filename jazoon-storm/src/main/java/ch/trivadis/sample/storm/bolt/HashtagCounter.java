package ch.trivadis.sample.storm.bolt;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class HashtagCounter extends BaseBasicBolt {
	transient Jedis jedis; 
	private String redisHost;

	public HashtagCounter(String redisHost) {
		this.redisHost = redisHost;
	}
	
	private String normalizeString(String input) {
		String result = input;
		result = StringUtils.lowerCase(result);
		return result;
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String hashtag = input.getStringByField("hashtag");
		String hashtagNormalized = normalizeString(hashtag);
		System.out.println("hashtag : " + hashtagNormalized);
		jedis.hincrBy("jfs2013:hashtags", hashtagNormalized, 1);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		jedis = new Jedis(redisHost, 6379, 1800);
		jedis.connect();
		super.prepare(stormConf, context);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
