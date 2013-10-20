package ch.trivadis.sample.storm.bolt;


import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

public class HashtagCounterCassandra extends CassandraBaseBolt {
	
	public HashtagCounterCassandra(String cassandraHost, int cassandraPort) {
		this.cassandraHost = cassandraHost;
		this.cassandraPort = cassandraPort;
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String hashtag = input.getStringByField("hashtag");
		System.out.println("hashtag : " + hashtag);
		
		PreparedStatement insertTweetCount = 
				session.prepare("UPDATE hashtag_count SET nof_occurences = nof_occurences + ? where hashtag = ?;");
		BoundStatement boundStatement = new BoundStatement(insertTweetCount);
		session.execute(boundStatement.bind(1L, hashtag));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
