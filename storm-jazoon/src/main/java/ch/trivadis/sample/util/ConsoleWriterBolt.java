package ch.trivadis.sample.util;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ch.trivadis.sample.domain.Tweet;

public class ConsoleWriterBolt extends BaseBasicBolt {

	public ConsoleWriterBolt() {
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String hashtag = input.getStringByField("hashtag");
		System.err.println("hashtag has been filtered: " + hashtag);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
