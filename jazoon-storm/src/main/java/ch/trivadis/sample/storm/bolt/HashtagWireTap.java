package ch.trivadis.sample.storm.bolt;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ch.trivadis.sample.domain.Tweet;

public class HashtagWireTap extends BaseBasicBolt {

	public HashtagWireTap() {
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String hashtag = input.getStringByField("hashtag");
		collector.emit("all", new Values(hashtag));
		if (hashtag.equals("storm")) {
			collector.emit("filtered", new Values(hashtag));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("all", new Fields("hashtag"));
		declarer.declareStream("filtered", new Fields("hashtag"));
	}

}
