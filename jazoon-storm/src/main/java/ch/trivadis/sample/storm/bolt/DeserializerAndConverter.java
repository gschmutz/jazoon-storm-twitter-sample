package ch.trivadis.sample.storm.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ch.trivadis.sample.domain.Tweet;
import ch.trivadis.sample.twitter.avro.v11.TwitterHashtagEntity;
import ch.trivadis.sample.twitter.avro.v11.TwitterStatusUpdate;
import ch.trivadis.sample.util.DateUtil;

public class DeserializerAndConverter extends BaseBasicBolt {
	transient Schema schema10 = null;
	transient Schema schema11 = null;
	
	private Tweet convert(TwitterStatusUpdate status) {
		
		List<String> hashtags = new ArrayList<String>();
		for (TwitterHashtagEntity entity : status.getHashtagEntities()) {
			hashtags.add(entity.getText().toString());
		}

		Double latitude = status.getCoordinatesLatitude();
		Double longitude = status.getCoordinatesLongitude();
		Date createdAt = DateUtil.toDate(status.getCreatedAt().toString());

		Tweet tweet = new Tweet(status.getId(), createdAt, status.getUser().getScreenName().toString(), status.getText().toString(), hashtags);
		
		return tweet;
	}	
	protected TwitterStatusUpdate deserialize(byte[] bytes) {
		if (schema10 == null) {
			Schema.Parser parser = new Schema.Parser();
			try {
				schema11 = parser.parse(getClass().getResourceAsStream("/TwitterSchema-v1.1.avsc"));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

		TwitterStatusUpdate result = null;
		try {
			DatumReader<TwitterStatusUpdate> reader = new SpecificDatumReader<TwitterStatusUpdate>(schema11,schema11);
			Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
			result = reader.read(null, decoder);
		} catch (IOException e) {
			System.err.println(e.toString());
			throw new RuntimeException(e);
		}
		return result;
	}

	public DeserializerAndConverter() {
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		byte[] b = input.getBinary(0);
		TwitterStatusUpdate status = deserialize(b);
		Tweet tweet = convert(status);
		
		collector.emit(new Values(tweet));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
