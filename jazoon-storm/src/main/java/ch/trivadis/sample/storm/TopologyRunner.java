package ch.trivadis.sample.storm;

import java.util.ArrayList;
import java.util.List;

import storm.kafka.KafkaConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import ch.trivadis.sample.storm.bolt.HashtagCounterRedis;
import ch.trivadis.sample.storm.bolt.HashtagSplitter;
import ch.trivadis.sample.storm.bolt.DeserializerAndConverter;

public class TopologyRunner {

	public static StormTopology createTopology() {
		
		TopologyBuilder builder = new TopologyBuilder();

        int partitions = 1;
        final String offsetPath = "/tweet_v11";
        final String consumerId = "v1";
        final String topic = "TOPIC_TWEET_V10";
        
        List<String> hosts = new ArrayList<String>();
        hosts.add("localhost");
        SpoutConfig kafkaConfig = new SpoutConfig(KafkaConfig.StaticHosts.fromHostString(hosts, partitions), topic, offsetPath, consumerId);
        kafkaConfig.bufferSizeBytes = 1024*1024*4;
        kafkaConfig.fetchSizeBytes = 1024*1024*4;
        kafkaConfig.forceFromStart = true;
		
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
		
		builder.setSpout("tweet-stream", kafkaSpout, 1);
		//builder.setSpout("tweet-stream", new TwitterStreamingSpoutMock(), 1);
		
		builder.setBolt("unmarshaller", new DeserializerAndConverter(), 2).shuffleGrouping("tweet-stream");
		
		builder.setBolt("hashtag-splitter", new HashtagSplitter(), 2)
				.shuffleGrouping("unmarshaller");
		
		//builder.setBolt("hashtag-filter", new HashtagWireTap(), 2).shuffleGrouping("hashtag-splitter"); 
		
		//builder.setBolt("hashtag-printer", new ConsoleWriterBolt(), 2)
		//	.fieldsGrouping("hashtag-filter", "filtered", new Fields("hashtag"));
		
		builder.setBolt("hashtag-counter", new HashtagCounterRedis("localhost",6379), 2)
				.fieldsGrouping("hashtag-splitter", new Fields("hashtag"));
		
		return builder.createTopology();
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {

		StormTopology topology = createTopology();
		
		Config conf = new Config();
		conf.setDebug(true);

		conf.setMaxTaskParallelism(3);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("word-count", conf, topology);

		Thread.sleep(90000000);

		cluster.shutdown();
	}

}
