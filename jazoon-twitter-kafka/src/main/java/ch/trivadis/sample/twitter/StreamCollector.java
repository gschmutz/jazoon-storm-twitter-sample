/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package ch.trivadis.sample.twitter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.scheduling.annotation.Scheduled;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import ch.trivadis.sample.twitter.avro.v11.TwitterStatusUpdate;
import ch.trivadis.sample.twitter.v11.TwitterStatusUpdateConverter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.DefaultStreamingEndpoint;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import com.twitter.hbc.twitter4j.handler.StatusStreamHandler;
import com.twitter.hbc.twitter4j.message.DisconnectMessage;
import com.twitter.hbc.twitter4j.message.StallWarningMessage;

@ManagedResource(description="Streaming Collector")
public class StreamCollector  {
	
	private final static Logger logger = LoggerFactory.getLogger(StreamCollector.class);

	private BlockingQueue<String> queue;
	private BasicClient client;
	private Twitter twitter = null;
	
	private AtomicLong tweetCounterLast = new AtomicLong(0);
	private AtomicLong tweetCounter = new AtomicLong(0);
	private AtomicLong deletionNoticeCounter = new AtomicLong(0);
	private AtomicLong trackLimitationNoticeCounter = new AtomicLong(0);
	private AtomicLong scrubGeoCounter = new AtomicLong(0);
	private AtomicLong exceptionCounter = new AtomicLong(0);
	private AtomicLong disconnectMessageCounter = new AtomicLong(0);
	private AtomicLong unknownMessageTypeCounter = new AtomicLong(0);
	private AtomicLong stallWarningCounter = new AtomicLong(0);
	private AtomicLong stallWarningMessageCounter = new AtomicLong(0);
	
	private String kafkaHostname;
	private String kafkaPort;
	private String kafkaTopicName;
	private int numberOfProcessingThreads = 5; 
	
	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessTokenSecret;
	
	private List<String> trackTerms = new ArrayList<String>();

	@ManagedAttribute(description="Number of tweets processed since start", currencyTimeLimit=15)
	  public Long getTweetCounter() {
	    return tweetCounter.longValue();
	}
	
	@ManagedAttribute(description="Number of deletion notice since start", currencyTimeLimit=15)
	  public Long getDeletionNoticeCounter() {
	    return deletionNoticeCounter.longValue();
	}

	@ManagedAttribute(description="Number of track limitation notice since start", currencyTimeLimit=15)
	  public Long getTrackLimitiationNoticeCounter() {
	    return trackLimitationNoticeCounter.longValue();
	}

	@ManagedAttribute(description="Number of scrub geo since start", currencyTimeLimit=15)
	  public Long getScrubGeoCounter() {
	    return scrubGeoCounter.longValue();
	}

	@ManagedAttribute(description="Number of exception since start", currencyTimeLimit=15)
	  public Long getExceptionCounter() {
	    return exceptionCounter.longValue();
	}

	@ManagedAttribute(description="Number of disconnect message since start", currencyTimeLimit=15)
	  public Long getDisconnectMessageCounter() {
	    return disconnectMessageCounter.longValue();
	}

	@ManagedAttribute(description="Number of unknown message type since start", currencyTimeLimit=15)
	  public Long getUnknownMessageTypeCounter() {
	    return unknownMessageTypeCounter.longValue();
	}

	@ManagedAttribute(description="Number of stall warning since start", currencyTimeLimit=15)
	  public Long getStallWarningCounter() {
	    return stallWarningCounter.longValue();
	}

	@ManagedAttribute(description="Number of stall warning messsage since start", currencyTimeLimit=15)
	  public Long getStallWarningMessageCounter() {
	    return stallWarningMessageCounter.longValue();
	}

	@Required
	public void setKafkaTopicName(String kafkaTopicName) {
		this.kafkaTopicName = kafkaTopicName;
	}

	@Required
	public void setKafkaHostname(String kafkaHostname) {
		this.kafkaHostname = kafkaHostname;
	}

	@Required
	public void setKafkaPort(String kafkaPort) {
		this.kafkaPort = kafkaPort;
	}

	public void setNumberOfProcessingThreads(int numberOfProcessingThreads) {
		this.numberOfProcessingThreads = numberOfProcessingThreads;
	}
	
	@Required
	public void setConsumerKey(String consumerKey) {
		this.consumerKey = consumerKey;
	}

	@Required
	public void setConsumerSecret(String consumerSecret) {
		this.consumerSecret = consumerSecret;
	}

	@Required
	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}

	@Required
	public void setAccessTokenSecret(String accessTokenSecret) {
		this.accessTokenSecret = accessTokenSecret;
	}

	public void setTrackTerms(List<String> trackTerms) {
		this.trackTerms = trackTerms;
	}

	protected DefaultStreamingEndpoint createEndpoint() {
		
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		
		endpoint.trackTerms(trackTerms);
		
		return endpoint;
	}
	
	/**
	 * Write the counters to the log every 5min
	 */
	@Scheduled(fixedRate=300000)
	public void logCounter() {

		logger.info("-----------------------------------------------------------------------------------------------------------------");
		logger.info("[Tweet=" + getTweetCounter() + "(+" 
												+ (getTweetCounter() - tweetCounterLast.get()) + ")" + 
												",Exc=" + getExceptionCounter() + 
												",Del=" + getDeletionNoticeCounter() +
												",Disc=" + getDisconnectMessageCounter() +
												",Scub=" + getScrubGeoCounter() +
												",Stall=" + getStallWarningCounter() +
												",Limit=" + getTrackLimitiationNoticeCounter() +
												",Unknown=" + getUnknownMessageTypeCounter() + "]");
		
		tweetCounterLast.set(tweetCounter.get());
	}
	
	public StreamCollector() {
	}

	public void start() {
		System.out.println("start() ...");
		// Create an appropriately sized blocking queue
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		
		// create the endpoint
		DefaultStreamingEndpoint endpoint = createEndpoint();
		System.out.println("endpoint created ...");

		endpoint.stallWarnings(false);

		// create an authentication
		Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

		// Create a new BasicClient. By default gzip is enabled.
		client = new ClientBuilder().name("sampleExampleClient")
				.hosts(Constants.STREAM_HOST).endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();
		System.out.println("client created ...");

		// Create an executor service which will spawn threads to do the actual
		// work of parsing the incoming messages and
		// calling the listeners on each message
		ExecutorService service = Executors
				.newFixedThreadPool(this.numberOfProcessingThreads);

		// Wrap our BasicClient with the twitter4j client
		Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(client,
				queue, Lists.newArrayList(listener2), service);

		// Establish a connection
		t4jClient.connect();
		System.out.println("connection established ...");

		for (int threads = 0; threads < this.numberOfProcessingThreads; threads++) {
			// This must be called once per processing thread
			t4jClient.process();
			System.out.println("thread " + threads + " started ...");

		}
	};

	public void stop() {
		client.stop();
	};

	public boolean isRunning() {
		return true;
	};
	
	// A bare bones StatusStreamHandler, which extends listener and gives some
	// extra functionality
	private StatusListener listener2 = new StatusStreamHandler() {
		Producer<String, byte[]> producer = null;
		
	    public void store(Status status) throws IOException, InterruptedException {
	        final String zkConnection = kafkaHostname + ":" + kafkaPort;
	        final String topic = kafkaTopicName;
	        
	        TwitterStatusUpdate update = TwitterStatusUpdateConverter.convert(status);

	        ByteArrayOutputStream out = new ByteArrayOutputStream();
	        DatumWriter<TwitterStatusUpdate> writer = new SpecificDatumWriter<TwitterStatusUpdate>(TwitterStatusUpdate.class);
	        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
	        writer.write(update, encoder);
	        encoder.flush();
	        out.close();

	        Message message = new Message(out.toByteArray());
	        if (producer == null) {
	        	System.out.println("Connecting to Kafka Server: " +zkConnection + " using topic " + topic);
	        	logger.info("Connecting to Kafka Server: " +zkConnection + " using topic " + topic);

	        	Properties props = new Properties();
	        	props.put("metadata.broker.list", zkConnection);
	        	props.put("request.required.acks", "1");
	        	//props.put("serializer.class", "kafka.serializer.StringEncoder");
	        	props.put("producer.type", "sync");
	        	props.put("compression.codec", "1");
	        	producer = new kafka.javaapi.producer.Producer<String, byte[]>(new ProducerConfig(props));
	        	
	        	System.out.println("Connected Sucessfully to Kafka Server: " +zkConnection + " using topic " + topic);
	        	logger.info("Connected Successfully to Kafka Server: " +zkConnection + " using topic " + topic);
	        }	
	        producer.send(new KeyedMessage<String, byte[]>(topic, out.toByteArray()));
	    }
		
		int i = 0;
		@Override
		public void onStatus(Status status) {
			boolean processIt = true;
			String retweetedUser = "";
			
			if (status == null) {
				System.err.println("status is null");
			}

			if (processIt) {
				try {
					store(status);
	
					// increment the counter of tweets
					tweetCounter.incrementAndGet();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					logger.error("Error occured in onStatus()", e);
					e.printStackTrace();
				} catch (InterruptedException e) {
					logger.error("Error occured in onStatus()", e);
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (NullPointerException e) {
					logger.error("Error occured in onStatus()", e);
					e.printStackTrace();
				}
			}
		}

		@Override
		public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			//too many, do not log
			//logger.info("=====> onDeletionNotice: " + statusDeletionNotice);
			deletionNoticeCounter.incrementAndGet();
		}

		@Override
		public void onTrackLimitationNotice(int limit) {
			logger.warn("=====> onTrackLimitationNotice: " + limit);
			trackLimitationNoticeCounter.incrementAndGet();
		}

		@Override
		public void onScrubGeo(long user, long upToStatus) {
			logger.warn("=====> onScrubGeo: " + user);
			scrubGeoCounter.incrementAndGet();
		}

		@Override
		public void onException(Exception e) {
			logger.error("=====> onException", e);
			exceptionCounter.incrementAndGet();
		}

		@Override
		public void onDisconnectMessage(DisconnectMessage message) {
			logger.error("=====> onDisconnectMessage: " + message);
			disconnectMessageCounter.incrementAndGet();
		}

		@Override
		public void onUnknownMessageType(String s) {
			logger.warn("=====> onUnknownMessageType: " + s);
			unknownMessageTypeCounter.incrementAndGet();
		}

		@Override
		public void onStallWarning(StallWarning arg0) {
			logger.warn("=====> onStallWarning: " + arg0);
			stallWarningCounter.incrementAndGet();
		}

		@Override
		public void onStallWarningMessage(StallWarningMessage arg0) {
			logger.warn("=====> onStallWarningMessage: " + arg0);
			stallWarningMessageCounter.incrementAndGet();
		}

	};

}
