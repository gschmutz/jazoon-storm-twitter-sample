package ch.trivadis.sample.storm.bolt;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseBasicBolt;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public abstract class CassandraBaseBolt extends BaseBasicBolt {
	transient Cluster cluster;
	transient Session session;
	protected String cassandraHost;
	protected int cassandraPort;

	public CassandraBaseBolt(String cassandraHost, int cassandraPort) {
		this.cassandraHost = cassandraHost;
		this.cassandraPort = cassandraPort;
	}

	public CassandraBaseBolt() {
		super();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		cluster = Cluster.builder().withPort(cassandraPort).addContactPoint(cassandraHost).build();
		session = cluster.connect("twitter_sample");
		super.prepare(stormConf, context);
	}

}