package muti.kafka.storm.integration.topology;



import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import muti.kafka.storm.integration.bolts.CountBolt;
import muti.kafka.storm.integration.bolts.SplitBolt;


public class KafkaStormSampleTopology {

	private static final Logger logger = LogManager.getLogger("ConsoleLogger");

	public static void main(String[] args) throws Exception {

		Config config = new Config();
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		String zkConnString = "localhost:2181";
		String topic = "my-first-topic";

		// ZkHosts is used to track the Kafka brokers dynamically by maintaining the details in ZooKeeper.
		// It is the simple and fast way to access the Kafka broker.
		BrokerHosts hosts = new ZkHosts(zkConnString);

		// Spoutconfig is an extension of KafkaConfig (API used to define configuration settings 
		// for the Kafka cluster) that supports additional ZooKeeper information.
		// arguments are:
		// - an implementation of the BrokerHosts interface
		// - the topic name
		// - the ZooKeeper root path
		// - the id to uniquely identify your spout on ZooKeeper
		SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());

		kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
		kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;

		// SchemeAsMultiScheme is an interface that dictates how 
		// the ByteBuffer consumed from Kafka gets transformed into 
		// a storm tuple. It is derived from MultiScheme and accepts
		// implementation of Scheme class. There are lot of implementation
		// of Scheme class and one such implementation is StringScheme, 
		// which parses the byte as a simple string  
		kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();

		// KafkaSpout is our Spout implementation, which will integrate 
		// with Storm. It fetches the messages from Kafka topic and 
		// emits them into Storm ecosystem as tuples. KafkaSpout get 
		// its configuration details from SpoutConfig provided.
		builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));

		builder.setBolt("word-spitter", new SplitBolt()).shuffleGrouping("kafka-spout");
		builder.setBolt("word-counter", new CountBolt()).shuffleGrouping("word-spitter");
		
		logger.info("Creating Local Cluster");
		LocalCluster cluster = new LocalCluster();
		
		logger.info("Submitting Storm Topology");
		cluster.submitTopology("KafkaStormSample", config, builder.createTopology());

		Thread.sleep(1000*30);

		cluster.shutdown();
		
		logger.info("Cluster shutdown completed");
	}

}