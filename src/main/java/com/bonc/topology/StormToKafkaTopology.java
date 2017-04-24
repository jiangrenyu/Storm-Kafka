package com.bonc.topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bonc.bolt.QueryBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;

/***
 * 
 * @author 姜仁雨
 * 从kafka接受消息，再写会Kafka,并且统计接受的消息数量
 */
public class StormToKafkaTopology {
	public static void main(String args[]) throws InterruptedException {
		// 配置zookeeper主机 端口号
		String topic = "kafkaTopic1";

		ZkHosts zkHosts = new ZkHosts("192.168.37.160:2181");

		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/broker", "a");// 保存zk的信息

		List<String> zkServers = new ArrayList<String>();

		for (String str : zkHosts.brokerZkStr.split(":")) {
			zkServers.add(str);
		}

		spoutConfig.zkServers = zkServers;
		spoutConfig.zkPort = 2181;
		spoutConfig.forceFromStart = false;
		spoutConfig.socketTimeoutMs = 60;
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		Config config = new Config();
		Map<String, String> map = new HashMap<String, String>();
		// 配置Kafka broker地址，写回到这台broker
		map.put("metadata.broker.list", "192.168.37.160:9092");
		// 设置消息的序列化类
		map.put("serializer.class", "kafka.serializer.StringEncoder");
		config.put("kafka.broker.properties", map);
		// 配置KafkaBolt生成的topic
		config.put("topic", "receiver");

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
		builder.setBolt("bolt1", new QueryBolt(), 2).shuffleGrouping("spout");
		builder.setBolt("bolt2", new KafkaBolt(), 2).fieldsGrouping("bolt1", new Fields("message"));

		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", config, builder.createTopology());
//			Thread.sleep(60000);
//			cluster.shutdown();
		}

	}
}
