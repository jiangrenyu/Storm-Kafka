package com.bonc.topology;

import java.util.ArrayList;
import java.util.List;

import com.bonc.bolt.CountBolt;
import com.bonc.bolt.SplitSentenceBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class KafkaTopology {
	public static void main(String args[])
			throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
		String topic = "kafkaTopic1";

		ZkHosts zkHosts = new ZkHosts("192.168.37.160:2181");

		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/broker", "a");// 保存zk的信息

		List<String> zkServers = new ArrayList<String>();

		for (String str : zkHosts.brokerZkStr.split(":")) {
			zkServers.add(str);
		}

		spoutConfig.zkServers = zkServers;
		spoutConfig.zkPort = 2181;
		spoutConfig.forceFromStart = true;
		spoutConfig.socketTimeoutMs = 60;
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig));
		builder.setBolt("splitSentenceBolt", new SplitSentenceBolt(), 3).shuffleGrouping("kafkaSpout");
		builder.setBolt("countBolt", new CountBolt(), 3).fieldsGrouping("splitSentenceBolt", new Fields("word"));

		Config conf = new Config();
		conf.setDebug(true);

		if (args.length != 0 && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

		} else {
			conf.setMaxTaskParallelism(3);//设置spout
			conf.setNumWorkers(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafka", conf, builder.createTopology());
			//Thread.sleep(60000);
			//cluster.shutdown();
		}

	}

}
