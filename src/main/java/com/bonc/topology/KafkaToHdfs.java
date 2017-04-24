package com.bonc.topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;


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

public class KafkaToHdfs {
	public static void main(String args[]) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, IOException{

		String topic = "kafkaTopic1";// kafka的topic
		String host = "192.168.37.160";
		String brokerZkPath = "/broker";
		ZkHosts zk = new ZkHosts(host);

		SpoutConfig spoutConfig = new SpoutConfig(zk, topic, brokerZkPath, "01");

		List<String> zkServers = new ArrayList<String>();

		for (String str : zk.brokerZkStr.split(",")) {
			zkServers.add(str);
		}

		spoutConfig.zkServers = zkServers;
		spoutConfig.zkPort = 2181;
		spoutConfig.forceFromStart = false;
		spoutConfig.socketTimeoutMs = 60;
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		// 定义字段之间的分隔符和行结束符
		RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter("|").withRecordDelimiter("\n");
		// 定义每次写入的tuple的数量
		SyncPolicy syncPolicy = new CountSyncPolicy(5);
		// 定义生成的文件
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/strom/kafka/").withPrefix("kafkaTo")
				.withExtension(".txt");

		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(0,Units.KB);

		HdfsBolt hdfsBolt = new HdfsBolt()
				.withFsUrl("file:///")
				.withFileNameFormat(fileNameFormat)
				.withRecordFormat(recordFormat)
				.withRotationPolicy(rotationPolicy)
				.withSyncPolicy(syncPolicy);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig));
		builder.setBolt("sentenceBolt", new SplitSentenceBolt(), 3).shuffleGrouping("spout");
		builder.setBolt("countBolt", new CountBolt(), 3).fieldsGrouping("sentenceBolt", new Fields("word"));
		builder.setBolt("hdfsBolt", hdfsBolt, 3).fieldsGrouping("countBolt", new Fields("word"));

		Config conf = new Config();
		conf.setDebug(true);

		if (args.length > 0) {
			try {
				conf.setNumWorkers(3);
				conf.setMaxTaskParallelism(3);
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			conf.setNumWorkers(3);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafkaToHdfs", conf, builder.createTopology());
		}

	}
}
