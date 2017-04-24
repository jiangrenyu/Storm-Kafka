package com.bonc.topology;

import com.bonc.bolt.CountBolt;
import com.bonc.bolt.SplitSentenceBolt;
import com.bonc.spout.RandomSentenceSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.zookeeper.server.NIOServerCnxnFactory;

public class StormTopology {
	public static void main(String args[])
			throws AlreadyAliveException, InvalidTopologyException, InterruptedException{
	   TopologyBuilder builder = new TopologyBuilder();
	   
	   builder.setSpout("randSpout", new RandomSentenceSpout(),2);
	   
	   builder.setBolt("splitBolt", new SplitSentenceBolt(), 3).shuffleGrouping("randSpout");
	   
	   builder.setBolt("countBolt", new CountBolt(), 3).fieldsGrouping("splitBolt", new Fields("word"));
	   
	   Config conf = new Config();
	   
	   conf.setDebug(true);
	   
	   if(args!=null&&args.length>0){
		   conf.setNumWorkers(3);
		   
		   StormSubmitter.submitTopology("wordCount", conf, builder.createTopology());
		
	   }else{
		   conf.setMaxTaskParallelism(3);
		   
		   LocalCluster cluster = new LocalCluster();
		   
		   cluster.submitTopology("wordCount", conf, builder.createTopology());
		   
		   Thread.sleep(6000);

		   cluster.shutdown();
	   }
	   
	   
  }
}
