package com.bonc.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CountBolt extends BaseBasicBolt {

	Map<String, Integer> countMap = new HashMap<String, Integer>();

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));

	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.getString(0);

		Integer count = countMap.get(word);

		if (count == null) {
			count = 0;
		}
		count++;

		countMap.put(word, count);
        System.out.println("单词:  "+word+"  的数量    "+"  已经达到了  :   "+count);
		collector.emit(new Values(word, count));
	}

}
