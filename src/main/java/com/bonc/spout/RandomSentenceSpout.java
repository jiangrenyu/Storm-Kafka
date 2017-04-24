package com.bonc.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomSentenceSpout extends BaseRichSpout {

	public SpoutOutputCollector collector; // 定义一个发射器

	public Random _rand;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this._rand = new Random();
	}

	public void nextTuple() {
		Utils.sleep(1000);
		// 定义一个数组作为数据源
		String sentences[] = new String[] { "the cow jumped over the moon", "an apple a day keeps the doctor away",
				"four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };

		String sentence = sentences[_rand.nextInt(sentences.length)];

		collector.emit(new Values(sentence));

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
