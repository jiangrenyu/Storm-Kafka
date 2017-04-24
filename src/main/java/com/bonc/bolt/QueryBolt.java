package com.bonc.bolt;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.bonc.utils.CountMessageUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class QueryBolt implements IRichBolt {

	List<String> list;
	OutputCollector collector;
	String fileName = "E://系统文件/开发测试/out/bolt.txt";
	public static int lastCount = 0 ;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		list = new ArrayList<String>();
		this.collector = collector;

	}

	public void execute(Tuple input) {
		String valueStr = input.getString(0);
		
		list.add(valueStr);
		collector.ack(input);
		collector.emit(new Values(valueStr));
		
		System.out.println("接收到的消息 :" + valueStr);
		lastCount = CountMessageUtils.getCount(valueStr,lastCount);
		System.out.println("处理的数据量：" + lastCount + "条");

	}

	public void cleanup() {
		try {
			OutputStream os = new FileOutputStream(fileName);
			PrintStream ps = new PrintStream(os);
			ps.println("开始");
			ps.println(list.size());
			for (String str : list) {
				ps.println(str);
			}
			ps.println("结束");
			try {
				ps.close();
				os.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException f) {
			f.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
