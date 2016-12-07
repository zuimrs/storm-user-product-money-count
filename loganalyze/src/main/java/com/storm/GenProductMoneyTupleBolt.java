package com.storm.loganalyze;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class GenProductMoneyTupleBolt extends BaseRichBolt {

	private OutputCollector collector;

	public void execute(Tuple tuple){
		String sentence = tuple.getStringByField("record");
		String [] words = sentence.split(",");
		this.collector.emit(new Values(words[1],words[2]));
	}

	public void prepare(Map config,TopologyContext context,OutputCollector collector){
		this.collector=collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields("product","money"));
	}

}