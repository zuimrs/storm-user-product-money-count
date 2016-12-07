package com.storm.loganalyze;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class UserMoneyCountBolt extends BaseRichBolt {

	private OutputCollector collector;
	private HashMap<String,Float> counts = null;
	/*
	* when receive a new user and money,firstly search this user
	* and his total money(if there is not a record then init to 0)
	* add current money and store,then pass the user and total money
	* to next bolt.
	*/
	public void execute(Tuple tuple){
		String user = tuple.getStringByField("user");
		Float money = Float.parseFloat(tuple.getStringByField("money"));
		Float sumMoney = this.counts.get(user);
		if(sumMoney == null)
			sumMoney = 0.0F;
		sumMoney+=money;
		this.counts.put(user,sumMoney);
		this.collector.emit(new Values(user,sumMoney));
	}

	public void prepare(Map config,TopologyContext context,OutputCollector collector){
		this.collector=collector;
		this.counts = new HashMap<String,Float>();

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields("user","money"));
	}

}