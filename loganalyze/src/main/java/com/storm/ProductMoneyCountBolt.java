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

public class ProductMoneyCountBolt extends BaseRichBolt {

	private OutputCollector collector;
	private HashMap<String,Float> counts = null;
	/*
	* when receive a new product and money,firstly search this product
	* and it's total money(if there is not a record then init to 0)
	* add current money and store,then pass the product and total money
	* to next bolt.
	*/
	public void execute(Tuple tuple){
		String product = tuple.getStringByField("product");
		Float money = Float.parseFloat(tuple.getStringByField("money"));
		Float sumMoney = this.counts.get(product);
		if(sumMoney == null)
			sumMoney = 0.0F;
		sumMoney+=money;
		this.counts.put(product,sumMoney);
		this.collector.emit(new Values(product,sumMoney));
	}

	public void prepare(Map config,TopologyContext context,OutputCollector collector){
		this.collector= collector;
		this.counts = new HashMap<String,Float>();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields("product","money"));
	}

}