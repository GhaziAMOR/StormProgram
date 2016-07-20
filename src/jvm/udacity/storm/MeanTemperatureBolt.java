package udacity.storm;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

public class MeanTemperatureBolt extends BaseBasicBolt   
{
	@Override 
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		// nothing to declare : this is the last bolt in the toplogy
	}
	
	@Override 
	public void execute(Tuple tuple , BasicOutputCollector outputCollector)
	{
		tuple.getStringByField("temperature");
		
	}
}
