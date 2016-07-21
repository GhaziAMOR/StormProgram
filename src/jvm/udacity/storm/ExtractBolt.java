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
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

public class ExtractBolt extends BaseRichBolt {
	OutputCollector _collector;
	
	 @Override
	    public void prepare(
	        Map                     map,
	        TopologyContext         topologyContext,
	        OutputCollector         collector)
	    {
	      // save the output collector for emitting tuples
	      _collector = collector;
	    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("temperature"));
		
	}
	
	@Override 
	public void execute (Tuple tuple )
	{
		String value = tuple.getStringByField("value");
		String[] parts = value.split(":");
		parts[1] = parts[1].substring(1,parts[1].indexOf("}"));
		System.out.println("------------- tuple after extract bolt : ----- "+parts[1]);
		_collector.emit(tuple,new Values(parts[1]));
	}

}
