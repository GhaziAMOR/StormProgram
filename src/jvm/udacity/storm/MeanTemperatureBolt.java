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

public class MeanTemperatureBolt extends BaseRichBolt   
{
	
	private float mean ;
	private int count = 0 ; 
	OutputCollector _collector;
	@Override 
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		// nothing to declare : this is the last bolt in the toplogy
	}     
	
	 public void prepare(
		        Map                     map,
		        TopologyContext         topologyContext,
		        OutputCollector         collector)
		    {
		      // save the output collector for emitting tuples
		      _collector = collector;
		    }
	@Override 
	public void execute(Tuple tuple)
	{
		String temp = tuple.getString(0);
		//String temp = tuple.getStringByField("temperature");
		
		
		//int value =  Integer.parseInt(temp);	
		float value = Float.parseFloat(temp);
		//System.out.println("|||||||||||||||||||||||||||||||||||||||||| temp ="+ value);
		
		
		if ( !checkCount())
		{
			mean = mean + value ; 
		}
		else
		{
			System.out.println("*********************************************** mean temperature = "+ mean);
		}
	}
	
	public boolean checkCount()
	{
		if (count < 10)
		{
			count ++ ;
			return(true);	
		}
		else
		{
			count = 0 ;
			mean= mean / 10;
			return(false);
		}
		
	}
	
	
}
