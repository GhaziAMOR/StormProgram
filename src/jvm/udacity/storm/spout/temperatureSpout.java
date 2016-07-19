package udacity.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;

import java.util.List;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;


public class temperatureSpout extends BaseRichSpout {

	private SpoutOutputCollector outputCollector; 

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields("value"));
	}

	@Override
	public void open(Map configMap, TopologyContext context,SpoutOutputCollector outputCollector)
	{
		this.outputCollector = outputCollector;
		DBCursor myDoc = null;
    	try
    	{
    		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
    		DB db = mongoClient.getDB( "local" );
     		DBCollection coll = db.getCollection("testCollection");
     		BasicDBObject query1 = new BasicDBObject();
     		BasicDBObject query = new BasicDBObject("_id",0);  
     		query.append("node",0);
     		myDoc = coll.find(query1,query);
		   	while(myDoc.hasNext()) 
		   	{
       			System.out.println("**************************  " +  myDoc.next());
  			}
     		//DBObject myDoc = coll.find(query1,query);
    		//System.out.println("**************************  " +  myDoc);
		}
		catch(Exception e)
		{

		}
		finally 
		{
   			myDoc.close();
		}
	}

	@Override
	public void nextTuple()
	{
		// to complete
	}
}