package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
/*
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
*/
//********* ADDED 1-of-4 imported http://mvnrepository.com/artifact/com.lambdaworks/lettuce/
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

// COPY AND PASE: following code into pom.xml file (located lesson1/stage1/pom.xml)
//<dependency>
//  <groupId>com.lambdaworks</groupId>
//  <artifactId>lettuce</artifactId>
//  <version>2.3.3</version>
//</dependency>
//
//********* END 1-of-4

//********* BEGIN stage2 exercise part 1-of-2 ***********
//import spout/RandomSentenceSpout
import udacity.storm.spout.RandomSentenceSpout;
import udacity.storm.spout.temperatureSpout;

//********** END stage 2 exercise part 1-of-2 ***********

/**
 * This is a basic example of a Storm topology.
 */

/**
 * This is a basic example of a storm topology.
 *
 * This topology demonstrates how to add three exclamation marks '!!!'
 * to each word emitted
 *
 * This is an example for Udacity Real Time Analytics Course - ud381
 *
 */
public class ReporterExclamationTopology {

  /**
   * A bolt that adds the exclamation marks '!!!' to word
   */
  public static class ExclamationBolt extends BaseRichBolt
  {
    // To output tuples from this bolt to the next stage bolts, if any
    OutputCollector _collector;

    //********* ADDED 2-of-4
    // place holder to keep the connection to redis
    RedisConnection<String,String> redis;
    //********* END 2-of-4

    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         collector)
    {
      // save the output collector for emitting tuples
      _collector = collector;

      //********* ADDED 3-of-4
      // instantiate a redis connection
      RedisClient client = new RedisClient("localhost",6379);

      // initiate the actual connection
      redis = client.connect();
      //********* END 3-of-4
    }

    @Override
    public void execute(Tuple tuple)
    {
      // get the column word from tuple
      String word = tuple.getString(0);

      // build the word with the exclamation marks appended
      StringBuilder exclamatedWord = new StringBuilder();
      exclamatedWord.append(word).append("!!!");

      // emit the word with exclamations
      _collector.emit(tuple, new Values(exclamatedWord.toString()));

      //********* ADDED 4-of-4 redis reporter
      long count = 30;
      redis.publish("WordCountTopology", exclamatedWord.toString() + "|" + Long.toString(count));
      //********* END 4-of-4
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      // tell storm the schema of the output tuple for this spout

      // tuple consists of a single column called 'exclamated-word'
      declarer.declare(new Fields("exclamated-word"));
    }
  }

  public static void main(String[] args) throws Exception
  {
/*DBCursor myDoc = null;
     try{
    MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
    DB db = mongoClient.getDB( "local" );
    
     DBCollection coll = db.getCollection("testCollection");
     BasicDBObject query1 = new BasicDBObject();
     BasicDBObject query = new BasicDBObject("_id",0);
  
     myDoc = coll.find(query1,query);

   while(myDoc.hasNext()) {
       System.out.println("**************************  " +  myDoc.next());
   }
     //DBObject myDoc = coll.find(query1,query);
    //System.out.println("**************************  " +  myDoc);

} finally {
   myDoc.close();
}*/
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();


    //********* BEGIN stage2 exercise part 2-of-2 ***********
    // attach the word spout to the topology - parallelism of 10
    //builder.setSpout("word", new TestWordSpout(), 10);
   // builder.setSpout("rand-sentence", new RandomSentenceSpout(), 10);
    builder.setSpout("rand-sentence", new temperatureSpout(), 10);

    // attach the exclamation bolt to the topology - parallelism of 3
    //builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
    builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("rand-sentence");

    // attach another exclamation bolt to the topology - parallelism of 2
    //builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
    builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("rand-sentence");

    //********* END stage2 exercise part 2-of-2 ***********

    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("exclamation", conf, builder.createTopology());

      // let the topology run for 30 seconds. note topologies never terminate!
      Thread.sleep(30000);

      // kill the topology
      cluster.killTopology("exclamation");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}