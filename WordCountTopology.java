package org.virginia.do5xb.twitter;

import java.util.Arrays;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import org.virginia.do5xb.twitter.TwitterSpoutCS4740;

public class WordCountTopology {

  //Entry point for the topology
  public static void main(String[] args) throws Exception {
  //Used to build the topology
      String consumerKey ="LpSVnydzwzS2i0lAUC0Xlm9Yx";
      String consumerSecret = "JpZisPcMMfj74Ap3TM9XWagYKty7D5C7TZmLi1CRXleXdfUfDS";
      String accessToken = "838862162-Ida83qXn2zk7kA1mBUqVf9oQAEWb3PGS0N7sJ2fY";
      String accessTokenSecret = "2cjWT4rzwgT1JDrC2L5GSMzgi7hcRvXzTmXenEyytswAI";
      //String[] arguments = args.clone();
      String[] keyWords = {"Celtics"};
      
    TopologyBuilder builder = new TopologyBuilder();
    //Add the spout, with a name of 'spout'
    //and parallelism hint of 5 executors
    builder.setSpout("twitter", new TwitterSpoutCS4740(consumerKey, consumerSecret, accessToken,accessTokenSecret, keyWords));
    //Add the SplitSentence bolt, with a name of 'split'
    //and parallelism hint of 8 executors
    //shufflegrouping subscribes to the spout, and equally distributes
    //tuples (sentences) across instances of the SplitSentence bolt
    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("twitter");
    //Add the counter, with a name of 'count'
    //and parallelism hint of 12 executors
    //fieldsgrouping subscribes to the split bolt, and
    //ensures that the same word is sent to the same instance (group by field 'word')
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    //new configuration
    Config conf = new Config();
    //Set to false to disable debug information when
    // running in production on a cluster
    conf.setDebug(false);

    //If there are arguments, we are running on a cluster
    //  if (args != null && args.length > 0) {
      //parallelism hint to set the number of workers
    //         conf.setNumWorkers(3);
      //submit the topology
    //   StormSubmitter.submitTopology(consumerKey, conf, builder.createTopology());
    // }
    //Otherwise, we are running locally
    //    else {
      //Cap the maximum number of executors that can be spawned
      //for a component to 3
        conf.setMaxTaskParallelism(3);
      //LocalCluster is used to run locally
           LocalCluster cluster = new LocalCluster();
      //submit the topology
           cluster.submitTopology("word-count", conf, builder.createTopology());
      //sleep
      Thread.sleep(50000);
      //shut down the cluster
            cluster.shutdown();
    // }
  }
}
