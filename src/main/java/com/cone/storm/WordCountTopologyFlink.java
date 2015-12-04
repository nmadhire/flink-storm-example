package com.cone.storm;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkSubmitter;
import org.apache.flink.storm.api.FlinkTopology;

public class WordCountTopologyFlink {

    //Entry point for the topology
    public static void main(String[] args) throws Exception {
        //Used to build the topology
        TopologyBuilder builder = new TopologyBuilder();
        //Add the spout, with a name of 'spout'
        builder.setSpout("spout", new RandomSentenceSpout(), 2);
        //Add the com.cone.storm.SplitSentence bolt, with a name of 'split'
        //shufflegrouping subscribes to the spout, and equally distributes
        //tuples (sentences) across instances of the com.cone.storm.SplitSentence bolt
        builder.setBolt("split", new SplitSentence(), 4).shuffleGrouping("spout");
        //Add the counter, with a name of 'count'
        //fieldsgrouping subscribes to the split bolt, and
        //ensures that the same word is sent to the same instance (group by field 'word')
        builder.setBolt("count", new WordCount(), 6).fieldsGrouping("split", new Fields("word"));

        builder.setBolt("printmsg", new PrintingBolt(), 1).shuffleGrouping("count");

        //new configuration
        Config conf = new Config();
        //conf.setDebug(true);

        //If there are arguments, we are running on a cluster
        if (args != null && args.length > 0) {
            //parallelism hint to set the number of workers
            conf.setNumWorkers(3);
            //submit the topology
            FlinkSubmitter.submitTopology(args[0], conf, FlinkTopology.createTopology(builder));
        }
        //Otherwise, we are running locally
        else {
            //Cap the maximum number of executors that can be spawned
            //for a component to 3
            conf.setMaxTaskParallelism(3);
            //LocalCluster is used to run locally
            FlinkLocalCluster cluster = new FlinkLocalCluster();
            //submit the topology
            cluster.submitTopology("word-count", conf, FlinkTopology.createTopology(builder));
            //sleep
            Thread.sleep(10000);
            //shut down the cluster
            //cluster.shutdown();
        }
    }
}
