package com.cone.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

public class PrintingBolt extends BaseRichBolt {

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {

    }

    public void execute(Tuple input) {
        System.out.println("---------[Printing on Console] " + input);

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
