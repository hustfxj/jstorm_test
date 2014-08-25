package jstorm.starter.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MetricBolt extends BaseRichBolt {
	  private OutputCollector _collector;

	  public MetricBolt() {
	    //Empty
	  }

	  @Override
	  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	    _collector = collector;
	  }

	  @Override
	  public void execute(Tuple tuple) {
	    _collector.emit(tuple, new Values(tuple.getString(0)));
	    _collector.ack(tuple);
	  }

	  @Override
	  public void cleanup() {
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("message"));
	  }

}
