package jstorm.starter.spoult;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MetricSpoult extends BaseRichSpout {
	  private int _sizeInBytes;
	  private long _messageCount;
	  private SpoutOutputCollector _collector;
	  private String [] _messages = null;
	  private boolean _ackEnabled;
	  private Random _rand = null;
	  public MetricSpoult(int sizeInBytes, boolean ackEnabled) {
	    if(sizeInBytes < 0) {
	      sizeInBytes = 0;
	    }
	    _sizeInBytes = sizeInBytes;
	    _messageCount = 0;
	    _ackEnabled = ackEnabled;
	  }

	  public boolean isDistributed() {
	    return true;
	  }

	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    _rand = new Random();
	    _collector = collector;
	    final int differentMessages = 100;
	    _messages = new String[differentMessages];
	    for(int i = 0; i < differentMessages; i++) {
	      StringBuilder sb = new StringBuilder(_sizeInBytes);
	      //Even though java encodes strings in UCS2, the serialized version sent by the tuples
	      // is UTF8, so it should be a single byte
	      for(int j = 0; j < _sizeInBytes; j++) {
	        sb.append(_rand.nextInt(9));
	      }
	      _messages[i] = sb.toString();
	    }
	  }

	  @Override
	  public void close() {
	    //Empty
	  }

	  @Override
	  public void nextTuple() {
	    final String message = _messages[_rand.nextInt(_messages.length)];
	    if(_ackEnabled) {
	      _collector.emit(new Values(message), _messageCount);
	    } else {
	      _collector.emit(new Values(message));
	    }
	    _messageCount++;
	  }


	  @Override
	  public void ack(Object msgId) {
	    //Empty
	  }

	  @Override
	  public void fail(Object msgId) {
	    //Empty
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("message"));
	  }

}
