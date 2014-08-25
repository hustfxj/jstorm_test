package jstorm.starter.sink.formatter;

import jstorm.starter.util.Configuration;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;



public abstract class Formatter {
    protected Configuration config;
    protected TopologyContext context;
    
    public void initialize(Configuration config, TopologyContext context) {
        this.config = config;
        this.context = context;
    }
    
    public abstract String format(Tuple tuple);
}
