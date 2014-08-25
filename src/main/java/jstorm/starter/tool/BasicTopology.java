package jstorm.starter.tool;

import java.util.Map;

import backtype.storm.Config;
import jstorm.starter.constants.BaseConstants.BaseConf;
import jstorm.starter.sink.BaseSink;

/**
 * The basic topology has only one spout and one sink, configured by the default
 * configuration keys.
 
 */
public abstract class BasicTopology extends AbstractTopology {
    protected AbstractSpout spout;
    protected BaseSink sink;
    protected int spoutThreads;
    protected int sinkThreads;
    
    public BasicTopology(String topologyName, Map config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        spout = loadSpout();
        sink  = loadSink();
        
        spoutThreads = config.getInt(getConfigKey(BaseConf.SPOUT_THREADS), 1);
        sinkThreads  = config.getInt(getConfigKey(BaseConf.SINK_THREADS), 1);
    }
}
