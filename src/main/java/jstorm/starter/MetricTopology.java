package jstorm.starter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


import jstorm.starter.spoult.MetricSpoult;
import jstorm.starter.bolt.MetricBolt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TaskStats;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.utils.JStormUtils;

//This is a basic example of a Storm topology about how to get defined metric.  Of course,you 
//can define a lot of  topologys freely.
public class MetricTopology {

	private static final Log LOG = LogFactory.getLog(MetricTopology.class);
	private final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
	private final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "bolt.parallel";
	private final static String TOPOLOGY_NUMS="topology.nums";
    //number of levels of bolts per topolgy
	private final static String TOPOLOGY_BOLTS_NUMS="topology.bolt.nums";
	//How often should metrics be collected
	private final static String TOPOLOGY_POLLFREQSEC="topology.pollFreq";
	//How long should the benchmark run for
	private final static String TOPOLOGY_RUNTIMESEC="topology.testRunTime";
	//message size
	private final static String TOPOLOGY_MESSAGE_SIZES="topology.messagesize";

	  private static class MetricsState {
	    long transferred = 0;
	    long lastTime = 0;
	    int slotsUsed = 0;
	    double lastThroughput = 0.00;
	  }


	private static Map conf = new HashMap<Object, Object>();

	private static void LoadProperty(String prop) {
		Properties properties = new Properties();

		try {
			InputStream stream = new FileInputStream(prop);
			properties.load(stream);
		} catch (FileNotFoundException e) {
			System.out.println("No such file " + prop);
		} catch (Exception e1) {
			e1.printStackTrace();

			return;
		}

		conf.putAll(properties);
	}
		
	  public void metrics(Nimbus.Client client, int poll, int total,int _messageSize) throws Exception {
	    System.out.println("status\ttopologies\ttotalSlots\tslotsUsed\ttargetTasks\ttargetTasksWithMetrics\ttime\ttime-diff ms\temitted\tthroughput (Mb/s)");
	    MetricsState state = new MetricsState();
	    long pollMs = poll * 1000;
	    long now = System.currentTimeMillis();
	    state.lastTime = now;
	    long startTime = now;
	    long cycle = 0;
	    long sleepTime;
	    long wakeupTime;
	    while (metrics(client, now, state, "WAITING",_messageSize)) {
	      now = System.currentTimeMillis();
	      cycle = (now - startTime)/pollMs;
	      wakeupTime = startTime + (pollMs * (cycle + 1));
	      sleepTime = wakeupTime - now;
	      if (sleepTime > 0) {
	        Thread.sleep(sleepTime);
	      }
	      now = System.currentTimeMillis();
	    }
	    long wantingtransferred=state.transferred;

	    now = System.currentTimeMillis();
	    cycle = (now - startTime)/pollMs;
	    wakeupTime = startTime + (pollMs * (cycle + 1));
	    sleepTime = wakeupTime - now;
	    if (sleepTime > 0) {
	      Thread.sleep(sleepTime);
	    }
	    now = System.currentTimeMillis();
	    long end = now + (total * 1000);
	    double sumThroughput = 0;
	    int nPolls = 0;
	    do {
	      nPolls++;
	      metrics(client, now, state, "RUNNING",_messageSize);
	      sumThroughput += state.lastThroughput;
	      
	      now = System.currentTimeMillis();
	      cycle = (now - startTime)/pollMs;
	      wakeupTime = startTime + (pollMs * (cycle + 1));
	      sleepTime = wakeupTime - now;
	      if (sleepTime > 0) {
	        Thread.sleep(sleepTime);
	      }
	      now = System.currentTimeMillis();
	    } while (now < end);
	    double avgThroughput = (sumThroughput)/ nPolls;
	    long avgtransferred=(state.transferred-wantingtransferred)/nPolls;
	    System.out.println("RUNNING " + nPolls + " Polls, AVG_Throughput = " + avgThroughput + " Mb/s");
	    System.out.println("RUNNING " + nPolls + " Polls, AVG_Transferred = " + avgtransferred);
	  }

	  public boolean metrics(Nimbus.Client client, long now, MetricsState state, String message,int _messageSize) throws Exception {
	    ClusterSummary summary = client.getClusterInfo();
	    int totalSlots = 0;
	    int totalUsedSlots = 0;
	    for (SupervisorSummary sup: summary.get_supervisors()) {
	      totalSlots += sup.get_num_workers();
	      totalUsedSlots += sup.get_num_used_workers();
	    }
	    int slotsUsedDiff = totalUsedSlots - state.slotsUsed;
	    state.slotsUsed = totalUsedSlots;

	    int numTopologies = summary.get_topologies_size();
	    long totalTransferred = 0;
	    int targetTasks = 0;
	    int targetTasksWithMetrics = 0;
	    for (TopologySummary ts: summary.get_topologies()) {
	      String id = ts.get_id();
	      TopologyInfo info = client.getTopologyInfo(id);
	      for (TaskSummary taskSummary: info.get_tasks()) {
	        if ("messageSpout".equals(taskSummary.get_component_id())) {
	            targetTasks++;
	            TaskStats stats = taskSummary.get_stats();
	        //    System.out.println("fxj_stats"+stats);
	            if (stats != null) {
	                Map<String,Map<String,Long>> emitted = stats.get_emitted();
	                if ( emitted != null) {
	                    Map<String, Long> e2 = emitted.get("All-time");
	                    if (e2 != null && !e2.isEmpty()) {
	                        targetTasksWithMetrics++;
	                        //The SOL messages are always on the default stream, so just count those
	                        Long dflt = e2.get("default");
	                        if (dflt != null) {
	                            totalTransferred += dflt;
	                        }
	                    }
	                }
	            }
	        }
	      }
	    }
	    long transferredDiff = totalTransferred - state.transferred;
	    state.transferred = totalTransferred;
	    long time = now - state.lastTime;
	    state.lastTime = now;
	    double throughput = (transferredDiff == 0 || time == 0) ? 0.0 : ((transferredDiff * _messageSize)/(1024.0 * 1024.0)/(time/1000.0));
	    
	    state.lastThroughput = throughput;
	    System.out.println(message+"\t"+numTopologies+"\t"+totalSlots+"\t"+totalUsedSlots+"\t"+targetTasks+"\t"+targetTasksWithMetrics+"\t"+now+"\t"+time+"\t"+transferredDiff+"\t"+throughput);

	    return !(totalUsedSlots > 0 && slotsUsedDiff == 0 && targetTasks > 0 && targetTasksWithMetrics >= targetTasks);
	  } 

	 
	  public void realMain(String[] args) throws Exception {
		LoadProperty(args[0]);
	    Map clusterConf = Utils.readStormConfig();
	    clusterConf.putAll(Utils.readCommandLineOpts());
	    Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();


	    int _numWorkers=JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS), 1);
	    if (_numWorkers <= 0) {
	      throw new IllegalArgumentException("Need at least one worker");
	    }
	    boolean _ackEnabled=false;
	    int _ackers=JStormUtils.parseInt(conf.get(Config.TOPOLOGY_ACKERS), 0);
	    if (_ackers>0) {
	    	_ackEnabled =true;
		    }
        int _numTopologies=JStormUtils.parseInt(conf.get(TOPOLOGY_NUMS), 0);
		int _spoutParallel = JStormUtils.parseInt(
				conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
		int _boltParallel = JStormUtils.parseInt(
				conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT), 2);
		String _name = (String) conf.get(Config.TOPOLOGY_NAME);
		if (_name == null) 
			_name = "MetricTest";
		int _messageSize=JStormUtils.parseInt(conf.get(TOPOLOGY_MESSAGE_SIZES), 10);
		
	    try {
	      for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {
	        TopologyBuilder builder = new TopologyBuilder();
	        LOG.info("Adding in "+_spoutParallel+" spouts");
	        builder.setSpout("messageSpout", 
	            new MetricSpoult(_messageSize, _ackEnabled), _spoutParallel);
	        LOG.info("Adding in "+_boltParallel+" bolts");
	        builder.setBolt("messageBolt1", new MetricBolt(), _boltParallel)
	            .shuffleGrouping("messageSpout");
	        for (int levelNum = 2; levelNum <= JStormUtils.parseInt(conf.get(TOPOLOGY_BOLTS_NUMS), 0); levelNum++) {
	          LOG.info("Adding in "+_boltParallel+" bolts at level "+levelNum);
	          builder.setBolt("messageBolt"+levelNum, new MetricBolt(), _boltParallel)
	              .shuffleGrouping("messageBolt"+(levelNum - 1));
	        }
	        
			int ackerNum = JStormUtils.parseInt(
					conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 0);
			Config.setNumAckers(conf, ackerNum);
			int workerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS),
					1);
			conf.put(Config.TOPOLOGY_WORKERS, workerNum);
			
	
	        StormSubmitter.submitTopology(_name+"_"+topoNum, conf, builder.createTopology());
	      }
	      int  _pollFreqSec=JStormUtils.parseInt(conf.get(TOPOLOGY_POLLFREQSEC), 10);
	      int  _testRunTimeSec=JStormUtils.parseInt(conf.get(TOPOLOGY_RUNTIMESEC), 100);
	      metrics(client, _pollFreqSec, _testRunTimeSec,_messageSize);
	    } finally {
	      //Kill it right now!!!
	      KillOptions killOpts = new KillOptions();
	      killOpts.set_wait_secs(1);

	      for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {
	        LOG.info("KILLING "+_name+"_"+topoNum);
	        try {
	          client.killTopologyWithOpts(_name+"_"+topoNum, killOpts);
	        } catch (Exception e) {
	          LOG.error("Error tying to kill "+_name+"_"+topoNum,e);
	        }
	      }
	    }
	  }
	  
	  public static void main(String[] args) throws Exception {
	    new MetricTopology().realMain(args);
	  }

}
