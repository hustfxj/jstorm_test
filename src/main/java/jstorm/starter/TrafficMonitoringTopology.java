package jstorm.starter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.local.LocalCluster;

import jstorm.starter.bolt.MapMatchingBolt;
import jstorm.starter.bolt.SpeedCalculatorBolt;
import jstorm.starter.constants.TrafficMonitoringConstants.Conf;
import jstorm.starter.model.gis.RoadGridList;
import jstorm.starter.tool.BasicTopology;
import jstorm.starter.util.Configuration;
import static jstorm.starter.constants.TrafficMonitoringConstants.*;


public class TrafficMonitoringTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoringTopology.class);
    
    private int mapMatcherThreads;
    private int speedCalcThreads;

    public TrafficMonitoringTopology(String topologyName, Map config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        
        mapMatcherThreads = config.getInt(Conf.MAP_MATCHER_THREADS, 1);
        speedCalcThreads  = config.getInt(Conf.SPEED_CALCULATOR_THREADS, 1);
    }
    
    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED,
                Field.BEARING, Field.LATITUDE, Field.LONGITUDE));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.MAP_MATCHER, new MapMatchingBolt(), mapMatcherThreads)
               .shuffleGrouping(Component.SPOUT);
        
        builder.setBolt(Component.SPEED_CALCULATOR, new SpeedCalculatorBolt(), speedCalcThreads)
               .fieldsGrouping(Component.MAP_MATCHER, new Fields(Field.ROAD_ID));
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .shuffleGrouping(Component.SPEED_CALCULATOR);
        
        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }

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
    private static Map conf = new HashMap<Object, Object>();

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("Please input configuration file");
			System.exit(-1);
		}
		LoadProperty(args[0]);
		Configuration config=new Configuration();
		config = Configuration.fromMap(conf);
        String shapeFile = config.getString(Conf.MAP_MATCHER_SHAPEFILE);
        RoadGridList sectors = new RoadGridList(config, shapeFile);
        
		LoadProperty(args[0]);
		TrafficMonitoringTopology topology =new TrafficMonitoringTopology("TrafficTopology",conf);
		topology.initialize();
		
        //test local
/*		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("TrafficTopology", conf, topology.buildTopology());
		Thread.sleep(60000);
		cluster.shutdown();*/
		//test
		StormSubmitter.submitTopology("TrafficTopology", conf, topology.buildTopology());
	}
    
}
