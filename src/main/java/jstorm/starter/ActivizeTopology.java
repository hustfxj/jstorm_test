package jstorm.starter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import jstorm.starter.bolt.CompanyAggregatorBolt;
import jstorm.starter.bolt.DBWriterBolt;
import jstorm.starter.bolt.FileWriterBolt;
import jstorm.starter.bolt.MessageReceiverBolt;
import jstorm.starter.bolt.UserAggregatorBolt;
import jstorm.starter.other.IServiceBusQueueDetail;
import jstorm.starter.other.ServiceBusQueueConnection;
import jstorm.starter.spoult.ServiceBusQueueSpout;

import backtype.storm.Config;
import com.alibaba.jstorm.local.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
//This is a basic example of a Storm topology about how to build connecitons with other SqlServer/ServiceBus/AMQP,
//then find matching information  and abstract useful information to file. 
public class ActivizeTopology {

	public static void main(String[] args) throws Exception {

		Properties prop = new Properties();
		try {
			prop.load(ActivizeTopology.class.getClassLoader()
					.getResourceAsStream("config.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		String connectionString = prop.getProperty("SB_CONNECTION");
		String queueName = "fitnessdata";
		String spoutId = "Spout";

		IServiceBusQueueDetail connection = new ServiceBusQueueConnection(
				connectionString, queueName);
		TopologyBuilder builder = new TopologyBuilder();

		// sets spout, connects to ServiceBus Queue
		builder.setSpout(spoutId, new ServiceBusQueueSpout(connection), 6);

		// sets bolts
		builder.setBolt("MessageReceiverBolt", new MessageReceiverBolt(), 3)
				.shuffleGrouping(spoutId);
		builder.setBolt("UserAggregatorBolt", new UserAggregatorBolt(), 3)
				.fieldsGrouping("MessageReceiverBolt", new Fields("deviceId"));
		builder.setBolt("CompanyAggregatorBolt", new CompanyAggregatorBolt(), 3)
				.fieldsGrouping("MessageReceiverBolt", new Fields("companyId"));
		builder.setBolt("UserDisplayBolt", new FileWriterBolt(), 3)
				.shuffleGrouping("UserAggregatorBolt");


		Config conf = new Config();
		conf.setDebug(false);
		// conf.setMaxTaskParallelism(3);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("ActivizeTopology", conf,
				builder.createTopology());

		// Waits for key press to stop program
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		br.readLine();

		cluster.shutdown();
	}
}
