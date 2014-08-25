package jstorm.starter.bolt;

import java.util.HashMap;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UserAggregatorBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	public static HashMap<String, HashMap<String, Double>> userData = new HashMap<String, HashMap<String, Double>>();

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String deviceId = tuple.getString(0);
		String companyId = tuple.getString(1);
		String date = tuple.getString(2);
		String time = tuple.getString(3);
		double calorie = tuple.getDouble(4);
		double distance = tuple.getDouble(5);
		double runStep = tuple.getDouble(6);
		double totalStep = tuple.getDouble(7);
		double walkStep = tuple.getDouble(8);
		double deltaCalorie = tuple.getDouble(9);
		double deltaDistance = tuple.getDouble(10);
		double deltaRunStep = tuple.getDouble(11);
		double deltaTotalStep = tuple.getDouble(12);
		double deltaWalkStep = tuple.getDouble(13);

		if (!userData.containsKey(deviceId)) {
			userData.put(deviceId, new HashMap<String, Double>());
		}

		userData.get(deviceId).put("calorie", calorie);
		userData.get(deviceId).put("distance", distance);
		userData.get(deviceId).put("runStep", runStep);
		userData.get(deviceId).put("totalStep", totalStep);
		userData.get(deviceId).put("walkStep", walkStep);

		collector.emit(new Values(deviceId, companyId, date, time, calorie,
				distance, runStep, totalStep, walkStep, deltaCalorie,
				deltaDistance, deltaRunStep, deltaTotalStep, deltaWalkStep));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("deviceId", "companyId", "date", "time",
				"calorie", "distance", "runStep", "totalStep", "walkStep",
				"deltaCalorie", "deltaDistance", "deltaRunStep",
				"deltaTotalStep", "deltaWalkStep"));
	}
}