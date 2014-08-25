package jstorm.starter.bolt;

import java.util.Hashtable;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CompanyAggregatorBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	public static Hashtable<String, Hashtable<String, Double>> companyData = new Hashtable<String, Hashtable<String, Double>>();

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

		// temporarily turns the String into a double
		String[] splitDate = date.split("/");
		String stringDate = splitDate[0] + splitDate[1] + splitDate[2];
		double doubleDate = Double.parseDouble(stringDate);

		if (!companyData.containsKey(companyId)) {
			companyData.put(companyId, new Hashtable<String, Double>());
			companyData.get(companyId).put("date", doubleDate);
			companyData.get(companyId).put("deltaCalorie", 0.0);
			companyData.get(companyId).put("deltaDistance", 0.0);
			companyData.get(companyId).put("deltaRunStep", 0.0);
			companyData.get(companyId).put("deltaTotalStep", 0.0);
			companyData.get(companyId).put("deltaWalkStep", 0.0);
		} else if (companyData.get(companyId).get("date") != doubleDate) {
			companyData.get(companyId).put("date", doubleDate);
			companyData.get(companyId).put("deltaCalorie", deltaCalorie);
			companyData.get(companyId).put("deltaDistance", deltaDistance);
			companyData.get(companyId).put("deltaRunStep", deltaRunStep);
			companyData.get(companyId).put("deltaTotalStep", deltaTotalStep);
			companyData.get(companyId).put("deltaWalkStep", deltaWalkStep);
		} else {
			companyData.get(companyId).put(
					"deltaCalorie",
					companyData.get(companyId).get("deltaCalorie")
							+ deltaCalorie);
			companyData.get(companyId).put(
					"deltaDistance",
					companyData.get(companyId).get("deltaDistance")
							+ deltaDistance);
			companyData.get(companyId).put(
					"deltaRunStep",
					companyData.get(companyId).get("deltaRunStep")
							+ deltaRunStep);
			companyData.get(companyId).put(
					"deltaTotalStep",
					companyData.get(companyId).get("deltaTotalStep")
							+ deltaTotalStep);
			companyData.get(companyId).put(
					"deltaWalkStep",
					companyData.get(companyId).get("deltaWalkStep")
							+ deltaWalkStep);
		}

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