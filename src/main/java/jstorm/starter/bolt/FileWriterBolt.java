package jstorm.starter.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FileWriterBolt extends BaseBasicBolt {

	public static File file;
	public static FileWriter fw;
	public static BufferedWriter bw;

	private static final long serialVersionUID = 1L;

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

		// Creating a file to write the data to
		// Temporary and for testing purposes
		try {
			file = new File("conf/user.out");

			if (!file.exists()) {
				file.createNewFile();
			}

			fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);

			bw.write(deviceId + "|" + companyId + "|" + date + "|" + time + "|"
					+ calorie + "|" + distance + "|" + runStep + "|"
					+ totalStep + "|" + walkStep + "\n");

			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		collector.emit(new Values("Done"));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("display"));
	}
}
