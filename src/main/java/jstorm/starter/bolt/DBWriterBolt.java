package jstorm.starter.bolt;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DBWriterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String table = tuple.getString(0);
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

		if (table.equals("Raw")) {
			writeRawQuery(table, deviceId, companyId, date, time, calorie, 
					distance, runStep, totalStep, walkStep, deltaCalorie, deltaDistance,
					deltaRunStep, deltaTotalStep, deltaWalkStep);
		} else if (table.equals("DailyCompany")) {
			
		} else if (table.equals("DailyUser")) {
			
		} else if (table.equals("RTCompany")) {
			
		} else if (table.equals("RTUser")) {
			
		} else {
			System.out.println("ERROR: This is not a valid table name!");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("DBWriter"));
	}

	public static void connectDB(String connectionString, String sqlString) {

		Connection connection = null; // For making the connection
		Statement statement = null; // For the SQL statement
		ResultSet resultSet = null; // For the result set

		try {
			// Ensure the SQL Server driver class is available.
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

			// Establish the connection.
			connection = DriverManager.getConnection(connectionString);

			// Use the connection to create the SQL statement.
			statement = connection.createStatement();

			// Execute the statement.
			statement.executeUpdate(sqlString);

		}
		// Exception handling
		catch (ClassNotFoundException cnfe) {
			System.out.println("ClassNotFoundException " + cnfe.getMessage());
		} catch (Exception e) {
			System.out.println("Exception " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				// Close resources.
				if (null != connection)
					connection.close();
				if (null != statement)
					statement.close();
				if (null != resultSet)
					resultSet.close();
			} catch (SQLException sqlException) {
				// No additional action if close() statements fail.
			}
		}

	}

	public static void loadPropertiesDB(String sqlString) {
		Properties prop = new Properties();
		try {
			prop.load(DBWriterBolt.class.getClassLoader().getResourceAsStream(
					"config.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		String connectionString = prop.getProperty("SQL_CONNECTION");

		connectDB(connectionString, sqlString);
	}

	public static void writeRawQuery(String table, String deviceId, String companyId,
			String date, String time, double calorie, double distance,
			double runStep, double totalStep, double walkStep,
			double deltaCalorie, double deltaDistance, double deltaRunStep,
			double deltaTotalStep, double deltaWalkStep) {
		String sqlQuery = "INSERT INTO [dbo].[" + table + "]"
				+ "([deviceId],[companyId],[timestamp],[calorie],[distance],"
				+ "[runStep],[totalStep],[walkStep],[deltaCalorie],"
				+ "[deltaDistance],[deltaRunStep],[deltaTotalStep],"
				+ "[deltaWalkStep]) VALUES ('"
				+ deviceId + "','"
				+ companyId + "','"
				+ date + " "
				+ time + "',"
				+ calorie + ","
				+ distance + ","
				+ runStep + ","
				+ totalStep + ","
				+ walkStep + ","
				+ deltaCalorie + ","
				+ deltaDistance + ","
				+ deltaRunStep + ","
				+ deltaTotalStep + "," + deltaWalkStep + ")";

		loadPropertiesDB(sqlQuery);

	}

	public static void writeUserQuery(String table, String deviceId,
			String date, String time, String metricName, double metricValue) {
		String sqlQuery = "INSERT INTO [dbo].[" + table + "]"
				+ "([deviceId],[timestamp],[metric],[value]) VALUES ('"
				+ deviceId + "','" 
				+ date + " " 
				+ time + "','" 
				+ metricName + "'," 
				+ metricValue + ")";

		loadPropertiesDB(sqlQuery);
	}

	public static void writeCompanyQuery(String table, String companyId,
			String date, String time, String metricName, double metricValue) {
		String sqlQuery = "INSERT INTO [dbo].[" + table + "]"
				+ "([comanpyId],[timestamp],[metric],[value]) VALUES ('"
				+ companyId + "','" 
				+ date + " " 
				+ time + "','" 
				+ metricName + "'," 
				+ metricValue + ")";

		loadPropertiesDB(sqlQuery);
	}
}
