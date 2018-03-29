package com.datastax.vehicle;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.geometry.Point;
import com.datastax.vehicle.model.Vehicle;
import com.github.davidmoten.geo.LatLong;


public class VehicleDao {
	
	private DseSession session;
	private static String keyspaceName = "datastax";
	private static String vehicleTable = keyspaceName + ".vehicle";
	private static String currentLocationTable = keyspaceName + ".current_location";
	private static String vehicleStateTable = keyspaceName + ".vehicle_state";

	private static final String INSERT_INTO_VEHICLE = "Insert into " + vehicleTable + " (vehicle, day, date, lat_long, tile, speed, temperature) values (?,?,?,?,?,?,?);";
	private static final String INSERT_INTO_CURRENTLOCATION = "Insert into " + currentLocationTable + "(vehicle, tile1, tile2, lat_long, date, speed, temperature) values (?,?,?,?,?,?,?)" ;
	private static final String INSERT_INTO_VEHICLESTATE = "Insert into " + vehicleStateTable + "(vehicle, day, state_change_time, vehicle_state) values (?,?,?,?)" ;

	private static final String QUERY_BY_VEHICLE = "select * from " + vehicleTable + " where vehicle = ? and day = ?";
	
	private PreparedStatement insertVehicle;
	private PreparedStatement insertCurrentLocation;
	private PreparedStatement insertVehicleState;
	private PreparedStatement queryVehicle;
	private PreparedStatement queryVehicleState;

	
	private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd"); 

	public VehicleDao(String[] contactPoints) {

		DseCluster cluster = DseCluster.builder()
				.addContactPoints(contactPoints).build();
		
		this.session = cluster.connect();

		this.insertVehicle = session.prepare(INSERT_INTO_VEHICLE);
		this.insertCurrentLocation = session.prepare(INSERT_INTO_CURRENTLOCATION);
		this.insertVehicleState = session.prepare(INSERT_INTO_VEHICLESTATE);
		
		this.queryVehicle = session.prepare(QUERY_BY_VEHICLE);
	}
	
	public void insertVehicleData(Vehicle vehicle){

		session.execute(insertVehicle.bind(vehicle.getVehicle(), dateFormatter.format(vehicle.getDate()), vehicle.getDate(), 
				new Point(vehicle.getLatLong().getLat(),vehicle.getLatLong().getLon()), vehicle.getTile2(), vehicle.getSpeed(), vehicle.getTemperature()));
		
		session.execute(insertCurrentLocation.bind(vehicle.getVehicle(), vehicle.getTile(), vehicle.getTile2(), 
				 new Point(vehicle.getLatLong().getLat(),vehicle.getLatLong().getLon()), vehicle.getDate(),vehicle.getSpeed(), vehicle.getTemperature()));
	}


	public void insertVehicleStatus(String vehicleId, Date stateDate, String status) {
		session.execute(insertVehicleState.bind(vehicleId, dateFormatter.format(stateDate), stateDate, status));
	}

	public List<Vehicle> getVehicleMovements(String vehicleId, String dateString) {
		ResultSet resultSet = session.execute(this.queryVehicle.bind(vehicleId, dateString));
		
		List<Vehicle> vehicleMovements = new ArrayList<Vehicle>();
		List<Row> all = resultSet.all();
		
		for (Row row : all){
			Date date = row.getTimestamp("date");
			String lat_long = row.getString("lat_long");
			String tile = row.getString("tile2");
			Double temperature = row.getDouble("temperature");
			Double speed = row.getDouble("speed");

			
			Double lat = Double.parseDouble(lat_long.substring(0, lat_long.lastIndexOf(",")));
			Double lng = Double.parseDouble(lat_long.substring(lat_long.lastIndexOf(",") + 1));
			
			Vehicle vehicle = new Vehicle(vehicleId, date, new LatLong (lat, lng), tile, "", temperature, speed);			
			vehicleMovements.add(vehicle);
		}
		
		return vehicleMovements;
	}

	public List<Vehicle> searchVehiclesByLonLatAndDistance(int distance, LatLong latLong) {
		
		String cql = "select * from " + currentLocationTable
				+ " where solr_query = '{\"q\": \"*:*\", \"fq\": \"{!geofilt sfield=lat_long pt=" 
				+ latLong.getLat() + "," + latLong.getLon() + " d=" + distance + "}\"}'  limit 1000";
		ResultSet resultSet = session.execute(cql);
		
		List<Vehicle> vehicleMovements = new ArrayList<Vehicle>();
		List<Row> all = resultSet.all();
		
		for (Row row : all){
			Date date = row.getTimestamp("date");
			String vehicleId = row.getString("vehicle");
			String lat_long = row.getString("lat_long");
			String tile = row.getString("tile");
			Double temperature = row.getDouble("temperature");
			Double speed = row.getDouble("speed");

			Double lat = Double.parseDouble(lat_long.substring(0, lat_long.lastIndexOf(",")));
			Double lng = Double.parseDouble(lat_long.substring(lat_long.lastIndexOf(",") + 1));
			
			Vehicle vehicle = new Vehicle(vehicleId, date, new LatLong (lat, lng), tile, "", temperature, speed);			
			vehicleMovements.add(vehicle);
		}
		
		return vehicleMovements;
	}

	public List<Vehicle> getVehiclesByTile(String tile) {
		String cql = "select * from " + currentLocationTable + " where solr_query = '{\"q\": \"tile1: " + tile + "\"}' limit 1000";
		ResultSet resultSet = session.execute(cql);

		List<Vehicle> vehicleMovements = new ArrayList<Vehicle>();
		List<Row> all = resultSet.all();
		
		for (Row row : all){
			Date date = row.getTimestamp("date");
			String vehicleId = row.getString("vehicle");
			String lat_long = row.getString("lat_long");
			String tile1 = row.getString("tile1");
			String tile2 = row.getString("tile2");
			Double temperature = row.getDouble("temperature");
			Double speed = row.getDouble("speed");
			
			Double lat = Double.parseDouble(lat_long.substring(0, lat_long.lastIndexOf(",")));
			Double lng = Double.parseDouble(lat_long.substring(lat_long.lastIndexOf(",") + 1));
			
			Vehicle vehicle = new Vehicle(vehicleId, date, new LatLong (lat, lng), tile1, tile2, temperature, speed);			
			vehicleMovements.add(vehicle);
		}
		
		return vehicleMovements;
	}

}
