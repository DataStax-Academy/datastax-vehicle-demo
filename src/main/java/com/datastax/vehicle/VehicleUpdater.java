package com.datastax.vehicle;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.datastax.vehicle.model.EngineStatus;
import com.datastax.vehicle.model.VehicleStatus;
import org.apache.commons.math3.random.RandomGenerator;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.vehicle.model.Vehicle;
import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;

public class VehicleUpdater {
	
	private static Logger logger = LoggerFactory.getLogger(VehicleUpdater.class);

	private double lat = 0;
	private double lon = 0;

	private static Random RANDOM = new Random(System.currentTimeMillis());

	private static double[] LATS = {48.759231, 51.528308, 40.7127753, 52.52000659999999, 37.35410789999999,
                                    -33.8688197, 55.755826, 35.6894875, 48.7862947, 48.176834, 52.4357075,
                                    51.4682293};
	private static double[] LONS = {11.3926907, -0.3817765, -74.0059728, 13.404953999999975, -121.95523559999998,
                                    151.20929550000005, 37.617299900000035, 139.69170639999993, 9.2390514,
                                    11.5569026, 10.7606912, 7.2677247};
	
	private static Map<String, LatLong> vehicleLocations = new HashMap<String, LatLong>();
	private static Map<String, Double> vehicleSpeeds = new HashMap<String, Double>();
	private static Map<String, Double> vehicleTemperatures = new HashMap<String, Double>();

	private VehicleDao dao;

	public VehicleUpdater(VehicleDao dao){
		this.dao = dao;
	}

	public Vehicle updateVehicle(String vehicleId, DateTime date){
		
		//Get the current values
		LatLong location = vehicleLocations.get(vehicleId);
		Double speed = vehicleSpeeds.get(vehicleId);
		Double temperature = vehicleTemperatures.get(vehicleId);
		
		location = updateLocation(location);
		speed = updateSpeed(speed);
		temperature = updateTemperature(temperature);
					
		//update the values
		vehicleLocations.put(vehicleId, location);
		vehicleSpeeds.put(vehicleId, speed);
		vehicleTemperatures.put(vehicleId, temperature);
		
		String tile1 = GeoHash.encodeHash(location, 4);
		String tile2 = GeoHash.encodeHash(location, 7);
		
		HashMap<String, Double> properties = new HashMap<String, Double>(3);
		properties.put("p_Oilpressure", Math.random()*80 + 50);
		properties.put("p_Humidity", Math.random()*80 + 20);
		properties.put("p_Torque", Math.random()*180);
		
		Vehicle vehicle = new Vehicle(vehicleId, date.toDate(), location, tile1, tile2, temperature, speed);
		vehicle.setProperties(properties);
		
		return vehicle;
	}
	
	private Double updateSpeed(double speed){
		double acceleration = Math.random() < .5 ?  -3  : +3;
		
		//Change speed
		if (Math.random() > .8){
			speed = speed + acceleration;
		}
		
		if (speed > 200) return 200d;
		else if (speed < 0) return 0d; 
		else return speed;
	}
	
	private Double updateTemperature(double temperature){
		
		return (Double) (Math.random() < .5 ? temperature - .2 : temperature +.2);
	}

	private LatLong updateLocation(LatLong latLong) {
		
		double lon=latLong.getLon();
		double lat=latLong.getLat();

		if (Math.random() < .1)
			return latLong;

		if (Math.random() < .5)
			lon += .00001d;
		else
			lon -= .00001d;

		if (Math.random() < .5)
			lat += .00001d;
		else
			lat -= .00001d;
		
		return new LatLong(lat,lon);
	}

	public void createStartValues(int totalNoOfVehicles) {

		for (int i = 0; i < totalNoOfVehicles; i++) {
			int idx = RANDOM.nextInt(LATS.length);
			this.lat = LATS[idx];
			this.lon = LONS[idx];

			double lat = getRandomLat();
			double lon = getRandomLng();

			vehicleLocations.put("" + (i), new LatLong(lat, lon));
			vehicleSpeeds.put("" + (i), 0d);
			vehicleTemperatures.put("" + (i), 22d);
		}
	}

	/**
	 * @return
	 */
	private double getRandomLng() {
				
		double diff = Math.random() * 1;
		
		return (Math.random() < .5) ? this.lon + diff : this.lon - diff;
	}

	/**
	 */
	private double getRandomLat() {

		double diff = Math.random() * 2;
		
		return (Math.random() < .5) ? this.lat + diff : this.lat - diff;
	}

	public void startVehicle(String vehicleId, DateTime date) {
		logger.info("Starting Vehicle " + vehicleId + " at " + date.toString());
		dao.insertVehicleStatus(vehicleId, date, VehicleStatus.DRIVING.getStatusValue());
	}

	public void stopVehicle(String vehicleId, DateTime date) {
		logger.info("Stopping Vehicle " + vehicleId + " at " +date.toString());
		dao.insertVehicleStatus(vehicleId, date, VehicleStatus.STOPPED.getStatusValue());
	}

	public void startEngine(String vehicleId, DateTime date) {
		logger.info("Starting engine of vehicle " + vehicleId + " at " +date.toString());
		dao.insertVehicleStatus(vehicleId, date, EngineStatus.STARTED.getStatusValue());
	}

	public void stopEngine(String vehicleId, DateTime date) {
		logger.info("Stopping engine of vehicle " + vehicleId + " at " +date.toString());
		dao.insertVehicleStatus(vehicleId, date, EngineStatus.STOPPED.getStatusValue());
	}
}
