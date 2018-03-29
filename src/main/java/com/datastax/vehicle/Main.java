package com.datastax.vehicle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.KillableRunner;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.vehicle.model.Vehicle;
import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;

public class Main {
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	private static int TOTAL_VEHICLES = 10000;
	private static int BATCH = 10000;
	private double lat = 48.759231d;
	private double lon = 11.3926907d;
	
	private static Map<String, LatLong> vehicleLocations = new HashMap<String, LatLong>();
	private static Map<String, Double> vehicleSpeeds = new HashMap<String, Double>();
	private static Map<String, Double> vehicleTemperatures = new HashMap<String, Double>();
	private BlockingQueue<Vehicle> queue = new ArrayBlockingQueue<Vehicle>(1000);
	private VehicleDao dao;
	private static DateTime date = DateTime.now().minusDays(1);

	public Main() {

		
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		this.dao = new VehicleDao(contactPointsStr.split(","));
		
		int noOfThreads = Integer.parseInt(PropertyHelper.getProperty("noOfThreads", "50"));
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
		List<KillableRunner> tasks = new ArrayList<>();
		
		
		//Start Executors
		for (int i = 0; i < noOfThreads; i++) {
			
			KillableRunner task = new VehicleWriter(dao, queue);
			executor.execute(task);
			tasks.add(task);
		}
		
		Timer timer = new Timer();
		timer.start();

		logger.info("Creating Locations");
		createStartValues();

		//load historical data until current time, spacing each reading by 10 seconds
		while (date.isBefore(DateTime.now())) {
			date = date.plusSeconds(10);
			
			logger.info("Updating " + date.toString());			
			updateVehicles(date, queue);			
		}

		// load current data
		while (true) {
			date = DateTime.now();
			
			logger.info("Updating " + date.toString());			
			updateVehicles(date, queue);
			sleep(10);
		}

		//ThreadUtils.shutdown(tasks, executor);
		//System.exit(0);
	}

	private void updateVehicles(DateTime date, BlockingQueue<Vehicle> queue) {

		Vehicle vehicle;
		for (int i = 0; i < BATCH; i++) {
			String vehicleId = new Double(Math.random() * TOTAL_VEHICLES).intValue() + 1 + "";

			/*
			if (vehicle state is ON) {
			   vehicle = updateVehicle(date, vehicleId);
			 } else if (vehicle state is OFF && interval since last event > X mins && turned on less than Y times) {
			   turn on vehicle
			   vehicle = updateVehicle(date, vehicleId);
			 } else {
			   chck if it has to be turned on
			 }
			 */
			vehicle = updateVehicle(date, vehicleId); //TODO move this


			try {
				queue.put(vehicle);
			
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}		
	}

	private Vehicle updateVehicle(DateTime date, String vehicleId) {
		Vehicle vehicle;//Get the current values
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

		vehicle = new Vehicle(vehicleId, date.toDate(), location, tile1, tile2, temperature, speed);
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
			lon += .0001d;
		else
			lon -= .0001d;

		if (Math.random() < .5)
			lat += .0001d;
		else
			lat -= .0001d;
		
		return new LatLong(lat,lon);
	}

	private void createStartValues() {

		for (int i = 0; i < TOTAL_VEHICLES; i++) {
			double lat = getRandomLat();
			double lon = getRandomLng();

			vehicleLocations.put("" + (i + 1), new LatLong(lat, lon));
			vehicleSpeeds.put("" + (i + 1), 0d);
			vehicleTemperatures.put("" + (i + 1), 22d);
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

	private void sleep(int ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();
	}
}
