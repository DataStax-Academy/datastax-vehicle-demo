package com.datastax.vehicle.webservice;

import java.util.List;

import org.joda.time.DateTime;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.vehicle.VehicleDao;
import com.datastax.vehicle.model.Vehicle;
import com.github.davidmoten.geo.LatLong;

public class VehicleService {

	public static VehicleDao dao;
	
	public VehicleService(){
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		if (dao == null){
			dao = new VehicleDao(contactPointsStr.split(","));		
		}
	}

	public List<Vehicle> getVehicleMovements(String vehicle, String dateString) {
		
		return dao.getVehicleMovements(vehicle, dateString);
	}
	
	public List<Vehicle> getVehiclesByTile(String tile){
		
		return dao.getVehiclesByTile(tile);
		
	}
	
	public List<Vehicle> searchVehiclesByLonLatAndDistance(int distance, LatLong latLong){
		return dao.searchVehiclesByLonLatAndDistance(distance, latLong);
	}
	
	public List<Vehicle> searchAreaTimeLastPosition(DateTime from, DateTime to){
		
		return dao.getVehiclesByAreaTimeLastPosition(from, to);
	}
}
