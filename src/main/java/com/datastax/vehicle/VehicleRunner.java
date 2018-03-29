package com.datastax.vehicle;

import java.util.concurrent.BlockingQueue;

import org.joda.time.DateTime;

import com.datastax.vehicle.model.Vehicle;

public class VehicleRunner{

	private BlockingQueue<Vehicle> queue;
	private VehicleUpdater vehicleUpdater;
	private String vehicleId;
	private DateTime driveUntil;
	private DateTime sleepUntil;
	
	
	public enum Status { STARTING, STOPPING, STOPPED, DRIVING};
	private Status status;

	public VehicleRunner(String vehicleId, VehicleUpdater vehicleUpdater, BlockingQueue<Vehicle> queue, DateTime date) {
		this.vehicleId = vehicleId;
		this.queue = queue;
		this.vehicleUpdater = vehicleUpdater;
		
		if (Math.random() > .8){
			status = Status.STOPPED;
			int minutesToSleep = new Double(Math.random() * 720).intValue();			
			sleepUntil = date.plusMinutes(minutesToSleep);
		}else{
			int minutesToDriveAround = new Double(Math.random() * 110).intValue() + 10;
			driveUntil = date.plusMinutes(minutesToDriveAround);
			status = Status.DRIVING;
			vehicleUpdater.startVehicle(vehicleId, date);
		}
	}
	
	public void nextStep(DateTime date) throws Exception {
		
		if (status.equals(Status.STOPPED)){
			if (!date.isBefore(sleepUntil)){
				status = Status.STARTING;
			}			
		}
		
		if (status.equals(Status.STOPPING)){
			vehicleUpdater.stopVehicle(vehicleId, date);
			
			int minutesToSleep = new Double(Math.random() * 720).intValue();
			
			sleepUntil = date.plusMinutes(minutesToSleep);
			status = Status.STOPPED;

		}
		if (status.equals(Status.DRIVING)){
			if (date.isBefore(driveUntil)){
				queue.put(vehicleUpdater.updateVehicle(vehicleId, date));
			}else{
				status = Status.STOPPING;
			}
		}
		
		if (status.equals(Status.STARTING)){
			int minutesToDriveAround = new Double(Math.random() * 110).intValue() + 10;
			driveUntil = date.plusMinutes(minutesToDriveAround);
			status = Status.DRIVING;
			vehicleUpdater.startVehicle(vehicleId, date);
		}		
	}
}
