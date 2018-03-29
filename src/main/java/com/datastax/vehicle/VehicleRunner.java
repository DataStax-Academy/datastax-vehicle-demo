package com.datastax.vehicle;

import java.util.concurrent.BlockingQueue;

import com.datastax.vehicle.model.EngineStatus;
import com.datastax.vehicle.model.VehicleStatus;
import org.joda.time.DateTime;

import com.datastax.vehicle.model.Vehicle;

public class VehicleRunner{

	private BlockingQueue<Vehicle> queue;
	private VehicleUpdater vehicleUpdater;
	private String vehicleId;
	private DateTime driveUntil;
	private DateTime sleepUntil;
	private DateTime stopEngineUntil;
	
	
	//public enum Status { STARTING, STOPPING, STOPPED, DRIVING};
	private VehicleStatus vehicleStatus;
	private EngineStatus engineStatus;

	public VehicleRunner(String vehicleId, VehicleUpdater vehicleUpdater, BlockingQueue<Vehicle> queue, DateTime date) {
		this.vehicleId = vehicleId;
		this.queue = queue;
		this.vehicleUpdater = vehicleUpdater;
		
		if (Math.random() > .8){
			vehicleStatus = VehicleStatus.STOPPED;
			engineStatus = EngineStatus.STOPPED;
			int minutesToSleep = new Double(Math.random() * 720).intValue();			
			sleepUntil = date.plusMinutes(minutesToSleep);
		}else{
			int minutesToDriveAround = new Double(Math.random() * 110).intValue() + 10;
			driveUntil = date.plusMinutes(minutesToDriveAround);
			vehicleStatus = VehicleStatus.DRIVING;
			engineStatus = EngineStatus.STARTED;
			vehicleUpdater.startVehicle(vehicleId, date);
		}
	}
	
	public void nextStep(DateTime date) throws Exception {
		
		if (vehicleStatus.equals(VehicleStatus.STOPPED)){
			if (!date.isBefore(sleepUntil)){
				vehicleStatus = VehicleStatus.STARTING;
			}			
		}

		if (vehicleStatus.equals(VehicleStatus.STARTING)){
			int minutesToDriveAround = new Double(Math.random() * 110).intValue() + 10;
			driveUntil = date.plusMinutes(minutesToDriveAround);
			vehicleStatus = VehicleStatus.DRIVING;
			engineStatus = EngineStatus.STARTED;
			vehicleUpdater.startVehicle(vehicleId, date);
		}

		if (vehicleStatus.equals(VehicleStatus.DRIVING)){
			if (date.isBefore(driveUntil)) {
				queue.put(vehicleUpdater.updateVehicle(vehicleId, date));

				if (engineStatus == EngineStatus.STARTED) {
					if (Math.random() < 0.07) {	// in 7% of the times, stop the engine
						engineStatus = EngineStatus.STOPPED;
						vehicleUpdater.stopEngine(vehicleId, date);
						stopEngineUntil = date.plusSeconds(new Double(Math.random() * 10 + 1).intValue());
					}
					// otherwise leave the engine running as normal and do nothing
				} else {
					if (!date.isBefore(stopEngineUntil)) {
						engineStatus = EngineStatus.STARTED;
						vehicleUpdater.startEngine(vehicleId, date);
						stopEngineUntil = null;
					}
				}

			} else {
				vehicleStatus = VehicleStatus.STOPPING;
			}
		}

		if (vehicleStatus.equals(VehicleStatus.STOPPING)){
			vehicleUpdater.stopVehicle(vehicleId, date);

			int minutesToSleep = new Double(Math.random() * 720).intValue();
			
			sleepUntil = date.plusMinutes(minutesToSleep);
			vehicleStatus = VehicleStatus.STOPPED;
			engineStatus = EngineStatus.STOPPED;
		}


	}
}
