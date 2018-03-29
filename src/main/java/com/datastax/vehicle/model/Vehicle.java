package com.datastax.vehicle.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.github.davidmoten.geo.LatLong;

public class Vehicle {
	private String vehicle;
	private Date date;
	private LatLong latLong;
	private String tile;
	private String tile2;
	private double temperature;
	private double speed;
	private Map<String, Double> p_ = new HashMap<String, Double>();

	public Vehicle(String vehicle, Date date, LatLong latLong, String tile, String tile2, double temperature,
			double speed) {
		super();
		this.vehicle = vehicle;
		this.date = date;
		this.latLong = latLong;
		this.tile = tile;
		this.tile2 = tile2;
		this.setTemperature(temperature);
		this.setSpeed(speed);
	}

	public String getVehicle() {
		return vehicle;
	}

	public Date getDate() {
		return date;
	}

	public LatLong getLatLong() {
		return latLong;
	}

	public String getTile() {
		return tile;
	}
	
	public String getTile2() {
		return tile2;
	}

	public double getTemperature() {
		return temperature;
	}

	public void setTemperature(double temperature) {
		this.temperature = temperature;
	}

	public double getSpeed() {
		return speed;
	}

	public void setSpeed(double speed) {
		this.speed = speed;
	}

	public Map<String, Double> getProperties() {
		return p_;
	}

	public void setProperties(Map<String, Double> p_) {
		this.p_ = p_;
	}

	@Override
	public String toString() {
		return "Vehicle [vehicle=" + vehicle + ", date=" + date + ", latLong=" + latLong + ", tile=" + tile + ", tile2="
				+ tile2 + ", temperature=" + temperature + ", speed=" + speed + ", properties=" + p_ + "]";
	}
}
