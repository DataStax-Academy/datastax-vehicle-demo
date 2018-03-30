package com.datastax.vehicle.model;

public enum VehicleStatus {

    STARTING("VEHICLE_STARTING"),
    STOPPING("VEHICLE_STOPPING"),
    STOPPED("VEHICLE_STOPPED"),
    DRIVING("VEHICLE_DRIVING");

    private String statusValue;

    private VehicleStatus(String sv) {
        statusValue = sv;
    }

    public String getStatusValue() {
        return statusValue;
    }

}
