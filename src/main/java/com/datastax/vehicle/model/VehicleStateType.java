package com.datastax.vehicle.model;

public enum VehicleStateType {
    ON("on"),
    OFF("off");

    private final String typeValue;

    private VehicleStateType(String s) {
        this.typeValue = s;
    }

    public String getTypeValue() {
        return typeValue;
    }

    public static VehicleStateType byTypeValue(String v) {
       if (v == "on") {
           return ON;
       } else if (v == "off") {
           return OFF;
       } else {
           throw new IllegalArgumentException("Unknown state value: neither ON nor OFF");
       }
    }
}
