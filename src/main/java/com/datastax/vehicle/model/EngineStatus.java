package com.datastax.vehicle.model;

public enum EngineStatus {

    STARTED("ENGINE_STARTED"),
    STOPPED("ENGINE_STOPPED");

    private String statusValue;

    private EngineStatus(String sv) {
        statusValue = sv;
    }

    public String getStatusValue() {
        return statusValue;
    }
}
