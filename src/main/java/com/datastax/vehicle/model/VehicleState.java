package com.datastax.vehicle.model;

import java.util.Date;

public class VehicleState {
    private VehicleStateType latestStateType;
    private Date latestStateTime;
    private int numberOfStateChanges;

    public VehicleState(VehicleStateType currentStateType, Date currentStateTime, int numberOfStateChanges) {
        this.latestStateType = currentStateType;
        this.latestStateTime = currentStateTime;
        this.numberOfStateChanges = numberOfStateChanges;
    }

    public VehicleStateType getLatestStateType() {
        return latestStateType;
    }

    public Date getLatestStateTime() {
        return latestStateTime;
    }

    public int getNumberOfStateChanges() {
        return numberOfStateChanges;
    }
}
