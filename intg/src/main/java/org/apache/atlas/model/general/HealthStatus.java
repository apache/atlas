package org.apache.atlas.model.general;

import org.apache.atlas.type.AtlasType;

public class HealthStatus {
    public String name;
    public String status;
    public boolean fatal;
    public String checkTime;
    public String errorString;

    public HealthStatus(String name, String status, boolean fatal, String checkTime, String errorString) {
        this.name = name;
        this.status = status;
        this.fatal = fatal;
        this.checkTime = checkTime;
        this.errorString = errorString;
    }

    @Override
    public String toString() {
        return "HealthStatus{" +
                "name=" + name +
                ", status='" + status +
                ", fatal=" + fatal +
                ", checkTime=" + checkTime +
                ", errorString=" + errorString +
                '}';
    }
}
