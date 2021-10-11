package org.apache.atlas.model.general;

import java.util.HashMap;
import java.util.Map;

public class HealthCheck {
    public Map<String, HealthStatus> results = new HashMap<>();

    public void setComponentHealth(String component, HealthStatus status) {
        this.results.put(component, status);
    }

    public HealthStatus getComponentHealth(String component) {
        return this.results.get(component);
    }

    @Override
    public String toString() {
        return results.toString();
    }
}
