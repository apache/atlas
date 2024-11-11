package org.apache.atlas.discovery;

public class TimeoutTracker {

    private final long startTime;
    private final long timeoutMillis;

    public TimeoutTracker(long timeoutMillis) {
        this.startTime = System.currentTimeMillis();
        this.timeoutMillis = timeoutMillis;
    }

    public boolean hasTimedOut() {
        return (System.currentTimeMillis() - startTime) > timeoutMillis;
    }

}
