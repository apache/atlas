package org.apache.atlas.discovery;

public class TimeoutChecker {
    private final long startTime;
    private final long timeoutMs;

    public TimeoutChecker(long timeoutMs) {
        this.startTime = System.currentTimeMillis();
        this.timeoutMs = timeoutMs;
    }

    public boolean hasTimedOut() {
        return System.currentTimeMillis() - startTime > timeoutMs;
    }
}