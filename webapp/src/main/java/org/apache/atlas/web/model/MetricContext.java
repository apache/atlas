package org.apache.atlas.web.model;

import io.micrometer.core.instrument.Timer;

import javax.servlet.http.HttpServletRequest;

public class MetricContext {
    private static final String ATTRIBUTE = MetricContext.class.getName();

    private final Timer.Sample timerSample;

    public MetricContext(Timer.Sample timerSample) {
        this.timerSample = timerSample;
    }

    public Timer.Sample getTimerSample() {
        return this.timerSample;
    }

    public void attachTo(HttpServletRequest request) {
        request.setAttribute(ATTRIBUTE, this);
    }

    public static MetricContext get(HttpServletRequest request) {
        return (MetricContext) request.getAttribute(ATTRIBUTE);
    }
}
