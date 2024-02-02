package org.apache.atlas.repository.audit;

import org.apache.atlas.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map;

@Component
public class StartupTimeLoggerBeanPostProcessor implements BeanPostProcessor {
    private final Map<String, Long> startTimeMap = new HashMap<>();

    public Map<String, Long> getDurationTimeMap() {
        return durationTimeMap;
    }

    private final Map<String, Long> durationTimeMap = new HashMap<>();

    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("Beans");

    private AtlasPerfTracer perf = null;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        // Record the start time
        startTimeMap.put(bean.getClass().getName(), System.currentTimeMillis());
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "Beans.create(" +  beanName + ")");
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        AtlasPerfTracer.log(perf);
        // Calculate and log the startup time
        long startTime = startTimeMap.getOrDefault(bean.getClass().getName(), -1L);
        long endTime = System.currentTimeMillis();
        if (startTime != -1L) {
            durationTimeMap.put(bean.getClass().getName(), endTime-startTime);
        }
        return bean;
    }
}