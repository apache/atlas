package org.apache.atlas.repository.audit;

import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class StartupTimeLogger implements ApplicationListener<ContextRefreshedEvent> {
    private final StartupTimeLoggerBeanPostProcessor beanPostProcessor;

    private static final Logger LOG = LoggerFactory.getLogger(StartupTimeLogger.class);

    public StartupTimeLogger(StartupTimeLoggerBeanPostProcessor beanPostProcessor) {
        this.beanPostProcessor = beanPostProcessor;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        // Print the startup times after all beans are loaded
        printHashMapInTableFormatDescendingOrder(beanPostProcessor.getDurationTimeMap());
    }

    public static void printHashMapInTableFormatDescendingOrder(Map<String, Long> map) {
        // Convert map to a list of entries
        List<Map.Entry<String, Long>> list = new ArrayList<>(map.entrySet());

        // Sort the list by values in descending order
        list.sort((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));
        LOG.info("Capturing Bean creation time {}", AtlasType.toJson(list));
    }
}