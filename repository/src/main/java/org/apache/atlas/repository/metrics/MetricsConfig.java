package org.apache.atlas.repository.metrics;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public TaskMetricsService taskMetricsService() {
        return new TaskMetricsService();
    }
} 