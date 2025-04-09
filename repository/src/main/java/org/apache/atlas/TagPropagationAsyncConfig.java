package org.apache.atlas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableAsync(proxyTargetClass = true)
public class TagPropagationAsyncConfig implements AsyncConfigurer {

    private static final Logger LOG = LoggerFactory.getLogger(TagPropagationAsyncConfig.class);

    @Bean
    public Executor classificationPropagationExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(75);
        executor.setMaxPoolSize(150);
        executor.setQueueCapacity(50000);
        executor.setKeepAliveSeconds(300); // 5 minutes to keep extra threads alive
        executor.setAllowCoreThreadTimeOut(true); // Allow thread pool to shrink when idle
        executor.setThreadNamePrefix("ClassificationPropagation-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        // Add custom exception handler
        executor.setTaskDecorator(runnable -> () -> {
            try {
                runnable.run();
            } catch (Exception e) {
                LOG.error("Error in async classification propagation", e);
                // Add any error handling/reporting logic
            }
        });

        executor.initialize();
        return executor;
    }

}