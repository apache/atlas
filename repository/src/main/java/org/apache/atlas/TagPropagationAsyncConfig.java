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

    @Bean(destroyMethod = "shutdown")
    public Executor tagPropagationNotifierExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(AtlasConfiguration.TAG_ASYNC_NOTIFIER_CORE_POOL_SIZE.getInt());
        executor.setMaxPoolSize(AtlasConfiguration.TAG_ASYNC_NOTIFIER_MAX_POOL_SIZE.getInt());
        executor.setQueueCapacity(AtlasConfiguration.TAG_ASYNC_NOTIFIER_QUEUE_CAPACITY.getInt());
        executor.setKeepAliveSeconds(AtlasConfiguration.TAG_ASYNC_NOTIFIER_KEEP_ALIVE_SECONDS.getInt());
        executor.setAllowCoreThreadTimeOut(true);
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