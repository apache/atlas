package org.apache.atlas.repository.graph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.service.redis.RedisService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.inject.Inject;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableScheduling
public class TypeDefRefreshConfig {
    private static final Logger LOG = LoggerFactory.getLogger(TypeDefRefreshConfig.class);

    private final RedisService redisService;
    private final AtlasTypeDefStoreInitializer typeDefStoreInitializer;

    @Inject
    public TypeDefRefreshConfig(RedisService redisService, AtlasTypeDefStoreInitializer typeDefStoreInitializer) {
        this.redisService = redisService;
        this.typeDefStoreInitializer = typeDefStoreInitializer;
    }

    /**
     * Creates an executor for parallel typedef refresh operations across pods.
     * Note: This is NOT an @Async executor - it's used explicitly with CompletableFuture.
     */
    @Bean(name = "typeDefAsyncExecutor", destroyMethod = "shutdown")
    public Executor typeDefAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);  // Minimum core pool - most scenarios have at least 2 pods
        executor.setMaxPoolSize(8);   // Scale up to max pod count when needed
        executor.setQueueCapacity(8); // Minimal queueing since we want parallel execution
        executor.setKeepAliveSeconds(30); // More aggressive timeout - 30 seconds
        executor.setAllowCoreThreadTimeOut(true); // Allow all threads to timeout
        executor.setWaitForTasksToCompleteOnShutdown(true); // Graceful shutdown
        executor.setAwaitTerminationSeconds(60); // Wait up to 1 minute during shutdown
        executor.setThreadNamePrefix("typedef-refresh-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy()); // Handle queue overflow

        // Add error handling for async tasks
        executor.setTaskDecorator(runnable -> () -> {
            try {
                runnable.run();
            } catch (Exception e) {
                LOG.error("Error in async typedef refresh", e);
            }
        });

        executor.initialize();
        return executor;
    }

    /**
     * Scheduled task that runs every 10 minutes to check if the local typedef version
     * is behind the version in Redis. If so, triggers a typedef refresh.
     */
    @Scheduled(fixedRateString = "${atlas.typedef.scheduler.fixed-rate:600000}")
    public void checkTypeDefVersion() {
        try {
            // Check if Redis is available before attempting version check
            if (!redisService.isAvailable()) {
                LOG.debug("TypeDef version check skipped - Redis unavailable");
                return;
            }

            // Get version from Redis
            String redisVersionStr = redisService.getValue(AtlasTypeDefStoreInitializer.TYPEDEF_LATEST_VERSION, "1");
            if (redisVersionStr == null) {
                LOG.debug("TypeDef version check skipped - could not get version from Redis");
                return;
            }
            long redisVersion = Long.parseLong(redisVersionStr);

            // Get current version
            long currentVersion = AtlasTypeDefStoreInitializer.getCurrentTypedefInternalVersion();

            LOG.debug("TypeDef version check - Redis: {}, Current: {}", redisVersion, currentVersion);

            if (currentVersion < redisVersion) {
                LOG.info("Local TypeDef version {} is behind Redis version {}. Triggering refresh...",
                        currentVersion, redisVersion);

                try {
                    typeDefStoreInitializer.init();
                    LOG.info("TypeDef refresh completed successfully. New version: {}",
                            AtlasTypeDefStoreInitializer.getCurrentTypedefInternalVersion());
                } catch (Exception e) {
                    LOG.error("Failed to refresh TypeDefs", e);
                }
            }
        } catch (Exception e) {
            LOG.error("Error checking TypeDef versions", e);
        }
    }
}
