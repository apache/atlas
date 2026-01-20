package org.apache.atlas.web.rest;

import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.config.CassandraConfigDAO;
import org.apache.atlas.service.config.ConfigKey;
import org.apache.atlas.service.config.DynamicConfigCacheStore;
import org.apache.atlas.service.config.DynamicConfigCacheStore.ConfigEntry;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * REST endpoint to refresh config cache on this pod.
 *
 * Called by ConfigCacheRefresher from other pods when a config is updated.
 * This ensures all pods have consistent config cache without requiring Redis pub/sub.
 */
@Path("admin/config")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class ConfigCacheRefreshREST {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigCacheRefreshREST.class);

    private final DynamicConfigStore configStore;

    @Inject
    public ConfigCacheRefreshREST(DynamicConfigStore configStore) {
        this.configStore = configStore;
    }

    /**
     * Refresh a specific config key from Cassandra into local cache.
     *
     * @param key the config key to refresh
     * @param traceId trace ID for logging correlation
     * @return 200 if successful, 500 on error
     */
    @POST
    @Path("/refresh")
    @Timed
    public Response refreshCache(@QueryParam("key") String key, @QueryParam("traceId") String traceId) {
        LOG.info("ConfigCacheRefreshREST: Received refresh request for key: {} :: traceId: {}", key, traceId);

        try {
            if (!DynamicConfigStore.isEnabled()) {
                LOG.warn("ConfigCacheRefreshREST: DynamicConfigStore is not enabled, skipping refresh :: traceId: {}", traceId);
                return Response.ok().build();
            }

            if (key == null || key.isEmpty()) {
                // Refresh all configs
                configStore.loadAllConfigsIntoCache();
                LOG.info("ConfigCacheRefreshREST: Refreshed all configs :: traceId: {}", traceId);
            } else {
                // Refresh specific key
                refreshSingleKey(key);
                LOG.info("ConfigCacheRefreshREST: Refreshed config key: {} :: traceId: {}", key, traceId);
            }

            return Response.ok().build();

        } catch (Exception e) {
            LOG.error("ConfigCacheRefreshREST: Error refreshing config cache for key: {} :: traceId: {}",
                key, traceId, e);
            return Response.serverError()
                .entity("{\"error\": \"" + e.getMessage() + "\"}")
                .build();
        }
    }

    /**
     * Get the current cache state for a specific key (for debugging/monitoring).
     *
     * @param key the config key
     * @return the cached value and metadata
     */
    @GET
    @Path("/cache/{key}")
    @Timed
    public Response getCacheState(@PathParam("key") String key) {
        try {
            if (!DynamicConfigStore.isEnabled()) {
                return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("{\"error\": \"DynamicConfigStore is not enabled\"}")
                    .build();
            }

            ConfigEntry entry = configStore.getCacheStore().get(key);
            if (entry == null) {
                return Response.status(Response.Status.NOT_FOUND)
                    .entity("{\"error\": \"Key not found in cache\"}")
                    .build();
            }

            return Response.ok()
                .entity(new CacheStateResponse(
                    key,
                    entry.getValue(),
                    entry.getUpdatedBy(),
                    entry.getLastUpdated() != null ? entry.getLastUpdated().toString() : null,
                    System.getenv().getOrDefault("HOSTNAME", "unknown")
                ))
                .build();

        } catch (Exception e) {
            LOG.error("ConfigCacheRefreshREST: Error getting cache state for key: {}", key, e);
            return Response.serverError()
                .entity("{\"error\": \"" + e.getMessage() + "\"}")
                .build();
        }
    }

    private void refreshSingleKey(String key) throws AtlasBaseException {
        if (!CassandraConfigDAO.isInitialized()) {
            LOG.warn("ConfigCacheRefreshREST: CassandraConfigDAO not initialized, cannot refresh key: {}", key);
            return;
        }

        try {
            ConfigEntry entry = CassandraConfigDAO.getInstance().getConfig(key);
            DynamicConfigCacheStore cacheStore = configStore.getCacheStore();

            if (entry != null) {
                cacheStore.put(key, entry.getValue(), entry.getUpdatedBy());
                LOG.debug("ConfigCacheRefreshREST: Updated cache for key: {} = {}", key, entry.getValue());
            } else {
                cacheStore.remove(key);
                LOG.debug("ConfigCacheRefreshREST: Removed key from cache: {}", key);
            }
        } catch (Exception e) {
            LOG.error("ConfigCacheRefreshREST: Error refreshing key: {}", key, e);
            throw new AtlasBaseException("Failed to refresh config key: " + key, e);
        }
    }

    /**
     * Get the current maintenance mode status.
     *
     * Returns:
     * - enabled: Whether maintenance mode is set in the config store
     * - activated: Whether task processing has actually paused
     * - activatedAt: Timestamp when task processing paused
     * - activatedBy: Pod ID that paused task processing
     *
     * @return maintenance mode status
     */
    @GET
    @Path("/maintenance-mode/status")
    @Timed
    public Response getMaintenanceModeStatus() {
        try {
            if (!DynamicConfigStore.isEnabled()) {
                return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("{\"error\": \"DynamicConfigStore is not enabled\"}")
                    .build();
            }

            boolean enabled = DynamicConfigStore.getConfigAsBoolean(ConfigKey.MAINTENANCE_MODE.getKey());
            String activatedAt = DynamicConfigStore.getConfig(ConfigKey.MAINTENANCE_MODE_ACTIVATED_AT.getKey());
            String activatedBy = DynamicConfigStore.getConfig(ConfigKey.MAINTENANCE_MODE_ACTIVATED_BY.getKey());
            boolean activated = activatedAt != null && !activatedAt.isEmpty();

            return Response.ok()
                .entity(new MaintenanceModeStatusResponse(
                    enabled,
                    activated,
                    activatedAt,
                    activatedBy,
                    System.getenv().getOrDefault("HOSTNAME", "unknown")
                ))
                .build();

        } catch (Exception e) {
            LOG.error("ConfigCacheRefreshREST: Error getting maintenance mode status", e);
            return Response.serverError()
                .entity("{\"error\": \"" + e.getMessage() + "\"}")
                .build();
        }
    }

    /**
     * Response DTO for cache state
     */
    public static class CacheStateResponse {
        private String key;
        private String value;
        private String updatedBy;
        private String lastUpdated;
        private String podId;

        public CacheStateResponse(String key, String value, String updatedBy, String lastUpdated, String podId) {
            this.key = key;
            this.value = value;
            this.updatedBy = updatedBy;
            this.lastUpdated = lastUpdated;
            this.podId = podId;
        }

        public String getKey() { return key; }
        public String getValue() { return value; }
        public String getUpdatedBy() { return updatedBy; }
        public String getLastUpdated() { return lastUpdated; }
        public String getPodId() { return podId; }
    }

    /**
     * Response DTO for maintenance mode status
     */
    public static class MaintenanceModeStatusResponse {
        private boolean enabled;
        private boolean activated;
        private String activatedAt;
        private String activatedBy;
        private String respondingPod;

        public MaintenanceModeStatusResponse(boolean enabled, boolean activated, String activatedAt,
                                             String activatedBy, String respondingPod) {
            this.enabled = enabled;
            this.activated = activated;
            this.activatedAt = activatedAt;
            this.activatedBy = activatedBy;
            this.respondingPod = respondingPod;
        }

        public boolean isEnabled() { return enabled; }
        public boolean isActivated() { return activated; }
        public String getActivatedAt() { return activatedAt; }
        public String getActivatedBy() { return activatedBy; }
        public String getRespondingPod() { return respondingPod; }
    }
}
