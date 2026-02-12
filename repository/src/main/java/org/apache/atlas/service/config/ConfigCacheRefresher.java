package org.apache.atlas.service.config;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Refreshes config cache across all Atlas pods using Kubernetes API for pod discovery.
 *
 * Similar to TypeCacheRefresher, this component:
 * 1. Discovers all Atlas pods via Kubernetes API
 * 2. Calls the refresh endpoint on each pod
 * 3. Waits for all pods to acknowledge (with timeout and retry)
 *
 * This ensures strong consistency for config changes across all pods
 * without requiring Redis pub/sub.
 */
@Component
public class ConfigCacheRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigCacheRefresher.class);

    private KubernetesClient k8sClient;
    private CloseableHttpClient httpClient;
    private ExecutorService executor;

    @Value("${atlas.config.refresh.timeout:30}")
    private int refreshTimeoutSeconds;

    @Value("${atlas.config.refresh.retries:3}")
    private int refreshRetries;

    @Value("${atlas.k8s.pod.labels:atlas,atlas-read}")
    private String podLabels;

    @Value("${atlas.k8s.namespace:atlas}")
    private String namespace;

    @Value("${atlas.server.http.port:21000}")
    private int atlasPort;

    @PostConstruct
    public void init() throws AtlasException {
        // Initialize thread pool for parallel pod refresh
        this.executor = Executors.newFixedThreadPool(10);

        // Only initialize K8s client in Kubernetes environments
        if (isKubernetesEnvironment()) {
            try {
                this.k8sClient = new KubernetesClientBuilder().build();
                LOG.info("ConfigCacheRefresher: Kubernetes client initialized successfully");
            } catch (Exception e) {
                LOG.warn("ConfigCacheRefresher: Failed to initialize Kubernetes client: {}. Pod discovery disabled.", e.getMessage());
                this.k8sClient = null;
            }
        } else {
            LOG.info("ConfigCacheRefresher: Running in local environment. Pod discovery disabled.");
            this.k8sClient = null;
        }

        initHttpClient();
    }

    @PreDestroy
    public void cleanup() {
        if (k8sClient != null) {
            k8sClient.close();
        }
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                LOG.warn("Error closing HttpClient", e);
            }
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    /**
     * Refresh config cache on all other Atlas pods.
     * This is a BLOCKING call that waits for all pods to refresh.
     *
     * @param configKey the config key that was updated
     * @return RefreshSummary with results from all pods
     */
    public RefreshSummary refreshAllPodsCache(String configKey) {
        final String traceId = RequestContext.get() != null ?
            RequestContext.get().getTraceId() : "no-trace";

        String currentPodIp = getCurrentPodIp();
        List<String> otherPodIps = getOtherAtlasPodIps(currentPodIp);

        if (otherPodIps.isEmpty()) {
            LOG.info("ConfigCacheRefresher: No other Atlas pods found to notify for key: {}", configKey);
            return new RefreshSummary(configKey, 0, 0, Collections.emptyList());
        }

        LOG.info("ConfigCacheRefresher: Refreshing {} other Atlas pods for config key: {}",
            otherPodIps.size(), configKey);

        // Parallel refresh of all other pods
        List<CompletableFuture<RefreshResult>> futures = otherPodIps.stream()
            .map(podIp -> CompletableFuture.supplyAsync(() ->
                refreshPodWithRetry(podIp, configKey, traceId), executor))
            .collect(Collectors.toList());

        try {
            // Wait for all refreshes to complete (with timeout)
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(refreshTimeoutSeconds + 5, TimeUnit.SECONDS);

            // Collect results
            List<RefreshResult> results = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

            long successful = results.stream().filter(RefreshResult::isSuccess).count();
            LOG.info("ConfigCacheRefresher: Config refresh completed for key {}: {}/{} pods succeeded",
                configKey, successful, results.size());

            if (successful < results.size()) {
                List<String> failedPods = results.stream()
                    .filter(r -> !r.isSuccess())
                    .map(RefreshResult::getPodIp)
                    .collect(Collectors.toList());
                LOG.warn("ConfigCacheRefresher: Failed to refresh pods: {}", failedPods);
            }

            return new RefreshSummary(configKey, (int) successful, results.size(), results);

        } catch (TimeoutException e) {
            LOG.error("ConfigCacheRefresher: Timeout waiting for pod refreshes to complete for key: {}", configKey, e);
            return new RefreshSummary(configKey, 0, otherPodIps.size(), Collections.emptyList());
        } catch (Exception e) {
            LOG.error("ConfigCacheRefresher: Error during pod refresh notification for key: {}", configKey, e);
            return new RefreshSummary(configKey, 0, otherPodIps.size(), Collections.emptyList());
        }
    }

    private boolean isKubernetesEnvironment() {
        String kubernetesServiceHost = System.getenv("KUBERNETES_SERVICE_HOST");
        return kubernetesServiceHost != null && !kubernetesServiceHost.isEmpty();
    }

    private void initHttpClient() {
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(5000)
            .setSocketTimeout(refreshTimeoutSeconds * 1000)
            .setConnectionRequestTimeout(5000)
            .build();

        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(50);
        connectionManager.setDefaultMaxPerRoute(10);

        this.httpClient = HttpClientBuilder.create()
            .setConnectionManager(connectionManager)
            .setDefaultRequestConfig(requestConfig)
            .disableAutomaticRetries()
            .build();

        LOG.info("ConfigCacheRefresher: HttpClient initialized with timeout: {}s", refreshTimeoutSeconds);
    }

    private List<String> getOtherAtlasPodIps(String currentPodIp) {
        if (k8sClient == null) {
            LOG.debug("ConfigCacheRefresher: Kubernetes client not available, returning empty pod list");
            return Collections.emptyList();
        }

        try {
            String[] labels = podLabels.split(",");
            if (labels.length == 0) {
                labels = new String[]{"atlas", "atlas-read"};
            }

            LOG.debug("ConfigCacheRefresher: Looking for pods with labels {} in namespace {}",
                String.join(",", labels), namespace);

            List<String> podIps = k8sClient.pods()
                .inNamespace(namespace)
                .withLabelIn("app", labels)
                .list()
                .getItems()
                .stream()
                .filter(pod -> {
                    String phase = pod.getStatus().getPhase();
                    if (!"Running".equals(phase)) {
                        return false;
                    }

                    List<ContainerStatus> containerStatuses = pod.getStatus().getContainerStatuses();
                    if (containerStatuses == null || containerStatuses.isEmpty()) {
                        return false;
                    }

                    return containerStatuses.stream().allMatch(ContainerStatus::getReady);
                })
                .map(pod -> pod.getStatus().getPodIP())
                .filter(ip -> ip != null && !ip.equals(currentPodIp))
                .collect(Collectors.toList());

            LOG.debug("ConfigCacheRefresher: Discovered {} other Atlas pods: {}", podIps.size(), podIps);
            return podIps;

        } catch (KubernetesClientException e) {
            LOG.error("ConfigCacheRefresher: Error querying Kubernetes API: {}", e.getMessage());
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.error("ConfigCacheRefresher: Unexpected error discovering Atlas pods", e);
            return Collections.emptyList();
        }
    }

    private String getCurrentPodIp() {
        String podIp = System.getenv("POD_IP");
        if (podIp != null && !podIp.isEmpty()) {
            return podIp;
        }

        try {
            InetAddress localHost = InetAddress.getLocalHost();
            return localHost.getHostAddress();
        } catch (UnknownHostException e) {
            LOG.warn("ConfigCacheRefresher: Could not determine current pod IP", e);
            return "unknown";
        }
    }

    private RefreshResult refreshPodWithRetry(String podIp, String configKey, String traceId) {
        String lastError = null;

        for (int attempt = 1; attempt <= refreshRetries; attempt++) {
            RefreshResult result = refreshPod(podIp, configKey, traceId, attempt);

            if (result.isSuccess()) {
                return result;
            }

            lastError = result.getError();

            if (attempt < refreshRetries) {
                try {
                    int backoffMs = 1000 * attempt;
                    LOG.warn("ConfigCacheRefresher: Retry {}/{} for pod {} after {}ms",
                        attempt, refreshRetries, podIp, backoffMs);
                    Thread.sleep(backoffMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("ConfigCacheRefresher: Interrupted during retry backoff", e);
                    break;
                }
            }
        }

        LOG.error("ConfigCacheRefresher: Failed to refresh pod {} after {} attempts. Last error: {}",
            podIp, refreshRetries, lastError);
        return new RefreshResult(podIp, false, 0, refreshRetries, lastError);
    }

    private RefreshResult refreshPod(String podIp, String configKey, String traceId, int attempt) {
        String encodedKey = URLEncoder.encode(configKey, StandardCharsets.UTF_8);
        String encodedTraceId = URLEncoder.encode(traceId, StandardCharsets.UTF_8);

        String url = String.format("http://%s:%d/api/atlas/admin/config/refresh?key=%s&traceId=%s",
            podIp, atlasPort, encodedKey, encodedTraceId);

        long startTime = System.currentTimeMillis();
        HttpPost httpPost = new HttpPost(url);

        try {
            LOG.debug("ConfigCacheRefresher: Sending refresh request to pod {} (attempt {}): {}",
                podIp, attempt, url);

            httpPost.setHeader("Accept", "application/json");

            CloseableHttpResponse response = httpClient.execute(httpPost);
            long duration = System.currentTimeMillis() - startTime;

            try {
                int statusCode = response.getStatusLine().getStatusCode();
                boolean success = statusCode == 200 || statusCode == 204;

                if (success) {
                    LOG.info("ConfigCacheRefresher: Successfully refreshed pod {} in {}ms (attempt {})",
                        podIp, duration, attempt);
                } else {
                    LOG.warn("ConfigCacheRefresher: Pod {} returned status: {} (attempt {})",
                        podIp, statusCode, attempt);
                }

                return new RefreshResult(podIp, success, duration, attempt,
                    success ? null : "HTTP " + statusCode);

            } finally {
                response.close();
            }

        } catch (SocketTimeoutException e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("ConfigCacheRefresher: Timeout refreshing pod {} after {}ms (attempt {})",
                podIp, duration, attempt);
            return new RefreshResult(podIp, false, duration, attempt, "Timeout");

        } catch (ConnectTimeoutException e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("ConfigCacheRefresher: Connection timeout to pod {} (attempt {})", podIp, attempt);
            return new RefreshResult(podIp, false, duration, attempt, "Connection timeout");

        } catch (IOException e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("ConfigCacheRefresher: IO error refreshing pod {} (attempt {}): {}",
                podIp, attempt, e.getMessage());
            return new RefreshResult(podIp, false, duration, attempt, "IO error: " + e.getMessage());

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.error("ConfigCacheRefresher: Unexpected error refreshing pod {} (attempt {})",
                podIp, attempt, e);
            return new RefreshResult(podIp, false, duration, attempt, "Error: " + e.getMessage());

        } finally {
            httpPost.reset();
        }
    }

    /**
     * Result of a single pod refresh operation
     */
    @Data
    @AllArgsConstructor
    public static class RefreshResult {
        private String podIp;
        private boolean success;
        private long durationMs;
        private int attempts;
        private String error;
    }

    /**
     * Summary of refresh operation across all pods
     */
    @Data
    @AllArgsConstructor
    public static class RefreshSummary {
        private String configKey;
        private int successCount;
        private int totalCount;
        private List<RefreshResult> results;

        public boolean isFullySuccessful() {
            return successCount == totalCount;
        }
    }
}
