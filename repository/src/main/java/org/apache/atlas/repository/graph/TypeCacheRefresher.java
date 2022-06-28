package org.apache.atlas.repository.graph;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Component
public class TypeCacheRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefresher.class);
    private static final String URI = "/api/atlas/admin/types/refresh";
    private List<String> atlasHosts;

    @PostConstruct
    public void init() throws AtlasException{
        String[] hosts = ApplicationProperties.get().getStringArray("atlas.server.hosts");
        atlasHosts = Arrays.stream(hosts).collect(Collectors.toList());
        LOG.info("Found following atlas hosts to refresh type cache {}",atlasHosts);
    }

    public void refreshAllHostCache() throws AtlasBaseException {
        if(atlasHosts.isEmpty()) {
            LOG.info("Did not find any hosts to refresh type-def cache");
            return;
        }
        ExecutorService executorService = Executors.newFixedThreadPool(atlasHosts.size());
        List<CompletableFuture<?>> completableFutureList = new ArrayList<>(atlasHosts.size());
        try {
            atlasHosts.forEach(hostUrl -> {
                CompletableFuture<Void> httpApiFuture = CompletableFuture.runAsync(() -> refreshCache(hostUrl), executorService);
                completableFutureList.add(httpApiFuture);
            });
            //Wait for all nodes to finish
            CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0])).join();
        }
        catch (final Exception exception) {
            LOG.error(exception.getMessage(),exception);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Could not update type-def cache");
        }
        finally {
            executorService.shutdown();
        }
    }

    private void refreshCache(final String hostUrl) {
        final CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        final HttpPost httpPost = new HttpPost(hostUrl+URI);
        try {
            LOG.info("Refreshing cache on host {}",hostUrl);
            response = client.execute(httpPost);
            if(response.getStatusLine().getStatusCode() != 204) {
                throw new RuntimeException("Could not refresh cache on host "+hostUrl+". HTTP code = "+ response.getStatusLine().getStatusCode());
            }
            LOG.info("Refreshed cache successfully on host {}",hostUrl);
        } catch (IOException e) {
            LOG.error(e.getMessage(),e);
            throw new RuntimeException(e);
        }
        finally {
            IOUtils.closeQuietly(response);
            IOUtils.closeQuietly(client);
        }
    }
}
