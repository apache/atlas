package org.apache.atlas.repository.graph;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.VERTEX_INDEX;

@Component
public class TypeCacheRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefresher.class);
    private static final String URI = "/api/atlas/admin/types/refresh";
    private List<String> atlasHosts;
    private final IAtlasGraphProvider provider;

    @Inject
    public TypeCacheRefresher(final IAtlasGraphProvider provider) {
        this.provider = provider;
    }

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
            int totalFieldKeys = provider.get().getManagementSystem().getGraphIndex(VERTEX_INDEX).getFieldKeys().size();
            LOG.info("Found {} totalFieldKeys to be expected in other nodes",totalFieldKeys);
            atlasHosts.forEach(hostUrl -> {
                CompletableFuture<Void> httpApiFuture = CompletableFuture.runAsync(() -> refreshCache(hostUrl,totalFieldKeys), executorService);
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

    private void refreshCache(final String hostUrl,final int totalFieldKeys) {
        final CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        final HttpPost httpPost;
        try {
            URIBuilder builder = new URIBuilder(hostUrl+URI);
            builder.setParameter("expectedFieldKeys", String.valueOf(totalFieldKeys));
            httpPost = new HttpPost(builder.build());
            LOG.info("Refreshing cache on host {}",hostUrl);
            response = client.execute(httpPost);
            if(response.getStatusLine().getStatusCode() != 204) {
                throw new RuntimeException("Could not refresh cache on host "+hostUrl+". HTTP code = "+ response.getStatusLine().getStatusCode());
            }
            LOG.info("Refreshed cache successfully on host {}",hostUrl);
        } catch (IOException | URISyntaxException e) {
            LOG.error(e.getMessage(),e);
            throw new RuntimeException(e);
        }
        finally {
            IOUtils.closeQuietly(response);
            IOUtils.closeQuietly(client);
        }
    }
}
