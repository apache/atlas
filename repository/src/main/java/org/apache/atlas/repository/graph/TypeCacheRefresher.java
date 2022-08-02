package org.apache.atlas.repository.graph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static org.apache.atlas.repository.Constants.VERTEX_INDEX;

@Component
public class TypeCacheRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefresher.class);
    private String cacheRefresherEndpoint;
    private final IAtlasGraphProvider provider;

    @Inject
    public TypeCacheRefresher(final IAtlasGraphProvider provider) {
        this.provider = provider;
    }

    @PostConstruct
    public void init() throws AtlasException{
        cacheRefresherEndpoint = ApplicationProperties.get().getString("atlas.server.type.cache-refresher");
        LOG.info("Found {} as cache-refresher endpoint",cacheRefresherEndpoint);
    }

    public void refreshAllHostCache() throws AtlasBaseException {
        if(StringUtils.isBlank(cacheRefresherEndpoint)) {
            LOG.info("Did not find endpoint. Skipping refreshing type-def cache");
            return;
        }

        try {
            int totalFieldKeys = provider.get().getManagementSystem().getGraphIndex(VERTEX_INDEX).getFieldKeys().size();
            LOG.info("Found {} totalFieldKeys to be expected in other nodes",totalFieldKeys);
            refreshCache(cacheRefresherEndpoint,totalFieldKeys);
        }
        catch (final Exception exception) {
            LOG.error(exception.getMessage(),exception);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Could not update type definition cache");
        }
    }

    private void refreshCache(final String hostUrl,final int totalFieldKeys) {
        final CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = null;

        try {
            URIBuilder builder = new URIBuilder(hostUrl);
            builder.setParameter("expectedFieldKeys", String.valueOf(totalFieldKeys));
            final HttpPost httpPost = new HttpPost(builder.build());
            LOG.info("Invoking cache refresh endpoint {}",hostUrl);
            response = client.execute(httpPost);
            LOG.info("Received HTTP response code {} from cache refresh endpoint",response.getStatusLine().getStatusCode());
            if(response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("Error while calling cache-refresher on host "+hostUrl+". HTTP code = "+ response.getStatusLine().getStatusCode());
            }
            final String responseBody = EntityUtils.toString(response.getEntity());
            LOG.info("Response Body from cache-refresh = {}", responseBody);
            CacheRefreshResponseEnvelope cacheRefreshResponseEnvelope = convertStringToObject(responseBody);

            for (CacheRefreshResponse responseOfEachNode : cacheRefreshResponseEnvelope.getResponse()) {
                LOG.info("Host {} returns response code {}", responseOfEachNode.getHost(), responseOfEachNode.getHttpStatus());
                if(responseOfEachNode.getHttpStatus() != 204) {
                    throw new RuntimeException("Error while performing cache refresh on host "+hostUrl+". HTTP code = "+ response.getStatusLine().getStatusCode());
                }
            }

            LOG.info("Refreshed cache successfully on all hosts");
        } catch (IOException | URISyntaxException e) {
            LOG.error("Error while invoking cache-refresh endpoint " + e.getMessage(),e);
            throw new RuntimeException(e);
        }
        finally {
            IOUtils.closeQuietly(response);
            IOUtils.closeQuietly(client);
        }
    }

    private CacheRefreshResponseEnvelope convertStringToObject(final String responseBody) throws JsonProcessingException {
        //Sample string response
        //String input= "{\"response\": [{\"atlas-2\": \"status: 204, headers: {'Date': 'Mon, 01 Aug 2022 16:21:23 GMT', 'Content-Type': 'application/json;charset=utf-8', 'requestId': '38f26130-27d8-4910-b4a8-728ed47621e7'}\"}, {\"atlas-1\": \"status: 204, headers: {'Date': 'Mon, 01 Aug 2022 16:21:23 GMT', 'Content-Type': 'application/json;charset=utf-8', 'requestId': 'dc67d322-5219-4204-a558-fa5cf9ae8cd6'}\"}, {\"atlas-3\": \"status: 204, headers: {'Date': 'Mon, 01 Aug 2022 16:21:23 GMT', 'Content-Type': 'application/json;charset=utf-8', 'requestId': 'e60ae5d7-91cf-4d61-9499-16a1c44793be'}\"}]}";
        final ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(responseBody, CacheRefreshResponseEnvelope.class);
    }
}

class CacheRefreshResponseEnvelope {
    private List<CacheRefreshResponse> response;

    public List<CacheRefreshResponse> getResponse() {
        return response;
    }

    public void setResponse(List<CacheRefreshResponse> response) {
        this.response = response;
    }
}
class CacheRefreshResponse {
    private final Map<String,String> response;

    @JsonCreator
    public CacheRefreshResponse(Map<String, String> response){
        this.response = response;
    }

    public String getHost() {
        return response.entrySet().stream().findFirst().get().getKey();
    }

    public int getHttpStatus() {
        String responseValue = response.entrySet().stream().findFirst().get().getValue();
        //Parsing status: 204, headers: {'Date': 'Mon, 01 Aug 2022 16:21:23 GMT', 'Content-Type': 'application/json;charset=utf-8', 'requestId': 'dc67d322-5219-4204-a558-fa5cf9ae8cd6'}]
        String httpStatusCodeInString = responseValue.split(",")[0].split(":")[1].trim();
        return Integer.parseInt(httpStatusCodeInString);
    }

}

