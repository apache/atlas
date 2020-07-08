package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AtlasElasticsearchDatabase {
    private static final Logger LOG = LoggerFactory.getLogger(RestHighLevelClient.class);
    private static volatile RestHighLevelClient searchClient;
    public static final String INDEX_BACKEND_CONF = "atlas.graph.index.search.hostname";

    public static List<HttpHost> getHttpHosts() throws AtlasException {
        List<HttpHost> httpHosts = new ArrayList<>();
        Configuration configuration = ApplicationProperties.get();
        String indexConf = configuration.getString(INDEX_BACKEND_CONF);
        String[] hosts = indexConf.split(",");
        for (String host: hosts) {
            host = host.trim();
            String[] hostAndPort = host.split(":");
            if (hostAndPort.length == 1) {
                httpHosts.add(new HttpHost(hostAndPort[0]));
            } else if (hostAndPort.length == 2) {
                httpHosts.add(new HttpHost(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
            } else {
                throw new AtlasException("Invalid config");
            }
        }
        return httpHosts;
    }

    public static RestHighLevelClient getClient() {
        if (searchClient == null) {
            synchronized (AtlasElasticsearchDatabase.class) {
                if (searchClient == null) {
                    try {
                        List<HttpHost> httpHosts = getHttpHosts();

                    RestClientBuilder restClientBuilder = RestClient.builder(httpHosts.toArray(new HttpHost[0]));
                    searchClient =
                            new RestHighLevelClient(restClientBuilder);
                    } catch (AtlasException e) {

                    }
                }
            }
        }
        return searchClient;
    }
}
