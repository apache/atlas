/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.janusgraph.diskstorage.solr;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Optional;

/**
 * NOTE: Class to get access to SolrIndex.solrClient
 */
@PreInitializeConfigOptions
public class Solr6Index extends SolrIndex {
    private static final Logger LOG = LoggerFactory.getLogger(Solr6Index.class);

    public enum Mode {
        HTTP, CLOUD;

        public static Mode parse(String mode) {
            for (final Mode m : Mode.values()) {
                if (m.toString().equalsIgnoreCase(mode)) {
                    return m;
                }
            }

            throw new IllegalArgumentException("Unrecognized mode: "+mode);
        }
    }

    public static final ConfigOption<Boolean> CREATE_SOLR_CLIENT_PER_REQUEST = new ConfigOption(SOLR_NS, "create-client-per-request", "when false, allows the sharing of solr client across other components.", org.janusgraph.diskstorage.configuration.ConfigOption.Type.LOCAL, false);

    private static boolean    createSolrClientPerRequest = false;
    private static Solr6Index INSTANCE                   = null;

    private final Configuration config;
    private final Mode          solrMode;
    private final SolrClient    solrClient;

    public Solr6Index(Configuration config) throws BackendException {
        super(config);

        Mode solrMode = Mode.CLOUD;

        try {
            Field fld = SolrIndex.class.getDeclaredField("mode");

            fld.setAccessible(true);

            Object val = fld.get(this);

            if (val != null) {
                solrMode = Mode.parse(val.toString());
            } else {
                LOG.warn("SolrMode is not set. Assuming {}", solrMode);
            }
        } catch (Exception excp) {
            LOG.warn("Failed to get SolrMode. Assuming {}", solrMode, excp);
        }

        SolrClient solrClient = null;

        try {
            Field fld = SolrIndex.class.getDeclaredField("solrClient");

            fld.setAccessible(true);

            solrClient = (SolrClient) fld.get(this);
        } catch (Exception excp) {
            LOG.warn("Failed to get SolrClient", excp);
        }

        this.config     = config;
        this.solrMode   = solrMode;
        this.solrClient = solrClient;

        createSolrClientPerRequest = config.get(CREATE_SOLR_CLIENT_PER_REQUEST);
        INSTANCE                   = this;
    }

    public static SolrClient getSolrClient() {
        SolrClient ret   = null;
        Solr6Index index = INSTANCE;

        if (index != null) {
            ret = createSolrClientPerRequest ? index.createSolrClient() : index.solrClient;
        }

        if (ret == null) {
            LOG.warn("getSolrClient() returning null");
        }

        return ret;
    }

    public static void releaseSolrClient(SolrClient client) {
        if (createSolrClientPerRequest) {
            if (client != null) {
                try {
                    client.close();
                } catch (IOException excp) {
                    LOG.warn("Failed to close SolrClient.", excp);
                }
            }
        } else {
            LOG.debug("Ignoring the closing of solr client as it is owned by Solr6Index.");
        }
    }

    public static Solr6Index.Mode getSolrMode() {
        Solr6Index index = INSTANCE;

        return index != null ? index.solrMode : Mode.CLOUD;
    }

    private SolrClient createSolrClient() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("HttpClientBuilder = {}", HttpClientUtil.getHttpClientBuilder(), new Exception());
        }

        final SolrClient           ret;
        final ModifiableSolrParams clientParams = new ModifiableSolrParams();

        switch (solrMode) {
            case CLOUD:
                String[]         zookeeperUrl = config.get(ZOOKEEPER_URL);
                Optional<String> chroot       = Optional.empty();

                for(int i = zookeeperUrl.length - 1; i >= 0; --i) {
                    int chrootIndex = zookeeperUrl[i].indexOf("/");
                    if (chrootIndex != -1) {
                        String hostAndPort = zookeeperUrl[i].substring(0, chrootIndex);
                        if (!chroot.isPresent()) {
                            chroot = Optional.of(zookeeperUrl[i].substring(chrootIndex));
                        }

                        zookeeperUrl[i] = hostAndPort;
                    }
                }

                CloudSolrClient.Builder builder = (new CloudSolrClient.Builder(Arrays.asList(zookeeperUrl), chroot)).withLBHttpSolrClientBuilder((new LBHttpSolrClient.Builder()).withHttpSolrClientBuilder((new HttpSolrClient.Builder()).withInvariantParams(clientParams)).withBaseSolrUrls((String[])config.get(HTTP_URLS, new String[0]))).sendUpdatesOnlyToShardLeaders();
                CloudSolrClient cloudServer = builder.build();
                cloudServer.connect();

                ret = cloudServer;
                break;

            case HTTP:
                clientParams.add("allowCompression", new String[]{((Boolean)config.get(HTTP_ALLOW_COMPRESSION, new String[0])).toString()});
                clientParams.add("connTimeout", new String[]{((Integer)config.get(HTTP_CONNECTION_TIMEOUT, new String[0])).toString()});
                clientParams.add("maxConnectionsPerHost", new String[]{((Integer)config.get(HTTP_MAX_CONNECTIONS_PER_HOST, new String[0])).toString()});
                clientParams.add("maxConnections", new String[]{((Integer)config.get(HTTP_GLOBAL_MAX_CONNECTIONS, new String[0])).toString()});
                HttpClient client = HttpClientUtil.createClient(clientParams);

                ret = new LBHttpSolrClient.Builder().withHttpClient(client).withBaseSolrUrls(config.get(HTTP_URLS)).build();
                break;

            default:
                throw new IllegalArgumentException("Unsupported Solr operation mode: " + solrMode);
        }

        return ret;
    }
}