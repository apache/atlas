package org.apache.atlas.repository.graphdb.janus.customdatabase;

import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.es.ElasticSearchClient;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.diskstorage.es.ElasticSearchMutation;
import org.janusgraph.diskstorage.es.ElasticSearchSetup;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NAME;

public class AtlasElasticSearchIndex extends ElasticSearchIndex {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasElasticSearchIndex.class);
    private static final String INDEX_NAME_SEPARATOR = "_";

    private final String indexName;
    private final ElasticSearchClient client;

    private final Map<String, String> indexStoreNamesCache = new ConcurrentHashMap<>();
    private final boolean indexStoreNameCacheEnabled;

    private final Function<String, String> generateIndexStoreNameFunction = this::generateIndexStoreName;

    public AtlasElasticSearchIndex(Configuration config) throws BackendException {
        super(config);

        client = interfaceConfiguration(config).getClient();

        indexName = config.get(INDEX_NAME);
        indexStoreNameCacheEnabled = config.get(ENABLE_INDEX_STORE_NAMES_CACHE);
    }

    @Override
    public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever information,
                        BaseTransaction tx) throws BackendException {
        final List<ElasticSearchMutation> requests = new ArrayList<>();
        try {
            for (final Map.Entry<String, Map<String, List<IndexEntry>>> stores : documents.entrySet()) {
                final List<ElasticSearchMutation> requestByStore = new ArrayList<>();
                final String store = stores.getKey();
                final String indexStoreName = getIndexStoreName(store);
                for (final Map.Entry<String, List<IndexEntry>> entry : stores.getValue().entrySet()) {
                    final String docID = entry.getKey();
                    final List<IndexEntry> content = entry.getValue();
                    if (content == null || content.size() == 0) {
                        // delete
                        if (LOG.isDebugEnabled())
                            LOG.debug("Deleting entire document {}", docID);

                        requestByStore.add(ElasticSearchMutation.createDeleteRequest(indexStoreName, store, docID));
                    } else {
                        // Add
                        if (LOG.isDebugEnabled())
                            LOG.debug("Adding entire document {}", docID);
                        final Map<String, Object> source = getNewDocument(content, information.get(store));
                        requestByStore.add(ElasticSearchMutation.createUpdateRequest(indexStoreName, store, docID,
                                source));
                    }
                }
                requests.addAll(requestByStore);
            }
            if (!requests.isEmpty())
                client.bulkRequest(requests, null);
        } catch (final Exception e) {
            throw convert(e);
        }
    }

    private ElasticSearchSetup.Connection interfaceConfiguration(Configuration config) {
        final ElasticSearchSetup clientMode = ConfigOption.getEnumValue(config.get(INTERFACE), ElasticSearchSetup.class);

        try {
            return clientMode.connect(config);
        } catch (final IOException e) {
            throw new JanusGraphException(e);
        }
    }

    private String getIndexStoreName(String store) {

        if(indexStoreNameCacheEnabled){
            return indexStoreNamesCache.computeIfAbsent(store, generateIndexStoreNameFunction);
        }

        return generateIndexStoreName(store);
    }

    private String generateIndexStoreName(String store){
        return indexName + INDEX_NAME_SEPARATOR + store.toLowerCase();
    }

    private BackendException convert(Exception esException) {
        if (esException instanceof InterruptedException) {
            return new TemporaryBackendException("Interrupted while waiting for response", esException);
        } else {
            return new PermanentBackendException("Unknown exception while executing index operation", esException);
        }
    }
}
