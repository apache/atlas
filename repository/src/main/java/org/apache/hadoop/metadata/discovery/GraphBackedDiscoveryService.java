package org.apache.hadoop.metadata.discovery;

import com.google.common.base.Preconditions;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.repository.MetadataRepository;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

public class GraphBackedDiscoveryService implements DiscoveryService {

    private final MetadataRepository repository;

    @Inject
    GraphBackedDiscoveryService(MetadataRepository repository) throws MetadataException {
        this.repository = repository;
    }

    /**
     * Assumes the User is familiar with the persistence structure of the Repository.
     * The given query is run uninterpreted against the underlying Graph Store.
     * The results are returned as a List of Rows. each row is a Map of Key,Value pairs.
     *
     * @param gremlinQuery query in gremlin dsl format
     * @return List of Maps
     * @throws org.apache.hadoop.metadata.MetadataException
     */
    @Override
    public List<Map<String, String>> searchByGremlin(String gremlinQuery) throws MetadataException {
        Preconditions.checkNotNull(gremlinQuery, "gremlin query name cannot be null");
        // simple pass-through
        return repository.searchByGremlin(gremlinQuery);
    }
}
