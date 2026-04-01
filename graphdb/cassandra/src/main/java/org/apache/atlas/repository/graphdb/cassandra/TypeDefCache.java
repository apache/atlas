package org.apache.atlas.repository.graphdb.cassandra;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Cache for TypeDef name → vertex_id mappings, backed by the dedicated
 * {@code type_definitions} Cassandra table via {@link TypeDefRepository}.
 *
 * <h3>Cross-pod consistency</h3>
 * <ul>
 *   <li><b>Name cache (Caffeine)</b>: Maps type_name → vertex_id.
 *       Safe to cache because vertex_ids are immutable — an update changes
 *       vertex properties (in the {@code vertices} table) but not the vertex_id.
 *       Cache misses always fall through to Cassandra, so new typedefs are
 *       immediately visible on all pods. Deleted typedefs result in a cached
 *       vertex_id whose vertex no longer exists — {@code getVertex()} returns
 *       null and callers handle it.</li>
 *   <li><b>Category lookups</b>: Always read directly from Cassandra's
 *       {@code type_definitions_by_category} table (single-partition read, &lt;1ms).
 *       Not cached, because category queries happen rarely (startup, type registry
 *       refresh) and caching them introduces cross-pod staleness when a new
 *       typedef is created on another pod.</li>
 * </ul>
 */
public class TypeDefCache {

    private static final Logger LOG = LoggerFactory.getLogger(TypeDefCache.class);

    private final TypeDefRepository typeDefRepository;

    // name → vertex_id: fast 1:1 lookup, safe to cache (vertex_ids are immutable)
    private final Cache<String, String> nameToVertexId;

    public TypeDefCache(TypeDefRepository typeDefRepository) {
        this.typeDefRepository = typeDefRepository;
        this.nameToVertexId   = Caffeine.newBuilder()
                .maximumSize(5000)
                .build();
    }

    /**
     * Look up a TypeDef vertex_id by type name.
     * Checks cache first, falls back to Cassandra.
     */
    public String getVertexIdByName(String typeName) {
        String vertexId = nameToVertexId.getIfPresent(typeName);
        if (vertexId != null) {
            return vertexId;
        }

        // Cache miss — read from Cassandra type_definitions table
        vertexId = typeDefRepository.getVertexIdByName(typeName);
        if (vertexId != null) {
            nameToVertexId.put(typeName, vertexId);
        }
        return vertexId;
    }

    /**
     * Get all TypeDef entries for a given category.
     * Always reads from Cassandra (single-partition query on type_definitions_by_category)
     * to ensure cross-pod consistency. Not cached.
     */
    public List<TypeDefRepository.TypeDefEntry> getByCategory(String typeCategory) {
        List<TypeDefRepository.TypeDefEntry> entries = typeDefRepository.getByCategory(typeCategory);

        // Populate the name cache opportunistically from category results
        for (TypeDefRepository.TypeDefEntry entry : entries) {
            nameToVertexId.put(entry.typeName, entry.vertexId);
        }

        return entries;
    }

    /**
     * Store a TypeDef and update the name cache.
     * Called during commit when a TypeDef vertex is created/updated.
     */
    public void put(String typeName, String typeCategory, String vertexId) {
        typeDefRepository.put(typeName, typeCategory, vertexId);
        nameToVertexId.put(typeName, vertexId);

        LOG.debug("TypeDefCache.put: name={}, category={}, vertexId={}", typeName, typeCategory, vertexId);
    }

    /**
     * Remove a TypeDef from Cassandra and invalidate the name cache.
     */
    public void remove(String typeName) {
        typeDefRepository.delete(typeName);
        nameToVertexId.invalidate(typeName);
    }

    /**
     * Invalidate the name cache. Called when a full refresh is needed.
     */
    public void invalidateAll() {
        nameToVertexId.invalidateAll();
    }
}
