package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.graphdb.elasticsearch.AtlasElasticsearchDatabase;
import org.apache.atlas.type.AtlasType;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CassandraGraphQuery implements AtlasGraphQuery<CassandraVertex, CassandraEdge> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraGraphQuery.class);

    private final CassandraGraph graph;
    private final List<Predicate> predicates;
    private final List<AtlasGraphQuery<CassandraVertex, CassandraEdge>> orQueries;
    private String sortKey;
    private SortOrder sortOrder;
    private final boolean isChildQuery;

    public CassandraGraphQuery(CassandraGraph graph) {
        this(graph, false);
    }

    private CassandraGraphQuery(CassandraGraph graph, boolean isChildQuery) {
        this.graph        = graph;
        this.predicates   = new ArrayList<>();
        this.orQueries    = new ArrayList<>();
        this.isChildQuery = isChildQuery;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> has(String propertyKey, Object value) {
        predicates.add(new HasPredicate(propertyKey, value));
        return this;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> in(String propertyKey, Collection<?> values) {
        predicates.add(new InPredicate(propertyKey, values));
        return this;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> has(String propertyKey, QueryOperator op, Object values) {
        predicates.add(new OperatorPredicate(propertyKey, op, values));
        return this;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> orderBy(String propertyKey, SortOrder order) {
        this.sortKey   = propertyKey;
        this.sortOrder = order;
        return this;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> or(List<AtlasGraphQuery<CassandraVertex, CassandraEdge>> childQueries) {
        orQueries.addAll(childQueries);
        return this;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> createChildQuery() {
        return new CassandraGraphQuery(graph, true);
    }

    @Override
    public boolean isChildQuery() {
        return isChildQuery;
    }

    @Override
    public AtlasGraphQuery<CassandraVertex, CassandraEdge> addConditionsFrom(AtlasGraphQuery<CassandraVertex, CassandraEdge> otherQuery) {
        if (otherQuery instanceof CassandraGraphQuery) {
            CassandraGraphQuery other = (CassandraGraphQuery) otherQuery;
            this.predicates.addAll(other.predicates);
            this.orQueries.addAll(other.orQueries);
        }
        return this;
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices() {
        return executeVertexQuery(0, -1);
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices(int limit) {
        return executeVertexQuery(0, limit);
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices(int offset, int limit) {
        return executeVertexQuery(offset, limit);
    }

    @Override
    public Iterable<Object> vertexIds() {
        return executeVertexIdQuery(0, -1);
    }

    @Override
    public Iterable<Object> vertexIds(int limit) {
        return executeVertexIdQuery(0, limit);
    }

    @Override
    public Iterable<Object> vertexIds(int offset, int limit) {
        return executeVertexIdQuery(offset, limit);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges() {
        return executeEdgeQuery(0, -1);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges(int limit) {
        return executeEdgeQuery(0, limit);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges(int offset, int limit) {
        return executeEdgeQuery(offset, limit);
    }

    // ---- Internal execution ----

    private Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> executeVertexQuery(int offset, int limit) {
        // Try 1:1 index-based lookup for simple has predicates
        if (predicates.size() >= 1 && orQueries.isEmpty()) {
            Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> indexed = tryIndexLookup();
            if (indexed != null) {
                List<AtlasVertex<CassandraVertex, CassandraEdge>> result = new ArrayList<>();
                for (AtlasVertex<CassandraVertex, CassandraEdge> v : indexed) {
                    if (matchesAllPredicates(v)) {
                        result.add(v);
                    }
                }
                return applyPaging(result, offset, limit);
            }
        }

        // Try 1:N property index lookup (e.g., findTypeVerticesByCategory / getAll)
        if (predicates.size() >= 1 && orQueries.isEmpty()) {
            List<AtlasVertex<CassandraVertex, CassandraEdge>> propertyIndexResults = tryPropertyIndexLookup();
            if (!propertyIndexResults.isEmpty()) {
                return applyPaging(propertyIndexResults, offset, limit);
            }
        }

        // Fallback: scan vertices that match via graph.getVertices for first predicate
        if (!predicates.isEmpty() && predicates.get(0) instanceof HasPredicate) {
            HasPredicate first = (HasPredicate) predicates.get(0);
            Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> candidates = graph.getVertices(first.key, first.value);

            List<AtlasVertex<CassandraVertex, CassandraEdge>> result = new ArrayList<>();
            for (AtlasVertex<CassandraVertex, CassandraEdge> v : candidates) {
                if (matchesAllPredicates(v)) {
                    result.add(v);
                }
            }
            return applyPaging(result, offset, limit);
        }

        return Collections.emptyList();
    }

    private Iterable<Object> executeVertexIdQuery(int offset, int limit) {
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices = executeVertexQuery(offset, limit);
        List<Object> ids = new ArrayList<>();
        for (AtlasVertex<CassandraVertex, CassandraEdge> v : vertices) {
            ids.add(v.getId());
        }
        return ids;
    }

    @SuppressWarnings("unchecked")
    private Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> executeEdgeQuery(int offset, int limit) {
        // Try edge index lookup for known property keys (e.g., relationship GUID)
        if (!predicates.isEmpty() && orQueries.isEmpty()) {
            for (Predicate p : predicates) {
                if (p instanceof HasPredicate) {
                    HasPredicate hp = (HasPredicate) p;

                    // Relationship GUID lookup via edge_index table
                    if (Constants.RELATIONSHIP_GUID_PROPERTY_KEY.equals(hp.key) && hp.value != null) {
                        String edgeId = graph.getIndexRepository().lookupEdge("_r__guid_idx", String.valueOf(hp.value));
                        if (edgeId != null) {
                            CassandraEdge edge = graph.getEdgeRepository().getEdge(edgeId, graph);
                            if (edge != null) {
                                return Collections.singletonList(edge);
                            }
                        }
                        LOG.debug("executeEdgeQuery: no edge found for {} = {}", hp.key, hp.value);
                        return Collections.emptyList();
                    }
                }
            }
        }

        return Collections.emptyList();
    }

    private Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> tryIndexLookup() {
        // Extract known predicate keys for pattern matching
        Map<String, String> hasPreds = new LinkedHashMap<>();
        for (Predicate p : predicates) {
            if (p instanceof HasPredicate) {
                HasPredicate hp = (HasPredicate) p;
                hasPreds.put(hp.key, String.valueOf(hp.value));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("tryIndexLookup: predicates={}", hasPreds);
        }

        // 1. __guid lookup (entity or TypeDef vertex by GUID)
        String guidValue = hasPreds.get("__guid");
        if (guidValue == null) guidValue = hasPreds.get("Property.__guid");
        if (guidValue == null) guidValue = hasPreds.get(Constants.GUID_PROPERTY_KEY);
        if (guidValue != null) {
            LOG.info("tryIndexLookup: GUID lookup for [{}]", guidValue);
            String vertexId = graph.getIndexRepository().lookupVertex("__guid_idx", guidValue);
            LOG.info("tryIndexLookup: __guid_idx returned vertexId=[{}]", vertexId);
            if (vertexId != null) {
                AtlasVertex<CassandraVertex, CassandraEdge> vertex = graph.getVertex(vertexId);
                if (vertex != null) {
                    return Collections.singletonList(vertex);
                }
                LOG.warn("tryIndexLookup: index had vertexId [{}] but vertex not found in Cassandra!", vertexId);
            }

            // Fallback 1: check vertex cache (for uncommitted vertices)
            List<AtlasVertex<CassandraVertex, CassandraEdge>> cached = scanVertexCache();
            if (!cached.isEmpty()) {
                return cached;
            }

            // Fallback 2: search ES for the GUID (the vertex might exist but index entry is missing)
            AtlasVertex<CassandraVertex, CassandraEdge> esVertex = lookupVertexByGuidFromES(guidValue);
            if (esVertex != null) {
                LOG.warn("tryIndexLookup: GUID [{}] found via ES fallback (vertex_id={}), Cassandra index was missing! Rebuilding index.",
                        guidValue, ((CassandraVertex) esVertex).getIdString());
                // Rebuild the missing index entry so future lookups are fast
                graph.getIndexRepository().addIndex("__guid_idx", guidValue, ((CassandraVertex) esVertex).getIdString());
                return Collections.singletonList(esVertex);
            }

            LOG.info("tryIndexLookup: GUID [{}] not found in index, cache, or ES", guidValue);
            return Collections.emptyList();
        }

        // 2. qualifiedName + __typeName composite lookup (entity vertex)
        String qn = hasPreds.get("qualifiedName");
        if (qn == null) qn = hasPreds.get("Property.qualifiedName");
        if (qn == null) qn = hasPreds.get("__u_qualifiedName");
        if (qn == null) qn = hasPreds.get("Property.__u_qualifiedName");
        String entityTypeName = hasPreds.get("__typeName");
        if (entityTypeName == null) entityTypeName = hasPreds.get("Property.__typeName");
        if (entityTypeName == null) entityTypeName = hasPreds.get(Constants.ENTITY_TYPE_PROPERTY_KEY);
        if (qn != null && entityTypeName != null) {
            String indexKey = qn + ":" + entityTypeName;
            String vertexId = graph.getIndexRepository().lookupVertex("qn_type_idx", indexKey);
            if (vertexId != null) {
                AtlasVertex<CassandraVertex, CassandraEdge> vertex = graph.getVertex(vertexId);
                if (vertex != null) {
                    return Collections.singletonList(vertex);
                }
            }
            return scanVertexCache();
        }

        // 3. VERTEX_TYPE + TYPENAME lookup (TypeDef vertex by name)
        //    Pattern: has("__type", "typeSystem").has("__type.name"/"__type_name", name)
        //    Uses dedicated TypeDefCache for O(1) in-memory lookup.
        String vertexType  = hasPreds.get(Constants.VERTEX_TYPE_PROPERTY_KEY);
        String typeDefName = hasPreds.get(Constants.TYPENAME_PROPERTY_KEY);
        if (vertexType != null && typeDefName != null) {
            // Try TypeDefCache first (Caffeine in-memory → Cassandra type_definitions table)
            String vertexId = graph.getTypeDefCache().getVertexIdByName(typeDefName);
            if (vertexId == null) {
                // Fall back to generic index (handles data written before TypeDefCache existed)
                vertexId = graph.getIndexRepository().lookupVertex("type_typename_idx", vertexType + ":" + typeDefName);
            }
            if (vertexId != null) {
                AtlasVertex<CassandraVertex, CassandraEdge> vertex = graph.getVertex(vertexId);
                if (vertex != null) {
                    return Collections.singletonList(vertex);
                }
            }
            return scanVertexCache();
        }

        return null; // No index available
    }

    /**
     * Fallback: search Elasticsearch for a vertex by GUID and load it from Cassandra.
     * This handles the case where the Cassandra vertex_index entry is missing
     * (e.g., entity was created before index code was added, or index write failed).
     */
    @SuppressWarnings("unchecked")
    private AtlasVertex<CassandraVertex, CassandraEdge> lookupVertexByGuidFromES(String guid) {
        try {
            RestClient client = AtlasElasticsearchDatabase.getLowLevelClient();
            if (client == null) {
                return null;
            }

            String indexName = Constants.VERTEX_INDEX_NAME;
            // Search ES for __guid field match; the _id is the vertex_id (UUID)
            String query = "{\"query\":{\"term\":{\"__guid\":\"" + guid + "\"}},\"size\":1,\"_source\":false}";
            Request req = new Request("POST", "/" + indexName + "/_search");
            req.setJsonEntity(query);
            Response resp = client.performRequest(req);

            String body = EntityUtils.toString(resp.getEntity());
            Map<String, Object> responseMap = AtlasType.fromJson(body, Map.class);
            if (responseMap == null) {
                return null;
            }

            Map<String, Object> hitsWrapper = (Map<String, Object>) responseMap.get("hits");
            if (hitsWrapper == null) {
                return null;
            }

            List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsWrapper.get("hits");
            if (hits == null || hits.isEmpty()) {
                LOG.info("lookupVertexByGuidFromES: no ES hits for guid [{}]", guid);
                return null;
            }

            String vertexId = (String) hits.get(0).get("_id");
            if (vertexId == null) {
                return null;
            }

            LOG.info("lookupVertexByGuidFromES: found vertexId [{}] for guid [{}]", vertexId, guid);
            return graph.getVertex(vertexId);
        } catch (Exception e) {
            LOG.debug("lookupVertexByGuidFromES failed for guid [{}]: {}", guid, e.getMessage());
            return null;
        }
    }

    /**
     * Try 1:N property index lookup for queries that return multiple vertices.
     * Currently supports: VERTEX_TYPE + TYPE_CATEGORY (findTypeVerticesByCategory / getAll).
     */
    @SuppressWarnings("unchecked")
    private List<AtlasVertex<CassandraVertex, CassandraEdge>> tryPropertyIndexLookup() {
        Map<String, String> hasPreds = new LinkedHashMap<>();
        for (Predicate p : predicates) {
            if (p instanceof HasPredicate) {
                HasPredicate hp = (HasPredicate) p;
                hasPreds.put(hp.key, String.valueOf(hp.value));
            }
        }

        // VERTEX_TYPE + TYPE_CATEGORY lookup (findTypeVerticesByCategory)
        // Uses dedicated TypeDefCache for fast category lookups, with generic index fallback.
        String vertexType   = hasPreds.get(Constants.VERTEX_TYPE_PROPERTY_KEY);
        String typeCategory = hasPreds.get(Constants.TYPE_CATEGORY_PROPERTY_KEY);
        if (vertexType != null && typeCategory != null) {
            // Try TypeDefCache first (Caffeine in-memory → Cassandra type_definitions_by_category)
            List<TypeDefRepository.TypeDefEntry> cacheEntries = graph.getTypeDefCache().getByCategory(typeCategory);
            Set<String> seenIds = new HashSet<>();
            List<AtlasVertex<CassandraVertex, CassandraEdge>> results = new ArrayList<>();

            if (cacheEntries != null && !cacheEntries.isEmpty()) {
                for (TypeDefRepository.TypeDefEntry entry : cacheEntries) {
                    AtlasVertex<CassandraVertex, CassandraEdge> vertex = graph.getVertex(entry.vertexId);
                    if (vertex != null && matchesAllPredicates(vertex)) {
                        results.add(vertex);
                        seenIds.add(entry.vertexId);
                    }
                }
            }

            // Also check generic index to pick up entries not yet in TypeDefCache
            // (e.g., typedefs created at runtime that the Caffeine cache may have evicted)
            List<String> vertexIds = graph.getIndexRepository().lookupVertices(
                    "type_category_idx", vertexType + ":" + typeCategory);
            for (String vertexId : vertexIds) {
                if (!seenIds.contains(vertexId)) {
                    AtlasVertex<CassandraVertex, CassandraEdge> vertex = graph.getVertex(vertexId);
                    if (vertex != null && matchesAllPredicates(vertex)) {
                        results.add(vertex);
                        seenIds.add(vertexId);
                    }
                }
            }

            // Also check vertex cache for uncommitted vertices
            for (AtlasVertex<CassandraVertex, CassandraEdge> v : scanVertexCache()) {
                if (!seenIds.contains(((CassandraVertex) v).getIdString())) {
                    results.add(v);
                }
            }

            LOG.debug("tryPropertyIndexLookup type_category [{}] found {} vertices",
                    typeCategory, results.size());
            return results;
        }

        return Collections.emptyList();
    }

    /**
     * Scan the in-memory vertex cache for vertices matching all predicates.
     * Used as fallback when Cassandra index entries haven't been committed yet.
     */
    @SuppressWarnings("unchecked")
    private List<AtlasVertex<CassandraVertex, CassandraEdge>> scanVertexCache() {
        List<AtlasVertex<CassandraVertex, CassandraEdge>> result = new ArrayList<>();
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> allCachedVertices = (Iterable) graph.getVertices();
        for (AtlasVertex<CassandraVertex, CassandraEdge> v : allCachedVertices) {
            if (matchesAllPredicates(v)) {
                result.add(v);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private boolean matchesAllPredicates(AtlasVertex<CassandraVertex, CassandraEdge> vertex) {
        for (Predicate p : predicates) {
            if (!p.matches(vertex)) {
                return false;
            }
        }

        if (!orQueries.isEmpty()) {
            boolean anyMatch = false;
            for (AtlasGraphQuery<CassandraVertex, CassandraEdge> orQuery : orQueries) {
                CassandraGraphQuery cq = (CassandraGraphQuery) orQuery;
                boolean allMatch = true;
                for (Predicate p : cq.predicates) {
                    if (!p.matches(vertex)) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch) {
                    anyMatch = true;
                    break;
                }
            }
            if (!anyMatch) {
                return false;
            }
        }

        return true;
    }

    private <T> List<T> applyPaging(List<T> items, int offset, int limit) {
        if (offset > 0 && offset < items.size()) {
            items = items.subList(offset, items.size());
        }
        if (limit > 0 && limit < items.size()) {
            items = items.subList(0, limit);
        }
        return items;
    }

    // ---- Predicate types ----

    private interface Predicate {
        boolean matches(AtlasVertex<CassandraVertex, CassandraEdge> vertex);
    }

    private static class HasPredicate implements Predicate {
        final String key;
        final Object value;

        HasPredicate(String key, Object value) {
            this.key   = key;
            this.value = value;
        }

        @Override
        public boolean matches(AtlasVertex<CassandraVertex, CassandraEdge> vertex) {
            Object actual = vertex.getProperty(key, Object.class);
            if (actual == null) {
                return value == null;
            }
            return actual.equals(value) || String.valueOf(actual).equals(String.valueOf(value));
        }
    }

    private static class InPredicate implements Predicate {
        final String key;
        final Collection<?> values;

        InPredicate(String key, Collection<?> values) {
            this.key    = key;
            this.values = values;
        }

        @Override
        public boolean matches(AtlasVertex<CassandraVertex, CassandraEdge> vertex) {
            Object actual = vertex.getProperty(key, Object.class);
            if (actual == null) {
                return false;
            }
            for (Object v : values) {
                if (actual.equals(v) || String.valueOf(actual).equals(String.valueOf(v))) {
                    return true;
                }
            }
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static class OperatorPredicate implements Predicate {
        final String key;
        final QueryOperator op;
        final Object value;

        OperatorPredicate(String key, QueryOperator op, Object value) {
            this.key   = key;
            this.op    = op;
            this.value = value;
        }

        @Override
        public boolean matches(AtlasVertex<CassandraVertex, CassandraEdge> vertex) {
            Object actual = vertex.getProperty(key, Object.class);
            if (actual == null) {
                return false;
            }

            if (op instanceof ComparisionOperator) {
                return matchComparison((ComparisionOperator) op, actual, value);
            }

            if (op instanceof MatchingOperator) {
                return matchString((MatchingOperator) op, String.valueOf(actual), String.valueOf(value));
            }

            return false;
        }

        private boolean matchComparison(ComparisionOperator cop, Object actual, Object expected) {
            if (actual instanceof Comparable && expected instanceof Comparable) {
                int cmp = ((Comparable) actual).compareTo(expected);
                switch (cop) {
                    case EQUAL:              return cmp == 0;
                    case NOT_EQUAL:          return cmp != 0;
                    case GREATER_THAN:       return cmp > 0;
                    case GREATER_THAN_EQUAL: return cmp >= 0;
                    case LESS_THAN:          return cmp < 0;
                    case LESS_THAN_EQUAL:    return cmp <= 0;
                }
            }
            return false;
        }

        private boolean matchString(MatchingOperator mop, String actual, String pattern) {
            switch (mop) {
                case CONTAINS: return actual.toLowerCase().contains(pattern.toLowerCase());
                case PREFIX:   return actual.startsWith(pattern);
                case SUFFIX:   return actual.endsWith(pattern);
                case REGEX:    return actual.matches(pattern);
            }
            return false;
        }
    }
}
