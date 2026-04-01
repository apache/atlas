package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class IndexRepository {

    private static final Logger LOG = LoggerFactory.getLogger(IndexRepository.class);
    private static final int BATCH_CHUNK_SIZE = 100;

    private final CqlSession session;

    // 1:1 index (vertex_index table)
    private PreparedStatement insertIndexStmt;
    private PreparedStatement selectIndexStmt;
    private PreparedStatement deleteIndexStmt;

    // 1:N index (vertex_property_index table)
    private PreparedStatement insertPropertyIndexStmt;
    private PreparedStatement selectPropertyIndexStmt;
    private PreparedStatement deletePropertyIndexStmt;
    private PreparedStatement deletePropertyIndexVertexStmt;

    // 1:1 edge index (edge_index table)
    private PreparedStatement insertEdgeIndexStmt;
    private PreparedStatement selectEdgeIndexStmt;
    private PreparedStatement deleteEdgeIndexStmt;

    public IndexRepository(CqlSession session) {
        this.session = session;
        prepareStatements();
    }

    private void prepareStatements() {
        // 1:1 unique index
        insertIndexStmt = session.prepare(
            "INSERT INTO vertex_index (index_name, index_value, vertex_id) VALUES (?, ?, ?)"
        );

        selectIndexStmt = session.prepare(
            "SELECT vertex_id FROM vertex_index WHERE index_name = ? AND index_value = ?"
        );

        deleteIndexStmt = session.prepare(
            "DELETE FROM vertex_index WHERE index_name = ? AND index_value = ?"
        );

        // 1:N property index
        insertPropertyIndexStmt = session.prepare(
            "INSERT INTO vertex_property_index (index_name, index_value, vertex_id) VALUES (?, ?, ?)"
        );

        selectPropertyIndexStmt = session.prepare(
            "SELECT vertex_id FROM vertex_property_index WHERE index_name = ? AND index_value = ?"
        );

        deletePropertyIndexStmt = session.prepare(
            "DELETE FROM vertex_property_index WHERE index_name = ? AND index_value = ?"
        );

        deletePropertyIndexVertexStmt = session.prepare(
            "DELETE FROM vertex_property_index WHERE index_name = ? AND index_value = ? AND vertex_id = ?"
        );

        // 1:1 edge index
        insertEdgeIndexStmt = session.prepare(
            "INSERT INTO edge_index (index_name, index_value, edge_id) VALUES (?, ?, ?)"
        );

        selectEdgeIndexStmt = session.prepare(
            "SELECT edge_id FROM edge_index WHERE index_name = ? AND index_value = ?"
        );

        deleteEdgeIndexStmt = session.prepare(
            "DELETE FROM edge_index WHERE index_name = ? AND index_value = ?"
        );
    }

    // ---- 1:1 unique index (vertex_index) ----

    /**
     * Returns a bound INSERT statement for a 1:1 vertex index entry without executing it.
     * Used by CassandraGraph.commit() to combine vertex + index writes in a single LOGGED batch.
     */
    public BoundStatement bindInsertIndex(String indexName, String indexValue, String vertexId) {
        return insertIndexStmt.bind(indexName, indexValue, vertexId);
    }

    /**
     * Returns a bound INSERT statement for a 1:N property index entry without executing it.
     * Used by CassandraGraph.commit() to combine vertex + index writes in a single LOGGED batch.
     */
    public BoundStatement bindInsertPropertyIndex(String indexName, String indexValue, String vertexId) {
        return insertPropertyIndexStmt.bind(indexName, indexValue, vertexId);
    }

    public void addIndex(String indexName, String indexValue, String vertexId) {
        session.execute(insertIndexStmt.bind(indexName, indexValue, vertexId));
    }

    public String lookupVertex(String indexName, String indexValue) {
        LOG.info("lookupVertex: indexName=[{}], indexValue=[{}]", indexName, indexValue);
        ResultSet rs = session.execute(selectIndexStmt.bind(indexName, indexValue));
        Row row = rs.one();
        String result = row != null ? row.getString("vertex_id") : null;
        LOG.info("lookupVertex: result=[{}]", result);
        return result;
    }

    public void removeIndex(String indexName, String indexValue) {
        session.execute(deleteIndexStmt.bind(indexName, indexValue));
    }

    public void batchAddIndexes(List<IndexEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        for (int i = 0; i < entries.size(); i += BATCH_CHUNK_SIZE) {
            BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
            int end = Math.min(i + BATCH_CHUNK_SIZE, entries.size());
            for (int j = i; j < end; j++) {
                IndexEntry entry = entries.get(j);
                batch.addStatement(insertIndexStmt.bind(entry.indexName, entry.indexValue, entry.vertexId));
            }
            session.execute(batch.build());
        }
    }

    // ---- 1:N property index (vertex_property_index) ----

    public void addPropertyIndex(String indexName, String indexValue, String vertexId) {
        session.execute(insertPropertyIndexStmt.bind(indexName, indexValue, vertexId));
    }

    public List<String> lookupVertices(String indexName, String indexValue) {
        ResultSet rs = session.execute(selectPropertyIndexStmt.bind(indexName, indexValue));
        List<String> result = new ArrayList<>();
        for (Row row : rs) {
            result.add(row.getString("vertex_id"));
        }
        return result;
    }

    public void removePropertyIndex(String indexName, String indexValue) {
        session.execute(deletePropertyIndexStmt.bind(indexName, indexValue));
    }

    public void removePropertyIndexVertex(String indexName, String indexValue, String vertexId) {
        session.execute(deletePropertyIndexVertexStmt.bind(indexName, indexValue, vertexId));
    }

    public void batchAddPropertyIndexes(List<IndexEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        for (int i = 0; i < entries.size(); i += BATCH_CHUNK_SIZE) {
            BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
            int end = Math.min(i + BATCH_CHUNK_SIZE, entries.size());
            for (int j = i; j < end; j++) {
                IndexEntry entry = entries.get(j);
                batch.addStatement(insertPropertyIndexStmt.bind(entry.indexName, entry.indexValue, entry.vertexId));
            }
            session.execute(batch.build());
        }
    }

    // ---- 1:1 edge index (edge_index) ----

    public void addEdgeIndex(String indexName, String indexValue, String edgeId) {
        session.execute(insertEdgeIndexStmt.bind(indexName, indexValue, edgeId));
    }

    public String lookupEdge(String indexName, String indexValue) {
        ResultSet rs = session.execute(selectEdgeIndexStmt.bind(indexName, indexValue));
        Row row = rs.one();
        return row != null ? row.getString("edge_id") : null;
    }

    public void removeEdgeIndex(String indexName, String indexValue) {
        session.execute(deleteEdgeIndexStmt.bind(indexName, indexValue));
    }

    public void batchAddEdgeIndexes(List<EdgeIndexEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        // Chunk to avoid Cassandra batch_size_fail_threshold (default 50KB).
        int BATCH_LIMIT = 200;
        for (int i = 0; i < entries.size(); i += BATCH_LIMIT) {
            BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
            int end = Math.min(i + BATCH_LIMIT, entries.size());
            for (int j = i; j < end; j++) {
                EdgeIndexEntry entry = entries.get(j);
                batch.addStatement(insertEdgeIndexStmt.bind(entry.indexName, entry.indexValue, entry.edgeId));
            }
            session.execute(batch.build());
        }
    }

    // ---- Batch remove methods ----

    public void batchRemoveIndexes(List<IndexEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        int BATCH_LIMIT = 200;
        for (int i = 0; i < entries.size(); i += BATCH_LIMIT) {
            BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
            int end = Math.min(i + BATCH_LIMIT, entries.size());
            for (int j = i; j < end; j++) {
                IndexEntry entry = entries.get(j);
                batch.addStatement(deleteIndexStmt.bind(entry.indexName, entry.indexValue));
            }
            session.execute(batch.build());
        }
    }

    public void batchRemovePropertyIndexes(List<IndexEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        for (IndexEntry entry : entries) {
            batch.addStatement(deletePropertyIndexVertexStmt.bind(entry.indexName, entry.indexValue, entry.vertexId));
        }
        session.execute(batch.build());
    }

    public void batchRemoveEdgeIndexes(List<EdgeIndexEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        for (EdgeIndexEntry entry : entries) {
            batch.addStatement(deleteEdgeIndexStmt.bind(entry.indexName, entry.indexValue));
        }
        session.execute(batch.build());
    }

    // ---- Shared entry class ----

    public static class IndexEntry {
        public final String indexName;
        public final String indexValue;
        public final String vertexId;

        public IndexEntry(String indexName, String indexValue, String vertexId) {
            this.indexName  = indexName;
            this.indexValue = indexValue;
            this.vertexId   = vertexId;
        }
    }

    public static class EdgeIndexEntry {
        public final String indexName;
        public final String indexValue;
        public final String edgeId;

        public EdgeIndexEntry(String indexName, String indexValue, String edgeId) {
            this.indexName  = indexName;
            this.indexValue = indexValue;
            this.edgeId     = edgeId;
        }
    }
}
