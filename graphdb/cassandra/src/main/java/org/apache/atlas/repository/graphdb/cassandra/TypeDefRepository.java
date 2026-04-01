package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Dedicated Cassandra repository for TypeDef storage.
 * Provides O(1) lookup by type_name and O(N) lookup by category,
 * replacing the generic vertex_index / vertex_property_index tables
 * for TypeDef queries.
 */
public class TypeDefRepository {

    private static final Logger LOG = LoggerFactory.getLogger(TypeDefRepository.class);

    private final CqlSession session;

    private PreparedStatement insertTypeDefStmt;
    private PreparedStatement selectByNameStmt;
    private PreparedStatement selectByCategoryStmt;
    private PreparedStatement deleteByNameStmt;
    private PreparedStatement insertByCategoryStmt;
    private PreparedStatement deleteByCategoryStmt;

    public TypeDefRepository(CqlSession session) {
        this.session = session;
        prepareStatements();
    }

    private void prepareStatements() {
        insertTypeDefStmt = session.prepare(
            "INSERT INTO type_definitions (type_name, type_category, vertex_id, created_at, modified_at) " +
            "VALUES (?, ?, ?, ?, ?)"
        );

        selectByNameStmt = session.prepare(
            "SELECT type_name, type_category, vertex_id, created_at, modified_at " +
            "FROM type_definitions WHERE type_name = ?"
        );

        selectByCategoryStmt = session.prepare(
            "SELECT type_name, vertex_id FROM type_definitions_by_category WHERE type_category = ?"
        );

        deleteByNameStmt = session.prepare(
            "DELETE FROM type_definitions WHERE type_name = ?"
        );

        insertByCategoryStmt = session.prepare(
            "INSERT INTO type_definitions_by_category (type_category, type_name, vertex_id) VALUES (?, ?, ?)"
        );

        deleteByCategoryStmt = session.prepare(
            "DELETE FROM type_definitions_by_category WHERE type_category = ? AND type_name = ?"
        );
    }

    /**
     * Store a TypeDef entry, writing to both the primary table and the category index.
     */
    public void put(String typeName, String typeCategory, String vertexId) {
        Instant now = Instant.now();

        BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
        batch.addStatement(insertTypeDefStmt.bind(typeName, typeCategory, vertexId, now, now));
        batch.addStatement(insertByCategoryStmt.bind(typeCategory, typeName, vertexId));
        session.execute(batch.build());

        LOG.debug("TypeDefRepository.put: name={}, category={}, vertexId={}", typeName, typeCategory, vertexId);
    }

    /**
     * Look up a TypeDef vertex ID by type name. Returns null if not found.
     */
    public String getVertexIdByName(String typeName) {
        ResultSet rs = session.execute(selectByNameStmt.bind(typeName));
        Row row = rs.one();
        return row != null ? row.getString("vertex_id") : null;
    }

    /**
     * Get the category for a TypeDef by name. Returns null if not found.
     */
    public String getCategoryByName(String typeName) {
        ResultSet rs = session.execute(selectByNameStmt.bind(typeName));
        Row row = rs.one();
        return row != null ? row.getString("type_category") : null;
    }

    /**
     * Get all TypeDef vertex IDs for a given category (e.g., "ENTITY", "ENUM").
     */
    public List<TypeDefEntry> getByCategory(String typeCategory) {
        ResultSet rs = session.execute(selectByCategoryStmt.bind(typeCategory));
        List<TypeDefEntry> entries = new ArrayList<>();
        for (Row row : rs) {
            entries.add(new TypeDefEntry(row.getString("type_name"), row.getString("vertex_id")));
        }
        return entries;
    }

    /**
     * Delete a TypeDef by name from both tables.
     */
    public void delete(String typeName) {
        // Need to look up category first to delete from category index
        ResultSet rs = session.execute(selectByNameStmt.bind(typeName));
        Row row = rs.one();
        if (row != null) {
            String typeCategory = row.getString("type_category");
            BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
            batch.addStatement(deleteByNameStmt.bind(typeName));
            if (typeCategory != null) {
                batch.addStatement(deleteByCategoryStmt.bind(typeCategory, typeName));
            }
            session.execute(batch.build());
        }
    }

    public static class TypeDefEntry {
        public final String typeName;
        public final String vertexId;

        public TypeDefEntry(String typeName, String vertexId) {
            this.typeName = typeName;
            this.vertexId = vertexId;
        }
    }
}
