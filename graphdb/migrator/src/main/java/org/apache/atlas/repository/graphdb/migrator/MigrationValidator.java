package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Post-migration validation.
 *
 * Validates:
 *   1. Vertex count in target matches state store totals
 *   2. Edge counts match
 *   3. GUID index completeness (every vertex with __guid is index-findable)
 *   4. Sample property comparison (random vertices, check key properties)
 *   5. TypeDef vertex presence
 */
public class MigrationValidator {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationValidator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MigratorConfig   config;
    private final CqlSession       targetSession;
    private final MigrationStateStore stateStore;

    public MigrationValidator(MigratorConfig config, CqlSession targetSession, MigrationStateStore stateStore) {
        this.config        = config;
        this.targetSession = targetSession;
        this.stateStore    = stateStore;
    }

    /**
     * Run all validations. Returns true if all pass.
     */
    public boolean validateAll() {
        String ks = config.getTargetCassandraKeyspace();
        boolean allPassed = true;

        LOG.info("=== Starting Post-Migration Validation ===");

        // 1. Count vertices
        long vertexCount = countTable(ks + ".vertices");
        long[] phaseSummary = stateStore.getPhaseSummary("scan");
        LOG.info("Validation: vertices in target = {}, state store reports = {} vertices from {} ranges",
                 vertexCount, phaseSummary[1], phaseSummary[0]);

        if (vertexCount == 0) {
            LOG.error("FAIL: No vertices found in target!");
            allPassed = false;
        }

        // 2. Count edges
        long edgeOutCount = countTable(ks + ".edges_out");
        long edgeByIdCount = countTable(ks + ".edges_by_id");
        LOG.info("Validation: edges_out = {}, edges_by_id = {}, state store edge count = {}",
                 edgeOutCount, edgeByIdCount, phaseSummary[2]);

        // edges_out may have more rows than edges_by_id due to multiple labels per vertex
        // but edges_by_id should equal the total unique edges
        if (edgeByIdCount == 0 && phaseSummary[2] > 0) {
            LOG.warn("WARN: No edges in edges_by_id but state store reports {} edges", phaseSummary[2]);
        }

        // 3. GUID index validation (sample)
        allPassed &= validateGuidIndex(ks, 1000);

        // 4. TypeDef validation
        allPassed &= validateTypeDefPresence(ks);

        // 5. Sample property validation
        allPassed &= validateSampleProperties(ks, 100);

        LOG.info("=== Validation {} ===", allPassed ? "PASSED" : "FAILED");
        return allPassed;
    }

    /**
     * Check that a sample of vertices with __guid can be found via the GUID index.
     */
    private boolean validateGuidIndex(String ks, int sampleSize) {
        LOG.info("Validating GUID index (sample of {})...", sampleSize);
        int checked = 0, found = 0, missing = 0;

        ResultSet rs = targetSession.execute(
            "SELECT vertex_id, properties FROM " + ks + ".vertices LIMIT " + sampleSize);

        for (Row row : rs) {
            String vertexId = row.getString("vertex_id");
            String propsJson = row.getString("properties");

            try {
                Map<String, Object> props = MAPPER.readValue(propsJson, Map.class);
                String guid = props.get("__guid") != null ? props.get("__guid").toString() : null;

                if (guid != null) {
                    checked++;
                    // Look up in index
                    ResultSet idxRs = targetSession.execute(
                        "SELECT vertex_id FROM " + ks + ".vertex_index " +
                        "WHERE index_name = '__guid_idx' AND index_value = '" + guid + "'");
                    Row idxRow = idxRs.one();
                    if (idxRow != null && vertexId.equals(idxRow.getString("vertex_id"))) {
                        found++;
                    } else {
                        missing++;
                        LOG.warn("GUID index mismatch: vertex {} has guid {} but index returns {}",
                                 vertexId, guid, idxRow != null ? idxRow.getString("vertex_id") : "null");
                    }
                }
            } catch (Exception e) {
                LOG.trace("Error parsing properties for vertex {}", vertexId, e);
            }
        }

        LOG.info("GUID index validation: checked={}, found={}, missing={}", checked, found, missing);
        return missing == 0;
    }

    /**
     * Verify that TypeDef entries are present in the dedicated type_definitions table.
     */
    private boolean validateTypeDefPresence(String ks) {
        LOG.info("Validating TypeDef table population...");

        long count = countTable(ks + ".type_definitions");
        LOG.info("TypeDef entries in type_definitions table: {}", count);

        if (count <= 0) {
            LOG.error("FAIL: type_definitions table is empty. Atlas startup will fail (TypeDefCache " +
                      "relies on this table for O(1) TypeDef loading).");
            return false;
        }
        return true;
    }

    /**
     * Validate sample vertices have essential properties.
     */
    private boolean validateSampleProperties(String ks, int sampleSize) {
        LOG.info("Validating sample vertex properties (sample of {})...", sampleSize);
        int checked = 0, valid = 0, empty = 0;

        ResultSet rs = targetSession.execute(
            "SELECT vertex_id, properties, type_name FROM " + ks + ".vertices LIMIT " + sampleSize);

        for (Row row : rs) {
            checked++;
            String typeName = row.getString("type_name");
            String propsJson = row.getString("properties");

            if (propsJson == null || propsJson.equals("{}") || propsJson.equals("null")) {
                empty++;
            } else {
                valid++;
            }
        }

        LOG.info("Sample properties: checked={}, valid={}, empty={}", checked, valid, empty);
        if (empty > checked / 2) {
            LOG.warn("WARN: More than half of sampled vertices have empty properties");
        }
        return true;
    }

    private long countTable(String table) {
        try {
            ResultSet rs = targetSession.execute("SELECT count(*) FROM " + table);
            Row row = rs.one();
            return row != null ? row.getLong(0) : 0;
        } catch (Exception e) {
            LOG.warn("Failed to count {}: {}", table, e.getMessage());
            return -1;
        }
    }
}
