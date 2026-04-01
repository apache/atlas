package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class ClaimRepository {
    private static final Logger LOG = LoggerFactory.getLogger(ClaimRepository.class);

    private final CqlSession session;
    private final PreparedStatement insertClaimStmt;

    public ClaimRepository(CqlSession session) {
        this.session = session;
        this.insertClaimStmt = session.prepare(
                "INSERT INTO entity_claims (identity_key, vertex_id, claimed_at, source) VALUES (?, ?, ?, ?) IF NOT EXISTS");
    }

    public String claimOrGet(String identityKey, String candidateVertexId, String source) {
        if (identityKey == null || candidateVertexId == null) {
            return candidateVertexId;
        }

        try {
            Row appliedRow = session.execute(insertClaimStmt.bind(identityKey, candidateVertexId, Instant.now(), source)).one();
            if (appliedRow != null) {
                if (appliedRow.getBoolean("[applied]")) {
                    return candidateVertexId;
                }
                // INSERT IF NOT EXISTS returned [applied]=false — the existing row is in appliedRow
                String existing = appliedRow.getString("vertex_id");
                if (existing != null && !existing.isEmpty()) {
                    return existing;
                }
            }
        } catch (Exception e) {
            LOG.warn("claimOrGet insert failed for identityKey={} candidateVertexId={}: {}",
                    identityKey, candidateVertexId, e.getMessage());
        }

        return candidateVertexId;
    }
}
