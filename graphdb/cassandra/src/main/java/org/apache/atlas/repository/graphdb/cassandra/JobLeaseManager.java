package org.apache.atlas.repository.graphdb.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Distributed lease manager backed by Cassandra LWT (lightweight transactions).
 *
 * Provides mutual exclusion for background jobs across multiple pods without
 * requiring any external coordination service. Each lease is a single row in
 * the {@code job_leases} table with a TTL — if the holder crashes, the lease
 * expires automatically and another pod can acquire it.
 *
 * All operations are idempotent and safe under concurrent access.
 */
public class JobLeaseManager {

    private static final Logger LOG = LoggerFactory.getLogger(JobLeaseManager.class);

    private final CqlSession session;
    private final String podId;

    private final PreparedStatement acquireStmt;
    private final PreparedStatement releaseStmt;
    private final PreparedStatement selectStmt;

    public JobLeaseManager(CqlSession session) {
        this.session = session;
        this.podId = resolvePodId();

        // INSERT IF NOT EXISTS — only succeeds if no row exists (or TTL has expired)
        this.acquireStmt = session.prepare(
            "INSERT INTO job_leases (job_name, owner, acquired_at) VALUES (?, ?, ?) IF NOT EXISTS USING TTL ?"
        );

        // DELETE IF owner = podId — only the current owner can release
        this.releaseStmt = session.prepare(
            "DELETE FROM job_leases WHERE job_name = ? IF owner = ?"
        );

        // Simple read to check current owner
        this.selectStmt = session.prepare(
            "SELECT owner FROM job_leases WHERE job_name = ?"
        );

        LOG.info("JobLeaseManager initialized, podId={}", podId);
    }

    /**
     * Attempts to acquire a lease for the given job. Returns true if this pod
     * now holds the lease, false if another pod already holds it.
     *
     * The lease auto-expires after {@code ttlSeconds} if not explicitly released,
     * providing crash tolerance.
     */
    public boolean tryAcquire(String jobName, int ttlSeconds) {
        try {
            ResultSet rs = session.execute(acquireStmt.bind(jobName, podId, Instant.now(), ttlSeconds));
            boolean applied = rs.wasApplied();

            if (applied) {
                LOG.debug("Lease '{}' acquired by pod '{}' (TTL={}s)", jobName, podId, ttlSeconds);
            } else {
                // Check who holds it (for debug logging)
                Row row = rs.one();
                String currentOwner = row != null ? row.getString("owner") : "unknown";
                if (podId.equals(currentOwner)) {
                    // We already hold it (re-entrant case) — treat as success
                    LOG.debug("Lease '{}' already held by this pod '{}'", jobName, podId);
                    return true;
                }
                LOG.debug("Lease '{}' held by pod '{}', skipping", jobName, currentOwner);
            }
            return applied;
        } catch (Exception e) {
            LOG.warn("Failed to acquire lease '{}': {}", jobName, e.getMessage());
            return false;
        }
    }

    /**
     * Releases the lease for the given job. Only succeeds if this pod is the current owner.
     * Uses a conditional DELETE (LWT) to prevent a pod from releasing another pod's lease.
     */
    public void release(String jobName) {
        try {
            ResultSet rs = session.execute(releaseStmt.bind(jobName, podId));
            if (rs.wasApplied()) {
                LOG.debug("Lease '{}' released by pod '{}'", jobName, podId);
            } else {
                LOG.debug("Lease '{}' not released — not owned by this pod '{}'", jobName, podId);
            }
        } catch (Exception e) {
            LOG.warn("Failed to release lease '{}': {}", jobName, e.getMessage());
        }
    }

    /**
     * Checks whether this pod currently holds the given lease.
     */
    public boolean isHeldByMe(String jobName) {
        try {
            ResultSet rs = session.execute(selectStmt.bind(jobName));
            Row row = rs.one();
            return row != null && podId.equals(row.getString("owner"));
        } catch (Exception e) {
            LOG.warn("Failed to check lease '{}': {}", jobName, e.getMessage());
            return false;
        }
    }

    public String getPodId() {
        return podId;
    }

    private static String resolvePodId() {
        // Kubernetes sets HOSTNAME to the pod name (e.g., "atlas-main-0")
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isEmpty()) {
            return hostname;
        }
        // Fallback for local dev: use PID
        return "local-" + ProcessHandle.current().pid();
    }
}
