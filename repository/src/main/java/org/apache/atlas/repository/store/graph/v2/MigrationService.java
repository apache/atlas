package org.apache.atlas.repository.store.graph.v2;

public interface MigrationService {
    Boolean startMigration() throws Exception;
}
