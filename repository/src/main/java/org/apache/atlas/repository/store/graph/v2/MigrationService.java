package org.apache.atlas.repository.store.graph.v2;

public interface MigrationService extends Runnable {
    void startMigration() throws Exception;
}
