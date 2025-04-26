package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.model.CassandraTagOperation;

import java.util.Map;
import java.util.Stack;

public interface AtlasRollbackHandler {
    void rollbackCassandraTagOperations(Map<String, Stack<CassandraTagOperation>> cassandraOps);
}
