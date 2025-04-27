package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.CassandraTagOperation;
import org.apache.atlas.model.ESDeferredOperation;

import java.util.List;
import java.util.Map;
import java.util.Stack;

public interface EntityMutationPostProcessor {
    void rollbackCassandraTagOperations(Map<String, Stack<CassandraTagOperation>> cassandraOps) throws AtlasBaseException;

    void executeESOperations(List<ESDeferredOperation> esDeferredOperations);
}
