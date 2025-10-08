package org.apache.atlas.repository.store.graph.v2.repair;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.TransactionInterceptHelper;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Set;

@Component
public class RepairAttributeFactory {

    private final EntityGraphRetriever entityRetriever;
    private final TransactionInterceptHelper transactionInterceptHelper;
    private final AtlasGraph graph;

    @Inject
    public RepairAttributeFactory(EntityGraphRetriever entityRetriever,
                                  TransactionInterceptHelper transactionInterceptHelper, AtlasGraph graph) {
        this.entityRetriever = entityRetriever;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.graph = graph;
    }

    public AtlasRepairAttributeStrategy getStrategy(String repairType, Set<String> entityGuids) throws AtlasBaseException {
        switch (repairType) {
            case "REMOVE_INVALID_OUTPUT_PORT_GUIDS":
                return new RemoveInvalidGuidsRepairStrategy(entityRetriever, transactionInterceptHelper, graph);
            default:
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Unsupported repair type: " + repairType + ". Supported types: [REMOVE_INVALID_OUTPUT_PORT_GUIDS]");
        }
    }

    public boolean isValidRepairType(String repairType) {
        return "REMOVE_INVALID_OUTPUT_PORT_GUIDS".equals(repairType);
    }
}