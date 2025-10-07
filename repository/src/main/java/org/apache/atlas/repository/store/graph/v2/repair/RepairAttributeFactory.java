package org.apache.atlas.repository.store.graph.v2.repair;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.TransactionInterceptHelper;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Set;

@Component
public class RepairAttributeFactory {

    private final EntityGraphRetriever entityRetriever;
    private final TransactionInterceptHelper transactionInterceptHelper;

    @Inject
    public RepairAttributeFactory(EntityGraphRetriever entityRetriever,
                                  TransactionInterceptHelper transactionInterceptHelper) {
        this.entityRetriever = entityRetriever;
        this.transactionInterceptHelper = transactionInterceptHelper;
    }

    public AtlasRepairAttributeStrategy getStrategy(String repairType, Set<String> entityGuids) throws AtlasBaseException {
        switch (repairType) {
            case "REMOVE_INVALID_GUIDS":
                return new RemoveInvalidGuidsRepairStrategy(entityRetriever, entityGuids, transactionInterceptHelper);
            default:
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                        "Unsupported repair type: " + repairType + ". Supported types: [REMOVE_INVALID_GUIDS]");
        }
    }

    public boolean isValidRepairType(String repairType) {
        return "REMOVE_INVALID_GUIDS".equals(repairType);
    }
}