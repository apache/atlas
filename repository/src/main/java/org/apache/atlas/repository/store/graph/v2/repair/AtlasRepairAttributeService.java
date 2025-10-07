package org.apache.atlas.repository.store.graph.v2.repair;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.AtlasErrorCode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Set;

@Component
public class AtlasRepairAttributeService {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasRepairAttributeService.class);

    private final RepairAttributeFactory repairAttributeFactory;

    @Inject
    public AtlasRepairAttributeService(RepairAttributeFactory repairAttributeFactory) {
        this.repairAttributeFactory = repairAttributeFactory;
    }

    public void repairAttributes(String attributeName, String repairType, Set<String> entityGuids)
            throws AtlasBaseException {

        validateRequest(attributeName, repairType, entityGuids);

        LOG.info("Starting attribute repair - Type: {}, Attribute: {}, Entities: {}",
                repairType, attributeName, entityGuids.size());

        try {
            AtlasRepairAttributeStrategy strategy = repairAttributeFactory.getStrategy(repairType, entityGuids);

            strategy.validate(entityGuids, attributeName);
            strategy.repair(entityGuids, attributeName);

            LOG.info("Successfully completed attribute repair - Type: {}, Attribute: {}, Entities: {}",
                    repairType, attributeName, entityGuids.size());

        } catch (Exception e) {
            LOG.error("Error during attribute repair - Type: {}, Attribute: {}, Entities: {}",
                    repairType, attributeName, entityGuids.size(), e);
            throw e;
        }
    }

    private void validateRequest(String attributeName, String repairType, Set<String> entityGuids)
            throws AtlasBaseException {

        if (StringUtils.isEmpty(attributeName)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Attribute name cannot be empty");
        }

        if (StringUtils.isEmpty(repairType)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Repair type cannot be empty");
        }

        if (CollectionUtils.isEmpty(entityGuids)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Entity GUIDs cannot be empty");
        }

        if (entityGuids.size() > 1000) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                    "Too many entities. Maximum allowed: 1000, provided: " + entityGuids.size());
        }
    }
}