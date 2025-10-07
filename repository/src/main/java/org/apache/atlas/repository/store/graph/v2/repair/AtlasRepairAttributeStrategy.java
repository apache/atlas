package org.apache.atlas.repository.store.graph.v2.repair;


import org.apache.atlas.exception.AtlasBaseException;

import java.util.Set;

public interface AtlasRepairAttributeStrategy {

    String getRepairType();
    void repair(Set<String> entityGuids, String attributeName) throws AtlasBaseException;
    void validate (Set<String> entityGuids, String attributeName) throws AtlasBaseException;

}


