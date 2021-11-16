package org.apache.atlas.repository.store.graph.v2.glossary;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.graphdb.AtlasVertex;

public interface PreProcessor {
    void processAttributes(AtlasStruct entity, AtlasVertex vertex) throws AtlasBaseException;
}
