package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;


public interface PreProcessor {

    void processAttributes(AtlasStruct entity, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException;
}
