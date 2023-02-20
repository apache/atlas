package org.apache.atlas.repository.store.graph.v2.preprocessor.readme;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.glossary.GlossaryPreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public class ReadmePreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryPreProcessor.class);

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;

    public ReadmePreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("GlossaryPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreateorUpdateReadme(entity);
                break;
            case UPDATE:
                processUpdateReadme(entity, context.getVertex(entity.getGuid()));
                break;
        }
    }

    private void processCreateorUpdateReadme(AtlasStruct entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateReadme");
//        String readmeName = (String) entity.getAttribute(NAME);
//
//        if (StringUtils.isEmpty(readmeName) || isNameInvalid(readmeName)) {
//            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
//        }

//        if (glossaryExists(glossaryName)) {
//            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_ALREADY_EXISTS,glossaryName);
//        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName((AtlasEntity) entity));
        RequestContext.get().endMetricRecord(metricRecorder);
    }
    private void processUpdateReadme(AtlasStruct entity, AtlasVertex vertex) throws AtlasBaseException {

        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        entity.setAttribute(QUALIFIED_NAME, vertexQnName);
    }

    public static String createQualifiedName(AtlasEntity entity) {
        AtlasObjectId asset = (AtlasObjectId) entity.getRelationshipAttribute("asset");
        return asset.getGuid() + "/readme" ;
    }
}
