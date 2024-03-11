package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.PARENT_DOMAIN;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.PARENT_DOMAIN_QN;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class DataProductPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DomainPreProcessor.class);
    private AtlasEntityHeader parentDomain;
    public DataProductPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                              AtlasGraph graph, EntityGraphMapper entityGraphMapper) {
        super(typeRegistry, entityRetriever, graph);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (LOG.isDebugEnabled()) {
            LOG.debug("DataProductPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

//        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        setParent(entity, context);
        processUpdateDomain(entity, vertex);
    }

    private void processUpdateDomain(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateDomain");
        String productName = (String) entity.getAttribute(NAME);
        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

//        if (StringUtils.isEmpty(catName) || isNameInvalid(catName)) {
//            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
//        }

        AtlasEntity storedProduct = entityRetriever.toAtlasEntity(vertex);
        AtlasRelatedObjectId currentDomain = (AtlasRelatedObjectId) storedProduct.getRelationshipAttribute(PARENT_DOMAIN);
        AtlasEntityHeader currentDomainHeader = entityRetriever.toAtlasEntityHeader(currentDomain.getGuid());
        String currentDomainQualifiedName = (String) currentDomainHeader.getAttribute(QUALIFIED_NAME);

        String newDomainQualifiedName = (String) parentDomain.getAttribute(QUALIFIED_NAME);

        if (!currentDomainQualifiedName.equals(newDomainQualifiedName)) {
            //Auth check
            isAuthorized(currentDomainHeader, parentDomain);

            processMoveDataProductToAnotherDomain(entity, currentDomainQualifiedName, newDomainQualifiedName, vertexQnName);
            entity.setAttribute(PARENT_DOMAIN_QN, newDomainQualifiedName);

        } else {
            String vertexName = vertex.getProperty(NAME, String.class);
            if (!vertexName.equals(productName)) {
                domainExists(productName, newDomainQualifiedName);
            }

            entity.setAttribute(QUALIFIED_NAME, vertexQnName);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processMoveDataProductToAnotherDomain(AtlasEntity product,
                                                     String sourceDomainQualifiedName,
                                                     String targetDomainQualifiedName,
                                                     String currentDataProductQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processMoveDataProductToAnotherDomain");

        try {
            String domainName = (String) product.getAttribute(NAME);

            LOG.info("Moving dataProduct {} to Domain {}", domainName, targetDomainQualifiedName);

            domainExists(domainName, targetDomainQualifiedName);

            String updatedQualifiedName = currentDataProductQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);

            product.setAttribute(QUALIFIED_NAME, updatedQualifiedName);

            LOG.info("Moved dataProduct {} to Domain {}", domainName, targetDomainQualifiedName);

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void setParent(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("DataProductPreProcessor.setParent");
        if (parentDomain == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(PARENT_DOMAIN);

            if (objectId != null) {
                if (StringUtils.isNotEmpty(objectId.getGuid())) {
                    AtlasVertex vertex = context.getVertex(objectId.getGuid());

                    if (vertex == null) {
                        parentDomain = entityRetriever.toAtlasEntityHeader(objectId.getGuid());
                    } else {
                        parentDomain = entityRetriever.toAtlasEntityHeader(vertex);
                    }

                } else if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                        StringUtils.isNotEmpty((String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                    parentDomain = new AtlasEntityHeader(objectId.getTypeName(), objectId.getUniqueAttributes());

                }
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void domainExists(String domainName, String domainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("domainExists");

        boolean exists = false;
        try {
            List mustClauseList = new ArrayList();
            mustClauseList.add(mapOf("term", mapOf("__dataDomain", domainQualifiedName)));
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", DATA_DOMAIN_ENTITY_TYPE)));
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("term", mapOf("name.keyword", domainName)));


            Map<String, Object> bool = new HashMap<>();
            if (parentDomain != null) {
                String parentQname = (String) parentDomain.getAttribute(QUALIFIED_NAME);
                mustClauseList.add(mapOf("term", mapOf("__parentDomain", parentQname)));
            } else {
                List mustNotClauseList = new ArrayList();
                mustNotClauseList.add(mapOf("exists", mapOf("field", "__parentDomain")));
                bool.put("must_not", mustNotClauseList);
            }

            bool.put("must", mustClauseList);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

            List<AtlasEntityHeader> domains = indexSearchPaginated(dsl);

            if (CollectionUtils.isNotEmpty(domains)) {
                for (AtlasEntityHeader domain : domains) {
                    String name = (String) domain.getAttribute(NAME);
                    if (domainName.equals(name)) {
                        exists = true;
                        break;
                    }
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

        if (exists) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, domainName);
        }
    }

}
