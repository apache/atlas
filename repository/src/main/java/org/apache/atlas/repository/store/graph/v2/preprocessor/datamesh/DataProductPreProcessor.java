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
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class DataProductPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DomainPreProcessor.class);
    private AtlasEntityHeader parentDomain;
    private EntityMutationContext context;
    public DataProductPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                              AtlasGraph graph, EntityGraphMapper entityGraphMapper) {
        super(typeRegistry, entityRetriever, graph);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (operation == EntityMutations.EntityOperation.UPDATE && LOG.isDebugEnabled()) {
            LOG.debug("DataProductPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }
        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        setParent(entity, context);

        if (operation == EntityMutations.EntityOperation.UPDATE) {
            processUpdateDomain(entity, vertex);
        } else {
            LOG.error("DataProductPreProcessor.processAttributes: Operation not supported {}", operation);
        }
    }

    private void processUpdateDomain(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateDomain");
        String productName = (String) entity.getAttribute(NAME);
        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        AtlasEntityHeader currentParentDomainHeader = null;
        String currentParentDomainQualifiedName = "";

        AtlasEntity storedProduct = entityRetriever.toAtlasEntity(vertex);
        AtlasRelatedObjectId currentParentDomain = (AtlasRelatedObjectId) storedProduct.getRelationshipAttribute(DATA_DOMAIN);

        String newParentDomainQualifiedName = "";
        String superDomainQualifiedName = "";

        if(currentParentDomain != null){
            currentParentDomainHeader = entityRetriever.toAtlasEntityHeader(currentParentDomain.getGuid());
            currentParentDomainQualifiedName = (String) currentParentDomainHeader.getAttribute(QUALIFIED_NAME);
        }

        if (parentDomain != null) {
            newParentDomainQualifiedName = (String) parentDomain.getAttribute(QUALIFIED_NAME);
            superDomainQualifiedName = (String) parentDomain.getAttribute(SUPER_DOMAIN_QN);
            if(superDomainQualifiedName == null){
                superDomainQualifiedName = newParentDomainQualifiedName;
            }
        }

        if (!currentParentDomainQualifiedName.equals(newParentDomainQualifiedName) && entity.hasRelationshipAttribute(DATA_DOMAIN)) {
            //Auth check
            isAuthorized(currentParentDomainHeader, parentDomain);

            processMoveDataProductToAnotherDomain(entity, currentParentDomainQualifiedName, newParentDomainQualifiedName, vertexQnName, superDomainQualifiedName);
            entity.setAttribute(PARENT_DOMAIN_QN, newParentDomainQualifiedName);

        } else {
            String vertexName = vertex.getProperty(NAME, String.class);
            if (!vertexName.equals(productName)) {
                productExists(productName, newParentDomainQualifiedName);
            }
            entity.setAttribute(QUALIFIED_NAME, vertexQnName);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processMoveDataProductToAnotherDomain(AtlasEntity product,
                                                     String sourceDomainQualifiedName,
                                                     String targetDomainQualifiedName,
                                                     String currentDataProductQualifiedName,
                                                     String superDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processMoveDataProductToAnotherDomain");

        try {
            String domainName = (String) product.getAttribute(NAME);

            LOG.info("Moving dataProduct {} to Domain {}", domainName, targetDomainQualifiedName);

            productExists(domainName, targetDomainQualifiedName);

            String updatedQualifiedName = currentDataProductQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);

            product.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
            product.setAttribute(PARENT_DOMAIN_QN, targetDomainQualifiedName);
            product.setAttribute(SUPER_DOMAIN_QN, superDomainQualifiedName);

            LOG.info("Moved dataProduct {} to Domain {}", domainName, targetDomainQualifiedName);

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void setParent(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("DataProductPreProcessor.setParent");
        if (parentDomain == null) {
            Object relationshipAttribute = entity.getRelationshipAttribute(DATA_DOMAIN);

            if(relationshipAttribute instanceof AtlasObjectId){
                AtlasObjectId objectId = (AtlasObjectId) relationshipAttribute;
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
            else if(relationshipAttribute instanceof Map){
                Map<String, Object> relationshipMap = (Map<String, Object>) relationshipAttribute;
                if (StringUtils.isNotEmpty((String) relationshipMap.get("guid"))) {
                    AtlasVertex vertex = context.getVertex((String) relationshipMap.get("guid"));

                    if (vertex == null) {
                        parentDomain = entityRetriever.toAtlasEntityHeader((String) relationshipMap.get("guid"));
                    } else {
                        parentDomain = entityRetriever.toAtlasEntityHeader(vertex);
                    }

                }
                else  {
                    parentDomain = new AtlasEntityHeader((String) relationshipMap.get("typeName"), relationshipMap);

                }
            }
            else{
                LOG.warn("DataProductPreProcessor.setParent: Invalid relationshipAttribute {}", relationshipAttribute);
            }

        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void productExists(String productName, String parentDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("domainExists");

        boolean exists = false;
        try {
            List mustClauseList = new ArrayList();
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", DATA_PRODUCT_ENTITY_TYPE)));
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("term", mapOf("name.keyword", productName)));


            Map<String, Object> bool = new HashMap<>();
            if (parentDomain != null) {
                mustClauseList.add(mapOf("term", mapOf("parentDomainQualifiedName", parentDomainQualifiedName)));
            } else {
                List mustNotClauseList = new ArrayList();
                mustNotClauseList.add(mapOf("exists", mapOf("field", "parentDomainQualifiedName")));
                bool.put("must_not", mustNotClauseList);
            }

            bool.put("must", mustClauseList);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

            List<AtlasEntityHeader> products = indexSearchPaginated(dsl);

            if (CollectionUtils.isNotEmpty(products)) {
                for (AtlasEntityHeader product : products) {
                    String name = (String) product.getAttribute(NAME);
                    if (productName.equals(name)) {
                        exists = true;
                        break;
                    }
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

        if (exists) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, productName);
        }
    }

}
