package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class DataProductPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DataProductPreProcessor.class);
    private AtlasEntityHeader parentDomain;
    private EntityMutationContext context;
    private List<String> currentResources;
    private Map<String, String> updatedPolicyResources;
    public DataProductPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                              AtlasGraph graph) {
        super(typeRegistry, entityRetriever, graph);
        this.currentResources = new ArrayList<>();
        this.updatedPolicyResources = new HashMap<>();
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (LOG.isDebugEnabled()) {
            LOG.debug("DataProductPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }
        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        setParent(entity);

        switch (operation) {
            case CREATE:
                processCreateProduct(entity, vertex);
                break;
            case UPDATE:
                processUpdateDomain(entity, vertex);
                break;
        }
    }

    private void processCreateProduct(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateProduct");
        String productName = (String) entity.getAttribute(NAME);
        String parentDomainQualifiedName = (String) entity.getAttribute(PARENT_DOMAIN_QN);
        if(parentDomain != null ){
            parentDomainQualifiedName = (String) parentDomain.getAttribute(QUALIFIED_NAME);
        }

        productExists(productName, parentDomainQualifiedName);

        RequestContext.get().endMetricRecord(metricRecorder);
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

        if(entity.hasRelationshipAttribute(DATA_DOMAIN) && entity.getRelationshipAttribute(DATA_DOMAIN) == null){
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "DataProduct can only be moved to another Domain.");
        }

        if (!currentParentDomainQualifiedName.equals(newParentDomainQualifiedName) && entity.hasRelationshipAttribute(DATA_DOMAIN)) {
            //Auth check
            isAuthorized(currentParentDomainHeader, parentDomain);

            processMoveDataProductToAnotherDomain(entity, currentParentDomainQualifiedName, newParentDomainQualifiedName, vertexQnName, superDomainQualifiedName);
            updatePolicy(this.currentResources, this.updatedPolicyResources, this.context);
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
            String productName = (String) product.getAttribute(NAME);
            String updatedQualifiedName;

            if(StringUtils.isEmpty(targetDomainQualifiedName)){
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "DataProduct can only be moved to another Domain.");
            }

            LOG.info("Moving dataProduct {} to Domain {}", productName, targetDomainQualifiedName);

            productExists(productName, targetDomainQualifiedName);

            if(StringUtils.isEmpty(sourceDomainQualifiedName)){
                updatedQualifiedName = targetDomainQualifiedName + "/" + product.getAttribute(QUALIFIED_NAME);
            }
            else{
                updatedQualifiedName = currentDataProductQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);
            }

            product.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
            product.setAttribute(PARENT_DOMAIN_QN, targetDomainQualifiedName);
            product.setAttribute(SUPER_DOMAIN_QN, superDomainQualifiedName);

            //Store domainPolicies and resources to be updated
            String currentResource = "entity:"+ currentDataProductQualifiedName;
            String updatedResource = "entity:"+ updatedQualifiedName;
            this.updatedPolicyResources.put(currentResource, updatedResource);
            this.currentResources.add(currentResource);

            LOG.info("Moved dataProduct {} to Domain {}", productName, targetDomainQualifiedName);

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void setParent(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("DataProductPreProcessor.setParent");
        if (parentDomain == null) {
            Object relationshipAttribute = entity.getRelationshipAttribute(DATA_DOMAIN);
            Set<String> attributes = new HashSet<>(Arrays.asList(QUALIFIED_NAME, SUPER_DOMAIN_QN, PARENT_DOMAIN_QN, "__typeName"));

            parentDomain = getParent(relationshipAttribute, attributes);

        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void productExists(String productName, String parentDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("domainExists");

        boolean exists = false;
        try {
            List<Map<String, Object>> mustClauseList = new ArrayList();
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", DATA_PRODUCT_ENTITY_TYPE)));
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("term", mapOf("name.keyword", productName)));


            Map<String, Object> bool = new HashMap<>();
            if (parentDomain != null && StringUtils.isNotEmpty(parentDomainQualifiedName)){
                mustClauseList.add(mapOf("term", mapOf("parentDomainQualifiedName", parentDomainQualifiedName)));
            } else {
                List<Map<String, Object>> mustNotClauseList = new ArrayList();
                mustNotClauseList.add(mapOf("exists", mapOf("field", "parentDomainQualifiedName")));
                bool.put("must_not", mustNotClauseList);
            }

            bool.put("must", mustClauseList);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

            List<AtlasEntityHeader> products = indexSearchPaginated(dsl, null, this.discovery);

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
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, productName+" already exists in the domain");
        }
    }

}
