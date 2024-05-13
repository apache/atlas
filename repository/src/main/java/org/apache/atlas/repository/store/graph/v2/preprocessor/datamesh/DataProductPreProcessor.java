package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.repository.util.AccessControlUtils;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AccessControlUtils.*;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class DataProductPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DataProductPreProcessor.class);

    private EntityMutationContext context;
    private AtlasEntityStore entityStore;
    private Map<String, String> updatedPolicyResources;

    public DataProductPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                                   AtlasGraph graph, AtlasEntityStore entityStore) {
        super(typeRegistry, entityRetriever, graph);
        this.updatedPolicyResources = new HashMap<>();
        this.entityStore = entityStore;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("DataProductPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }
        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;

        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreateProduct(entity,vertex);
                break;
            case UPDATE:
                processUpdateProduct(entity, vertex);
                break;
        }
    }

    private void processCreateProduct(AtlasEntity entity,AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateProduct");
        String productName = (String) entity.getAttribute(NAME);
        String parentDomainQualifiedName = (String) entity.getAttribute(PARENT_DOMAIN_QN_ATTR);

        AtlasEntityHeader parentDomain = getParent(entity);
        if(parentDomain != null ){
            parentDomainQualifiedName = (String) parentDomain.getAttribute(QUALIFIED_NAME);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(parentDomainQualifiedName));
        entity.setCustomAttributes(customAttributes);

        productExists(productName, parentDomainQualifiedName);

        String productGuid = vertex.getProperty("__guid", String.class);

        AtlasEntity policy = new AtlasEntity();
        policy.setTypeName(POLICY_ENTITY_TYPE);
        policy.setAttribute(NAME,entity.getAttribute(NAME));
        policy.setAttribute(QUALIFIED_NAME, productGuid+"/read-policy");
        policy.setAttribute(AccessControlUtils.ATTR_POLICY_ACTIONS, Arrays.asList("entity-read"));
        policy.setAttribute(ATTR_POLICY_CATEGORY,"datamesh");
        policy.setAttribute(ATTR_POLICY_TYPE,POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_RESOURCES, Arrays.asList("entity:"+entity.getAttribute(QUALIFIED_NAME)));
        policy.setAttribute("accessControlPolicyCategory",POLICY_CATEGORY_PERSONA);
        policy.setAttribute(ATTR_POLICY_RESOURCES_CATEGORY,"entity");
        policy.setAttribute(ATTR_POLICY_SERVICE_NAME,"atlas");
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY,"dataProduct");

        switch ((String) entity.getAttribute("daapVisibility")) {
            case "Private":
                // do nothing for private daapVisibility
                break;
            case "Protected":
                // create policy for policyUsers and policyGroups
                policy.setAttribute(ATTR_POLICY_USERS, entity.getAttribute("daapVisibilityUsers"));
                policy.setAttribute(ATTR_POLICY_GROUPS, entity.getAttribute("daapVisibilityGroups"));
                break;
            case "Public":
                // set empty user list
                policy.setAttribute(ATTR_POLICY_USERS, Arrays.asList());
                // set user groups oto public to allow access for all
                policy.setAttribute(ATTR_POLICY_GROUPS, Arrays.asList("Public"));
                break;
        }

        AtlasEntity.AtlasEntitiesWithExtInfo policiesExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        policiesExtInfo.addEntity(policy);
        EntityStream entityStream = new AtlasEntityStream(policiesExtInfo);
        entityStore.createOrUpdate(entityStream, false); // adding new policy

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateProduct(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateProduct");

        if(entity.hasRelationshipAttribute(DATA_DOMAIN_REL_TYPE) && entity.getRelationshipAttribute(DATA_DOMAIN_REL_TYPE) == null){
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "DataProduct can only be moved to another Domain.");
        }

        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        AtlasEntity storedProduct = entityRetriever.toAtlasEntity(vertex);
        AtlasRelatedObjectId currentParentDomainObjectId = (AtlasRelatedObjectId) storedProduct.getRelationshipAttribute(DATA_DOMAIN_REL_TYPE);

        String newParentDomainQualifiedName = null;
        String currentParentDomainQualifiedName = null;
        AtlasEntityHeader currentParentDomainHeader = null;

        if(currentParentDomainObjectId != null) {
            currentParentDomainHeader = entityRetriever.toAtlasEntityHeader(currentParentDomainObjectId.getGuid());
            currentParentDomainQualifiedName = (String) currentParentDomainHeader.getAttribute(QUALIFIED_NAME);
        }

        AtlasEntityHeader newParentDomainHeader = getParent(entity);
        if (newParentDomainHeader != null) {
            newParentDomainQualifiedName = (String) newParentDomainHeader.getAttribute(QUALIFIED_NAME);
        }

        if (newParentDomainQualifiedName != null && !newParentDomainQualifiedName.equals(currentParentDomainQualifiedName)) {
            //Auth check
            isAuthorized(currentParentDomainHeader, newParentDomainHeader);

            String newSuperDomainQualifiedName = (String) newParentDomainHeader.getAttribute(SUPER_DOMAIN_QN_ATTR);
            if(StringUtils.isEmpty(newSuperDomainQualifiedName)){
                newSuperDomainQualifiedName = newParentDomainQualifiedName;
            }

            processMoveDataProductToAnotherDomain(entity, currentParentDomainQualifiedName, newParentDomainQualifiedName, vertexQnName, newSuperDomainQualifiedName);

            updatePolicies(this.updatedPolicyResources, this.context);

        } else {
            String productCurrentName = vertex.getProperty(NAME, String.class);
            String productNewName = (String) entity.getAttribute(NAME);

            if (!productCurrentName.equals(productNewName)) {
                productExists(productNewName, currentParentDomainQualifiedName);
            }
            entity.setAttribute(QUALIFIED_NAME, vertexQnName);
        }

        // check for daapVisibility change
        String currentProductDaapVisibility = storedProduct.getAttribute("daapVisibility").toString();
        String newProductDaapVisibility = (String) entity.getAttribute(NAME);

        if (newProductDaapVisibility != null && !newProductDaapVisibility.equals(currentProductDaapVisibility)) {

            AtlasEntity policy = new AtlasEntity();
            policy.setTypeName(POLICY_ENTITY_TYPE);
            policy.setAttribute(NAME,entity.getAttribute(NAME));
            policy.setAttribute(QUALIFIED_NAME,storedProduct.getGuid()+"/read-policy");

            switch (currentProductDaapVisibility) {
                case "Private":
                    switch (newProductDaapVisibility) {
                        case "Protected":
                            // create policy for policyUsers and policyGroups
                            policy.setAttribute(ATTR_POLICY_USERS, entity.getAttribute("daapVisibilityUsers"));
                            policy.setAttribute(ATTR_POLICY_GROUPS, entity.getAttribute("daapVisibilityGroups"));
                            break;
                        case "Public":
                            // set empty user list
                            policy.setAttribute(ATTR_POLICY_USERS, Arrays.asList());
                            // set user groups oto public to allow access for all
                            policy.setAttribute(ATTR_POLICY_GROUPS, Arrays.asList("Public"));
                    }
                    break;
                case "Protected":
                    switch (newProductDaapVisibility) {
                        case "Private":
                            // create policy for policyUsers and policyGroups
                            policy.setAttribute(ATTR_POLICY_USERS,Arrays.asList());
                            policy.setAttribute(ATTR_POLICY_GROUPS,Arrays.asList());
                            break;
                        case "Public":
                            // set empty user list
                            policy.setAttribute(ATTR_POLICY_USERS, Arrays.asList());
                            // set user groups oto public to allow access for all
                            policy.setAttribute(ATTR_POLICY_GROUPS, Arrays.asList("Public"));
                    }
                    break;
                case "Public":
                    switch (newProductDaapVisibility) {
                        case "Private":
                            // create policy for policyUsers and policyGroups
                            policy.setAttribute(ATTR_POLICY_USERS,Arrays.asList());
                            policy.setAttribute(ATTR_POLICY_GROUPS,Arrays.asList());
                            break;
                        case "Protected":
                            // create policy for policyUsers and policyGroups
                            policy.setAttribute(ATTR_POLICY_USERS, entity.getAttribute("daapVisibilityUsers"));
                            policy.setAttribute(ATTR_POLICY_GROUPS, entity.getAttribute("daapVisibilityGroups"));
                            break;
                    }
                    break;
            }
            AtlasEntity.AtlasEntitiesWithExtInfo policiesExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
            policiesExtInfo.addEntity(policy);
            EntityStream entityStream = new AtlasEntityStream(policiesExtInfo);
            entityStore.createOrUpdate(entityStream, false); // adding new policy
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

            LOG.info("Moving dataProduct {} to Domain {}", productName, targetDomainQualifiedName);

            productExists(productName, targetDomainQualifiedName);

            String updatedQualifiedName;
            if(StringUtils.isEmpty(sourceDomainQualifiedName)){
                updatedQualifiedName = targetDomainQualifiedName + "/" + product.getAttribute(QUALIFIED_NAME);
            } else {
                updatedQualifiedName = currentDataProductQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);
            }

            product.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
            product.setAttribute(PARENT_DOMAIN_QN_ATTR, targetDomainQualifiedName);
            product.setAttribute(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);

            //Store domainPolicies and resources to be updated
            String currentResource = "entity:"+ currentDataProductQualifiedName;
            String updatedResource = "entity:"+ updatedQualifiedName;
            this.updatedPolicyResources.put(currentResource, updatedResource);

            LOG.info("Moved dataProduct {} to Domain {}", productName, targetDomainQualifiedName);

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private AtlasEntityHeader getParent(AtlasEntity productEntity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("DataProductPreProcessor.getParent");

        Object relationshipAttribute = productEntity.getRelationshipAttribute(DATA_DOMAIN_REL_TYPE);

        RequestContext.get().endMetricRecord(metricRecorder);
        return getParent(relationshipAttribute, PARENT_ATTRIBUTES);
    }

    private void productExists(String productName, String parentDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("domainExists");

        try {
            exists(DATA_PRODUCT_ENTITY_TYPE, productName, parentDomainQualifiedName);

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private static String createQualifiedName(String parentDomainQualifiedName) throws AtlasBaseException {
        if (StringUtils.isEmpty(parentDomainQualifiedName)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Parent Domain Qualified Name cannot be empty or null");
        }
        return parentDomainQualifiedName + "/product/" + getUUID();

    }
}
