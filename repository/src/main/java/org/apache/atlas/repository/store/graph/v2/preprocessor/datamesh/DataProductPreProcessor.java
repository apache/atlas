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
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils;
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

public class DataProductPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DataProductPreProcessor.class);
    private static final String PRIVATE = "Private";
    private static final String PROTECTED = "Protected";
    private static final String PUBLIC = "Public";



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

        createDaapVisibilityPolicy(entity,vertex);

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

        // check for daapVisibility change
        String currentProductDaapVisibility = storedProduct.getAttribute(DAAP_VISIBILITY).toString();
        String newProductDaapVisibility = (String) entity.getAttribute(DAAP_VISIBILITY);

        if (newParentDomainQualifiedName != null && !newParentDomainQualifiedName.equals(currentParentDomainQualifiedName)) {

            if (newProductDaapVisibility != null && !newProductDaapVisibility.equals(currentProductDaapVisibility)){
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Moving the product to another domain, along with the change in Dapp visibility, is not allowed");
            }

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

        if (newProductDaapVisibility != null && !newProductDaapVisibility.equals(currentProductDaapVisibility)) {
            updateDaapVisibilityPolicy(entity, storedProduct, currentProductDaapVisibility,newProductDaapVisibility);
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
        return parentDomainQualifiedName + "/product/" + PreProcessorUtils.getUUID();

    }

    private void createDaapVisibilityPolicy(AtlasEntity entity,AtlasVertex vertex) throws AtlasBaseException {
        String productGuid = vertex.getProperty("__guid", String.class);

        AtlasEntity policy = new AtlasEntity();
        policy.setTypeName(POLICY_ENTITY_TYPE);
        policy.setAttribute(NAME, entity.getAttribute(NAME));
        policy.setAttribute(QUALIFIED_NAME, productGuid + "/read-policy");
        policy.setAttribute(ATTR_POLICY_ACTIONS, Arrays.asList("entity-read"));
        policy.setAttribute(ATTR_POLICY_CATEGORY, MESH_POLICY_CATEGORY);
        policy.setAttribute(ATTR_POLICY_TYPE, POLICY_TYPE_ALLOW);
        policy.setAttribute(ATTR_POLICY_RESOURCES, Arrays.asList("entity:" + entity.getAttribute(QUALIFIED_NAME)));
        policy.setAttribute(ATTR_POLICY_RESOURCES_CATEGORY, POLICY_RESOURCE_CATEGORY_PERSONA_ENTITY);
        policy.setAttribute(ATTR_POLICY_SERVICE_NAME, "atlas");
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, DATA_PRODUCT_ENTITY_TYPE);

        switch ((String) entity.getAttribute(DAAP_VISIBILITY)) {
            case PRIVATE:
                // do nothing for private daapVisibility
                break;
            case PROTECTED:
                // create policy for policyUsers and policyGroups
                policy.setAttribute(ATTR_POLICY_USERS, entity.getAttribute(DAAP_VISIBILITY_USERS));
                policy.setAttribute(ATTR_POLICY_GROUPS, entity.getAttribute(DAAP_VISIBILITY_GROUPS));
                break;
            case PUBLIC:
                // set empty user list
                policy.setAttribute(ATTR_POLICY_USERS, Arrays.asList());
                // set user groups oto public to allow access for all
                policy.setAttribute(ATTR_POLICY_GROUPS, Arrays.asList("public"));
                break;
        }

        try {
            RequestContext.get().setSkipAuthorizationCheck(true);
            AtlasEntity.AtlasEntitiesWithExtInfo policiesExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
            policiesExtInfo.addEntity(policy);
            EntityStream entityStream = new AtlasEntityStream(policiesExtInfo);
            entityStore.createOrUpdate(entityStream, false); // adding new policy
        } finally {
            RequestContext.get().setSkipAuthorizationCheck(false);
        }
    }

    private void updateDaapVisibilityPolicy(AtlasEntity newEntity, AtlasEntity currentEntity,  String currentProductDaapVisibility, String newProductDaapVisibility) throws AtlasBaseException{

        AtlasEntity policy = new AtlasEntity();
        policy.setTypeName(POLICY_ENTITY_TYPE);
        policy.setAttribute(NAME,newEntity.getAttribute(NAME));
        policy.setAttribute(QUALIFIED_NAME,currentEntity.getGuid()+"/read-policy");

        switch (currentProductDaapVisibility) {
            case PRIVATE:
                switch (newProductDaapVisibility) {
                    case PROTECTED:
                        // create policy for policyUsers and policyGroups
                        policy.setAttribute(ATTR_POLICY_USERS, newEntity.getAttribute(DAAP_VISIBILITY_USERS));
                        policy.setAttribute(ATTR_POLICY_GROUPS, newEntity.getAttribute(DAAP_VISIBILITY_GROUPS));
                        break;
                    case PUBLIC:
                        // set empty user list
                        policy.setAttribute(ATTR_POLICY_USERS, Arrays.asList());
                        // set user groups to public to allow access for all
                        policy.setAttribute(ATTR_POLICY_GROUPS, Arrays.asList("public"));
                }
                break;
            case PROTECTED:
                switch (newProductDaapVisibility) {
                    case PRIVATE:
                        // create policy for policyUsers and policyGroups
                        policy.setAttribute(ATTR_POLICY_USERS,Arrays.asList());
                        policy.setAttribute(ATTR_POLICY_GROUPS,Arrays.asList());
                        break;
                    case PUBLIC:
                        // set empty user list
                        policy.setAttribute(ATTR_POLICY_USERS, Arrays.asList());
                        // set user groups to public to allow access for all
                        policy.setAttribute(ATTR_POLICY_GROUPS, Arrays.asList("public"));
                }
                break;
            case PUBLIC:
                switch (newProductDaapVisibility) {
                    case PRIVATE:
                        // create policy for policyUsers and policyGroups
                        policy.setAttribute(ATTR_POLICY_USERS,Arrays.asList());
                        policy.setAttribute(ATTR_POLICY_GROUPS,Arrays.asList());
                        break;
                    case PROTECTED:
                        // create policy for policyUsers and policyGroups
                        policy.setAttribute(ATTR_POLICY_USERS, newEntity.getAttribute(DAAP_VISIBILITY_USERS));
                        policy.setAttribute(ATTR_POLICY_GROUPS, newEntity.getAttribute(DAAP_VISIBILITY_GROUPS));
                        break;
                }
                break;
        }

        try {
            RequestContext.get().setSkipAuthorizationCheck(true);
            AtlasEntity.AtlasEntitiesWithExtInfo policiesExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
            policiesExtInfo.addEntity(policy);
            EntityStream entityStream = new AtlasEntityStream(policiesExtInfo);
            entityStore.createOrUpdate(entityStream, false); // adding new policy
        } finally {
            RequestContext.get().setSkipAuthorizationCheck(false);
        }
    }
}
