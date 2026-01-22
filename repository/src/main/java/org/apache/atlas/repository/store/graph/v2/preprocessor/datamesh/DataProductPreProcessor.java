package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.cassandra.DynamicVertexService;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils;
import org.apache.atlas.repository.util.AtlasEntityUtils;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AccessControlUtils.*;
import static org.apache.atlas.v1.model.instance.Id.EntityState.ACTIVE;
import static org.apache.atlas.v1.model.instance.Id.EntityState.DELETED;

public class DataProductPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DataProductPreProcessor.class);
    private static final String PRIVATE = "Private";
    private static final String PROTECTED = "Protected";
    private static final String PUBLIC = "Public";
    private static final String DATA_PRODUCT = "dataProduct";

    private EntityMutationContext context;
    private AtlasEntityStore entityStore;
    private Map<String, String> updatedPolicyResources;
    private EntityGraphRetriever retrieverNoRelation = null;

    public DataProductPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                                   AtlasGraph graph, AtlasEntityStore entityStore,
                                   DynamicVertexService dynamicVertexService) {
        super(typeRegistry, entityRetriever, graph, dynamicVertexService);
        this.updatedPolicyResources = new HashMap<>();
        this.entityStore = entityStore;
        this.retrieverNoRelation = new EntityGraphRetriever(entityRetriever, true);
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
                processCreateProduct(entity, vertex);
                break;
            case UPDATE:
                processUpdateProduct(entity, vertex);
                break;
        }
    }

    private void processCreateProduct(AtlasEntity entity,AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateProduct");
        AtlasObjectId parentDomainObject = (AtlasObjectId) entity.getRelationshipAttribute(DATA_DOMAIN_REL_TYPE);
        String productName = (String) entity.getAttribute(NAME);
        String parentDomainQualifiedName = "";

        entity.removeAttribute(OUTPUT_PORT_GUIDS_ATTR);
        entity.removeAttribute(INPUT_PORT_GUIDS_ATTR);

        validateProductAssetDSLAttr(entity);

        if (parentDomainObject == null) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Cannot create a Product without a Domain Relationship");
        } else {
            AtlasVertex parentDomain = retrieverNoRelation.getEntityVertex(parentDomainObject);
            parentDomainQualifiedName = parentDomain.getProperty(QUALIFIED_NAME, String.class);


            entity.setAttribute(PARENT_DOMAIN_QN_ATTR, parentDomainQualifiedName);

            String superDomainQualifiedName = parentDomain.getProperty(SUPER_DOMAIN_QN_ATTR, String.class);
            if(StringUtils.isEmpty(superDomainQualifiedName)) {
                superDomainQualifiedName = parentDomainQualifiedName;
            }
            entity.setAttribute(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);
        }

        entity.setAttribute(DAAP_LINEAGE_STATUS_ATTR, DAAP_LINEAGE_STATUS_PENDING);

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(parentDomainQualifiedName));

        productExists(productName, parentDomainQualifiedName, null);

        createDaapVisibilityPolicy(entity, vertex);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateProduct(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateProduct");

        String state = vertex.getProperty(STATE_PROPERTY_KEY, String.class);

        if (DELETED.name().equals(state)) {
            //  To allow product restoration and update on daapLineageStatus but block all other updates if the product is archived
            boolean isBeingRestored = false;

            if (context != null && context.getEntitiesToRestore() != null) {
                isBeingRestored = context.getEntitiesToRestore().contains(vertex);
            }

            if (!isBeingRestored && !entity.hasAttribute(DAAP_LINEAGE_STATUS_ATTR)) {
                throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Cannot update DataProduct that is Archived!");
            }
        }

        entity.removeAttribute(OUTPUT_PORT_GUIDS_ATTR);
        entity.removeAttribute(INPUT_PORT_GUIDS_ATTR);

        if (entity.hasAttribute(DAAP_ASSET_DSL_ATTR)) {
            validateProductAssetDSLAttr(entity);
        }

        if(entity.hasRelationshipAttribute(DATA_DOMAIN_REL_TYPE) && entity.getRelationshipAttribute(DATA_DOMAIN_REL_TYPE) == null){
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "DataProduct can only be moved to another Domain.");
        }

        AtlasEntity diffEntity = RequestContext.get().getDifferentialEntity(entity.getGuid());

        if(entity.getAttribute(DAAP_LINEAGE_STATUS_ATTR) != null && entity.getAttribute(DAAP_LINEAGE_STATUS_ATTR).equals(DAAP_LINEAGE_STATUS_COMPLETED)){
            if (!ARGO_SERVICE_USER_NAME.equals(RequestContext.getCurrentUser())) {
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Can not update Lineage Status to Completed");
            }
            entity.setAttribute(DAAP_LINEAGE_STATUS_ATTR, DAAP_LINEAGE_STATUS_COMPLETED);
        }

        if(entity.hasAttribute(DAAP_ASSET_DSL_ATTR) && diffEntity.hasAttribute(DAAP_ASSET_DSL_ATTR)){
            entity.setAttribute(DAAP_LINEAGE_STATUS_ATTR, DAAP_LINEAGE_STATUS_PENDING);
            diffEntity.setAttribute(DAAP_LINEAGE_STATUS_ATTR, DAAP_LINEAGE_STATUS_PENDING);
        }

        if(entity.hasAttribute(STATE_PROPERTY_KEY) && !state.equals(entity.getAttribute(STATE_PROPERTY_KEY))){
            entity.setAttribute(DAAP_LINEAGE_STATUS_ATTR, DAAP_LINEAGE_STATUS_PENDING);
            diffEntity.setAttribute(DAAP_LINEAGE_STATUS_ATTR, DAAP_LINEAGE_STATUS_PENDING);
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

        boolean isDaapVisibilityChanged = isDaapVisibilityChanged(storedProduct, entity);

        if (newParentDomainQualifiedName != null && !newParentDomainQualifiedName.equals(currentParentDomainQualifiedName)) {

            if(isDaapVisibilityChanged){
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Moving the product to another domain along with the change in Daap visibility is not allowed");
            }

            //Auth check
            isAuthorizedToMove(DATA_PRODUCT_ENTITY_TYPE, currentParentDomainHeader, newParentDomainHeader);

            String newSuperDomainQualifiedName = (String) newParentDomainHeader.getAttribute(SUPER_DOMAIN_QN_ATTR);
            if(StringUtils.isEmpty(newSuperDomainQualifiedName)){
                newSuperDomainQualifiedName = newParentDomainQualifiedName;
            }

            processMoveDataProductToAnotherDomain(entity, vertex, currentParentDomainQualifiedName, newParentDomainQualifiedName, vertexQnName, newSuperDomainQualifiedName);

            updatePolicies(this.updatedPolicyResources, this.context);

        } else {
            entity.removeAttribute(PARENT_DOMAIN_QN_ATTR);
            entity.removeAttribute(SUPER_DOMAIN_QN_ATTR);
            String productCurrentName = vertex.getProperty(NAME, String.class);
            String productNewName = (String) entity.getAttribute(NAME);

            if (!productCurrentName.equals(productNewName)) {
                productExists(productNewName, currentParentDomainQualifiedName, storedProduct.getGuid());
            }
            entity.setAttribute(QUALIFIED_NAME, vertexQnName);
        }

        if (isDaapVisibilityChanged) {
            updateDaapVisibilityPolicy(entity, storedProduct);
        }
        else{
            // if isDaapVisibilityChanged is false, then do not update any daap visibility attributes in product entity as well
            entity.removeAttribute(DAAP_VISIBILITY_USERS_ATTR);
            entity.removeAttribute(DAAP_VISIBILITY_GROUPS_ATTR);
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processMoveDataProductToAnotherDomain(AtlasEntity product,
                                                       AtlasVertex productVertex,
                                                       String sourceDomainQualifiedName,
                                                       String targetDomainQualifiedName,
                                                       String currentDataProductQualifiedName,
                                                       String superDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processMoveDataProductToAnotherDomain");

        try {
            String productName = (String) product.getAttribute(NAME);
            LinkedHashMap<String, Object> updatedAttributes = new LinkedHashMap<>();

            LOG.info("Moving dataProduct {} to Domain {}", productName, targetDomainQualifiedName);

            productExists(productName, targetDomainQualifiedName, product.getGuid());

            String updatedQualifiedName;
            if(StringUtils.isEmpty(sourceDomainQualifiedName)){
                updatedQualifiedName = createQualifiedName(targetDomainQualifiedName);
            } else {
                updatedQualifiedName = currentDataProductQualifiedName.replace(sourceDomainQualifiedName, targetDomainQualifiedName);
            }

            product.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
            product.setAttribute(PARENT_DOMAIN_QN_ATTR, targetDomainQualifiedName);
            product.setAttribute(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);

            updatedAttributes.put(QUALIFIED_NAME, updatedQualifiedName);
            updatedAttributes.put(PARENT_DOMAIN_QN_ATTR, targetDomainQualifiedName);
            updatedAttributes.put(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);

            Iterator<AtlasEdge> existingParentEdges = productVertex.getEdges(AtlasEdgeDirection.IN, DATA_PRODUCT_EDGE_LABEL).iterator();
            if (existingParentEdges.hasNext()) {
                graph.removeEdge(existingParentEdges.next());
            }

            //Store domainPolicies and resources to be updated
            String currentResource = "entity:"+ currentDataProductQualifiedName;
            String updatedResource = "entity:"+ updatedQualifiedName;
            this.updatedPolicyResources.put(currentResource, updatedResource);

            for (Map.Entry<String, Object> entry : updatedAttributes.entrySet()) {
                RequestContext.get().getDifferentialEntitiesMap()
                        .get(product.getGuid()).setAttribute(entry.getKey(), entry.getValue());
            }

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

    private void productExists(String productName, String parentDomainQualifiedName, String guid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("productExists");

        try {
            exists(DATA_PRODUCT_ENTITY_TYPE, productName, parentDomainQualifiedName, guid);

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

    private AtlasEntity getPolicyEntity(AtlasEntity entity, String productGuid ) {
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
        policy.setAttribute(ATTR_POLICY_SUB_CATEGORY, DATA_PRODUCT); // create new constant attr

        return policy;
    }

    private void createDaapVisibilityPolicy(AtlasEntity entity,AtlasVertex vertex) throws AtlasBaseException {
        String productGuid = vertex.getProperty("__guid", String.class);
        String vis =  AtlasEntityUtils.getStringAttribute(entity,DAAP_VISIBILITY_ATTR);

        if (vis != null && !vis.equals(PRIVATE)){
            AtlasEntity policy = getPolicyEntity(entity, productGuid);

            switch (vis) {
                case PROTECTED:
                    setProtectedPolicyAttributes(policy, entity);
                    break;
                case PUBLIC:
                    setPublicPolicyAttributes(policy);
                    break;
            }
            createPolicy(policy);
        }
    }

    private void updateDaapVisibilityPolicy(AtlasEntity newEntity, AtlasEntity currentEntity) throws AtlasBaseException{
        String newProductDaapVisibility = AtlasEntityUtils.getStringAttribute(newEntity,DAAP_VISIBILITY_ATTR);// check case if attribute is not sent from FE
        AtlasObjectId atlasObjectId = new AtlasObjectId();
        atlasObjectId.setTypeName(POLICY_ENTITY_TYPE);
        atlasObjectId.setUniqueAttributes(AtlasEntityUtils.mapOf(QUALIFIED_NAME,currentEntity.getGuid()+"/read-policy"));
        AtlasVertex policyVertex = null;
        try {
            policyVertex = entityRetriever.getEntityVertex(atlasObjectId);
        }
        catch(AtlasBaseException exp){
            if(!exp.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)){
                throw exp;
            }
        }

        AtlasEntity policy;
        if (policyVertex == null) {
            policy = getPolicyEntity(newEntity, newEntity.getGuid());
        } else {
            policy = entityRetriever.toAtlasEntity(policyVertex);
        }

        Map<String, Object> updatedAttributes = new HashMap<>();

        if (newProductDaapVisibility.equals(PRIVATE)) {
            updatedAttributes = setPrivatePolicyAttributes(policy);
        }
        else if (newProductDaapVisibility.equals(PROTECTED)) {
            updatedAttributes = setProtectedPolicyAttributes(policy,
                   newEntity
            );
        }
        else if (newProductDaapVisibility.equals(PUBLIC)) {
            updatedAttributes = setPublicPolicyAttributes(policy);
        }

        if (policyVertex == null) {
            createPolicy(policy);
        } else {
            updatePolicy(policy, policyVertex, updatedAttributes);
        }
    }

    private void createPolicy(AtlasEntity policy) throws AtlasBaseException{
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

    private void updatePolicy(AtlasEntity policy, AtlasVertex policyVertex,Map<String, Object> updatedAttributes) {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(POLICY_ENTITY_TYPE);
        context.addUpdated(policy.getGuid(), policy, entityType, policyVertex);
        recordUpdatedChildEntities(policyVertex, updatedAttributes);
    }

    private Map<String, Object> setPrivatePolicyAttributes(AtlasEntity policy) {
        Map<String, Object> updatedAttributes = new HashMap<>();
        policy.setAttribute(ATTR_POLICY_USERS, Arrays.asList());
        policy.setAttribute(ATTR_POLICY_GROUPS, Arrays.asList());
        policy.setAttribute(ATTR_POLICY_IS_ENABLED, false);

        updatedAttributes.put(ATTR_POLICY_USERS, Arrays.asList());
        updatedAttributes.put(ATTR_POLICY_GROUPS, Arrays.asList());
        updatedAttributes.put(ATTR_POLICY_IS_ENABLED, false);

        return updatedAttributes;
    }

    private Map<String, Object> setProtectedPolicyAttributes(AtlasEntity policy, AtlasEntity entity) {
        List<String> users = AtlasEntityUtils.getListAttribute(entity, DAAP_VISIBILITY_USERS_ATTR);
        List<String> groups = AtlasEntityUtils.getListAttribute(entity, DAAP_VISIBILITY_GROUPS_ATTR);

        policy.setAttribute(ATTR_POLICY_USERS, users);
        policy.setAttribute(ATTR_POLICY_GROUPS, groups);
        policy.setAttribute(ATTR_POLICY_IS_ENABLED, true);

        Map<String, Object> updatedAttributes = new HashMap<>();
        updatedAttributes.put(ATTR_POLICY_USERS, users);
        updatedAttributes.put(ATTR_POLICY_GROUPS, groups);
        updatedAttributes.put(ATTR_POLICY_IS_ENABLED, true);
        return updatedAttributes;
    }

    private Map<String, Object> setPublicPolicyAttributes(AtlasEntity policy) {
        Map<String, Object> updatedAttributes = new HashMap<>();
        policy.setAttribute(ATTR_POLICY_USERS, Arrays.asList());
        policy.setAttribute(ATTR_POLICY_GROUPS, Arrays.asList("public"));
        policy.setAttribute(ATTR_POLICY_IS_ENABLED, true);

        updatedAttributes.put(ATTR_POLICY_USERS, Arrays.asList());
        updatedAttributes.put(ATTR_POLICY_GROUPS, Arrays.asList("public"));
        updatedAttributes.put(ATTR_POLICY_IS_ENABLED, true);
        return updatedAttributes;
    }

    private Boolean isDaapVisibilityChanged(AtlasEntity storedProduct, AtlasEntity newProduct){

        boolean isDaapVisibilityChanged;
        // check for daapVisibility change
        String currentProductDaapVisibility = AtlasEntityUtils.getStringAttribute(storedProduct, DAAP_VISIBILITY_ATTR);
        String newProductDaapVisibility = AtlasEntityUtils.getStringAttribute(newProduct, DAAP_VISIBILITY_ATTR); // check case if attribute is not sent from FE

        if(newProductDaapVisibility == null){
            return false;
        }

        isDaapVisibilityChanged = (!newProductDaapVisibility.equals(currentProductDaapVisibility));
        if(isDaapVisibilityChanged){
            return true;
        }

        // check if new daap visibility and old daap visibility is protected then check if any user, groups added changed
        if (newProductDaapVisibility.equals(PROTECTED) && currentProductDaapVisibility.equals(PROTECTED)){

            List<String> storedUsers = AtlasEntityUtils.getListAttribute(storedProduct, DAAP_VISIBILITY_USERS_ATTR);
            List<String> storedGroups = AtlasEntityUtils.getListAttribute(storedProduct, DAAP_VISIBILITY_GROUPS_ATTR);
            List<String> newUsers = AtlasEntityUtils.getListAttribute(newProduct, DAAP_VISIBILITY_USERS_ATTR);
            List<String> newGroups = AtlasEntityUtils.getListAttribute(newProduct, DAAP_VISIBILITY_GROUPS_ATTR);

            isDaapVisibilityChanged = compareLists(storedUsers, newUsers) || compareLists(storedGroups, newGroups);
        }

        return isDaapVisibilityChanged;
    }

    private void validateProductAssetDSLAttr(AtlasEntity entity) throws AtlasBaseException {
        if (entity.getAttribute(DAAP_ASSET_DSL_ATTR) == null) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "dataProductAssetsDSL attribute is mandatory for DataProducts");
        }

        String dslString = ((String) entity.getAttribute(DAAP_ASSET_DSL_ATTR)).trim();

        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> dslMap = mapper.readValue(dslString, new TypeReference<>() {});

            if (dslMap == null || dslMap.isEmpty()) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "dataProductAssetsDSL attribute cannot be empty");
            }
        } catch (JsonProcessingException e) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "dataProductAssetsDSL attribute must be a valid JSON object: " + e.getMessage());
        }
    }

    public static boolean compareLists(List<String> list1, List<String> list2) {
        return !CollectionUtils.disjunction(list1, list2).isEmpty();
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processProductDelete");

        try{
            if(RequestContext.get().getDeleteType() != DeleteType.SOFT){
                String productGuid = vertex.getProperty("__guid", String.class);
                AtlasObjectId atlasObjectId = new AtlasObjectId();
                atlasObjectId.setTypeName(POLICY_ENTITY_TYPE);
                atlasObjectId.setUniqueAttributes(AtlasEntityUtils.mapOf(QUALIFIED_NAME, productGuid+"/read-policy"));
                AtlasVertex policyVertex;
                try {
                    policyVertex = entityRetriever.getEntityVertex(atlasObjectId);
                    entityStore.deleteById(policyVertex.getProperty("__guid", String.class));
                }
                catch(AtlasBaseException exp){
                    if(!exp.getAtlasErrorCode().equals(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND)){
                        throw exp;
                    }
                }

                if (hasLinkedAssets(productGuid, PRODUCT_GUIDS)) {
                    throw new AtlasBaseException(AtlasErrorCode.OPERATION_NOT_SUPPORTED, "This product can't be deleted right now because it has linked assets that are in the process of being removed. Please try again shortly.");
                }
            }
            if(RequestContext.get().getDeleteType() == DeleteType.SOFT || RequestContext.get().getDeleteType() == DeleteType.DEFAULT){
                vertex.setProperty(DAAP_STATUS_ATTR, DAAP_ARCHIVED_STATUS);
                vertex.setProperty(DAAP_LINEAGE_STATUS_ATTR, DAAP_LINEAGE_STATUS_PENDING);
            }
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

    }

    /**
     * Validates that for archived DataProducts, only business metadata removals (null values) are allowed.
     * Any additions or updates (non-null values) in the updatedBusinessAttributes are blocked.
     */
    public static void validateBusinessMetadataUpdateOnArchivedProduct(AtlasVertex entityVertex, Map<String, Map<String, Object>> updatedBusinessAttributes) throws AtlasBaseException {
        if (entityVertex == null || MapUtils.isEmpty(updatedBusinessAttributes)) {
            return;
        }

        if (!DATA_PRODUCT_ENTITY_TYPE.equals(entityVertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class))) {
            return;
        }

        String entityState = entityVertex.getProperty(STATE_PROPERTY_KEY, String.class);

        if (!AtlasEntity.Status.DELETED.name().equals(entityState)) {
            return;
        }

        for (Map.Entry<String, Map<String, Object>> bmEntry : updatedBusinessAttributes.entrySet()) {
            Map<String, Object> bmAttributes = bmEntry.getValue();

            if (MapUtils.isNotEmpty(bmAttributes)) {
                boolean hasNonNullValue = bmAttributes.values().stream().anyMatch(Objects::nonNull);

                if (hasNonNullValue) {
                    throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Cannot add or update custom metadata on archived DataProduct.");
                }
            }
        }
    }
}
