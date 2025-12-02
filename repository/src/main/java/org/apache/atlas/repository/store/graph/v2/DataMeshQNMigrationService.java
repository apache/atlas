package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.NanoIdUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.graph.GraphHelper.getAllChildrenVertices;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES;

public class DataMeshQNMigrationService implements MigrationService {

    private static final Logger LOG = LoggerFactory.getLogger(DataMeshQNMigrationService.class);

    private final AtlasEntityStore entityStore;
    private final EntityDiscoveryService discovery;
    private final EntityGraphRetriever entityRetriever;

    private final AtlasTypeRegistry typeRegistry;
    private final RedisService redisService;
    private Map<String, String> updatedPolicyResources;

    private final int BATCH_SIZE = 20;

    boolean errorOccured = false;

    boolean skipSuperDomain = false;

    private int counter;
    private boolean forceRegen;
    private final TransactionInterceptHelper   transactionInterceptHelper;

    public DataMeshQNMigrationService(AtlasEntityStore entityStore, EntityDiscoveryService discovery, EntityGraphRetriever entityRetriever, AtlasTypeRegistry typeRegistry, TransactionInterceptHelper transactionInterceptHelper, RedisService redisService, boolean forceRegen) {
        this.entityRetriever = entityRetriever;
        this.entityStore = entityStore;
        this.discovery = discovery;
        this.typeRegistry = typeRegistry;
        this.redisService = redisService;
        this.transactionInterceptHelper = transactionInterceptHelper;
        this.forceRegen = forceRegen;

        this.updatedPolicyResources = new HashMap<>();
        this.counter = 0;
    }

    public void startMigration() throws Exception {
        try {
            redisService.putValue(DATA_MESH_QN, MigrationStatus.IN_PROGRESS.name());

            Set<String> attributes = new HashSet<>(Arrays.asList(SUPER_DOMAIN_QN_ATTR, PARENT_DOMAIN_QN_ATTR, "__customAttributes"));

            List<AtlasEntityHeader> entities = getEntity(DATA_DOMAIN_ENTITY_TYPE, attributes, null);

            for (AtlasEntityHeader superDomain: entities) {
                skipSuperDomain = false;
                updateChunk(superDomain);
            }
        } catch (Exception e) {
            LOG.error("Migration failed", e);
            redisService.putValue(DATA_MESH_QN, MigrationStatus.FAILED.name());
            throw e;
        }

        redisService.putValue(DATA_MESH_QN, MigrationStatus.SUCCESSFUL.name());
    }

    private void updateChunk(AtlasEntityHeader atlasEntity) throws AtlasBaseException {
        AtlasVertex vertex = entityRetriever.getEntityVertex(atlasEntity.getGuid());
        String qualifiedName = (String) atlasEntity.getAttribute(QUALIFIED_NAME);

        try{
            migrateDomainAttributes(vertex, "", "");

            if (counter > 0) {
                commitChanges();
            }

        } catch (AtlasBaseException e){
            this.errorOccured = true;
            LOG.error("Error while migrating qualified name for entity: {}", qualifiedName, e);
        }
    }

    private void migrateDomainAttributes(AtlasVertex vertex, String parentDomainQualifiedName, String superDomainQualifiedName) throws AtlasBaseException {
        if(skipSuperDomain) {
            return;
        }

        String currentQualifiedName = vertex.getProperty(QUALIFIED_NAME,String.class);
        String updatedQualifiedName = createDomainQualifiedName(parentDomainQualifiedName);

        Map<String, Object> updatedAttributes = new HashMap<>();

        Map<String,String> customAttributes = GraphHelper.getCustomAttributes(vertex);
        if(!this.forceRegen && customAttributes != null && customAttributes.get(MIGRATION_CUSTOM_ATTRIBUTE) != null && customAttributes.get(MIGRATION_CUSTOM_ATTRIBUTE).equals("true")){
            LOG.info("Entity already migrated: {}", currentQualifiedName);

            updatedQualifiedName = vertex.getProperty(QUALIFIED_NAME,String.class);

            if (StringUtils.isEmpty(superDomainQualifiedName)) {
                superDomainQualifiedName = vertex.getProperty(QUALIFIED_NAME,String.class);
            }

        } else {
            counter++;
            LOG.info("Migrating qualified name for Domain: {} to {}", currentQualifiedName, updatedQualifiedName);
            superDomainQualifiedName = commitChangesInMemory(currentQualifiedName, updatedQualifiedName, parentDomainQualifiedName, superDomainQualifiedName, vertex, updatedAttributes);
        }

        if (!skipSuperDomain) {
            Iterator<AtlasVertex> products = getAllChildrenVertices(vertex, DATA_PRODUCT_EDGE_LABEL);
            List<AtlasVertex> productsList = new ArrayList<>();
            products.forEachRemaining(productsList::add);

            for (AtlasVertex productVertex : productsList) {
                if (Objects.nonNull(productVertex)) {
                    migrateDataProductAttributes(productVertex, updatedQualifiedName, superDomainQualifiedName);
                } else {
                    LOG.warn("Found null product vertex");
                }

                if (skipSuperDomain) {
                    break;
                }
            }

            // Get all children domains of current domain
            Iterator<AtlasVertex> childDomains = getAllChildrenVertices(vertex, DOMAIN_PARENT_EDGE_LABEL);
            List<AtlasVertex> childDomainsList = new ArrayList<>();
            childDomains.forEachRemaining(childDomainsList::add);

            for (AtlasVertex childVertex : childDomainsList) {
                if (Objects.nonNull(childVertex)) {
                    migrateDomainAttributes(childVertex, updatedQualifiedName, superDomainQualifiedName);
                } else {
                    LOG.warn("Found null sub-domain vertex");
                }

                if (skipSuperDomain) {
                    break;
                }
            }

            recordUpdatedChildEntities(vertex, updatedAttributes);
            if (counter >= BATCH_SIZE) {
                commitChanges();
            }
        }
    }

    public void commitChanges() throws AtlasBaseException {
        try {
            updatePolicy(this.updatedPolicyResources);
        } catch (AtlasBaseException e) {
            this.errorOccured = true;
            this.skipSuperDomain = true;
            LOG.error("Failed to update set of policies: ", e);
            LOG.error("Failed policies: {}", AtlasType.toJson(this.updatedPolicyResources));
            throw e;
        } finally {
            this.updatedPolicyResources.clear();
        }

        try {
            transactionInterceptHelper.intercept();
            LOG.info("Committed a batch to the graph");
        } catch (Exception e){
            this.skipSuperDomain = true;
            this.errorOccured = true;
            LOG.error("Failed to commit set of assets: ", e);
            throw e;
        } finally {
            this.counter = 0;
        }
    }

    public String commitChangesInMemory(String currentQualifiedName, String updatedQualifiedName, String parentDomainQualifiedName, String superDomainQualifiedName, AtlasVertex vertex, Map<String, Object> updatedAttributes) {

        if(skipSuperDomain) {
            return "";
        }

        vertex.setProperty(QUALIFIED_NAME, updatedQualifiedName);

        if (StringUtils.isEmpty(parentDomainQualifiedName) && StringUtils.isEmpty(superDomainQualifiedName)){
            superDomainQualifiedName = updatedQualifiedName;
        } else{
            vertex.setProperty(PARENT_DOMAIN_QN_ATTR, parentDomainQualifiedName);
            vertex.setProperty(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);
        }

        updatedAttributes.put(QUALIFIED_NAME, updatedQualifiedName);

        //Store domainPolicies and resources to be updated
        String currentResource = "entity:"+ currentQualifiedName;
        String updatedResource = "entity:"+ updatedQualifiedName;
        this.updatedPolicyResources.put(currentResource, updatedResource);

        Map<String,String> customAttributes = GraphHelper.getCustomAttributes(vertex);
        if(Objects.isNull(customAttributes) || MapUtils.isEmpty(customAttributes)) {
            customAttributes = new HashMap<>();
        }
        customAttributes.put(MIGRATION_CUSTOM_ATTRIBUTE, "true");
        vertex.setProperty(CUSTOM_ATTRIBUTES_PROPERTY_KEY, AtlasEntityType.toJson(customAttributes));

        return superDomainQualifiedName;
    }


    private void migrateDataProductAttributes(AtlasVertex vertex, String parentDomainQualifiedName, String superDomainQualifiedName) throws AtlasBaseException {
        if(skipSuperDomain) {
            return;
        }

        String currentQualifiedName = vertex.getProperty(QUALIFIED_NAME,String.class);
        String updatedQualifiedName = createProductQualifiedName(parentDomainQualifiedName);

        Map<String,String> customAttributes = GraphHelper.getCustomAttributes(vertex);

        if(!this.forceRegen && customAttributes != null && customAttributes.get(MIGRATION_CUSTOM_ATTRIBUTE) != null && customAttributes.get(MIGRATION_CUSTOM_ATTRIBUTE).equals("true")) {
            LOG.info("Product already migrated: {}", currentQualifiedName);

        } else {
            counter++;
            LOG.info("Migrating qualified name for Product: {} to {}", currentQualifiedName, updatedQualifiedName);
            vertex.setProperty(QUALIFIED_NAME, updatedQualifiedName);

            //Store domainPolicies and resources to be updated
            String currentResource = "entity:" + currentQualifiedName;
            String updatedResource = "entity:" + updatedQualifiedName;
            this.updatedPolicyResources.put(currentResource, updatedResource);

            vertex.setProperty(PARENT_DOMAIN_QN_ATTR, parentDomainQualifiedName);
            vertex.setProperty(SUPER_DOMAIN_QN_ATTR, superDomainQualifiedName);

            if(Objects.isNull(customAttributes) || MapUtils.isEmpty(customAttributes)) {
                customAttributes = new HashMap<>();
            }
            customAttributes.put(MIGRATION_CUSTOM_ATTRIBUTE, "true");
            vertex.setProperty(CUSTOM_ATTRIBUTES_PROPERTY_KEY, AtlasEntityType.toJson(customAttributes));
        }

        if(counter >= BATCH_SIZE){
            commitChanges();
        }
    }

    protected void updatePolicy(Map<String, String> updatedPolicyResources) throws AtlasBaseException {
        if(skipSuperDomain) {
            return;
        }

        List<String> currentResources = new ArrayList<>(updatedPolicyResources.keySet());
        LOG.info("Updating policies for entities {}", currentResources);
        Map<String, Object> updatedAttributes = new HashMap<>();

        List<AtlasEntityHeader> policies = getEntity(POLICY_ENTITY_TYPE,new HashSet<>(Arrays.asList(ATTR_POLICY_RESOURCES, ATTR_POLICY_CATEGORY)), currentResources);
        if (CollectionUtils.isNotEmpty(policies)) {
            int batchSize = BATCH_SIZE;
            int totalPolicies = policies.size();

            for (int i = 0; i < totalPolicies; i += batchSize) {
                List<AtlasEntity> entityList = new ArrayList<>();
                List<AtlasEntityHeader> batch = policies.subList(i, Math.min(i + batchSize, totalPolicies));

                for (AtlasEntityHeader policy : batch) {
                    AtlasVertex policyVertex = entityRetriever.getEntityVertex(policy.getGuid());
                    AtlasEntity policyEntity = entityRetriever.toAtlasEntity(policyVertex);

                    List<String> policyResources = (List<String>) policyEntity.getAttribute(ATTR_POLICY_RESOURCES);
                    List<String> updatedPolicyResourcesList = new ArrayList<>();

                    for (String resource : policyResources) {
                        if (updatedPolicyResources.containsKey(resource)) {
                            updatedPolicyResourcesList.add(updatedPolicyResources.get(resource));
                        } else {
                            updatedPolicyResourcesList.add(resource);
                        }
                    }
                    updatedAttributes.put(ATTR_POLICY_RESOURCES, updatedPolicyResourcesList);

                    policyEntity.setAttribute(ATTR_POLICY_RESOURCES, updatedPolicyResourcesList);
                    entityList.add(policyEntity);
                    recordUpdatedChildEntities(policyVertex, updatedAttributes);
                }

                EntityStream entityStream = new AtlasEntityStream(entityList);
                entityStore.createOrUpdate(entityStream, false);
            }
        }
    }

    private static String createDomainQualifiedName(String parentDomainQualifiedName) {
        if (StringUtils.isNotEmpty(parentDomainQualifiedName)) {
            return parentDomainQualifiedName + "/domain/" + getUUID();
        } else{
            return "default/domain" + "/" + getUUID() + "/super";
        }
    }

    private static String createProductQualifiedName(String parentDomainQualifiedName) throws AtlasBaseException {
        if (StringUtils.isEmpty(parentDomainQualifiedName)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Parent Domain Qualified Name cannot be empty or null");
        }
        return parentDomainQualifiedName + "/product/" + getUUID();
    }

    public static String getUUID(){
        return NanoIdUtils.randomNanoId();
    }

    public List<AtlasEntityHeader> getEntity(String entityType, Set<String> attributes, List<String> resource) throws AtlasBaseException {

        List<Map<String, Object>> mustClauseList = new ArrayList<>();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", entityType)));

        if(entityType.equals(DATA_DOMAIN_ENTITY_TYPE)){
            Map<String, Object> childBool = new HashMap<>();
            List <Map<String, Object>> mustNotClauseList = new ArrayList<>();
            mustNotClauseList.add(mapOf("exists", mapOf("field", PARENT_DOMAIN_QN_ATTR)));

            Map<String, Object> shouldBool = new HashMap<>();
            shouldBool.put("must_not", mustNotClauseList);

            List <Map<String, Object>> shouldClauseList = new ArrayList<>();
            shouldClauseList.add(mapOf("bool", shouldBool));

            childBool.put("should", shouldClauseList);
            mustClauseList.add(mapOf("bool", childBool));
        }

        if(entityType.equals(POLICY_ENTITY_TYPE)){
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("terms", mapOf("policyResources", resource)));
        }

        Map<String, Object> bool = new HashMap<>();
        bool.put("must", mustClauseList);

        Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

        List<Map<String, Object>> sortList = new ArrayList<>();
        Map<String, Object> sortField = new HashMap<>();
        sortField.put("__timestamp", mapOf("order", "DESC"));
        sortList.add(sortField);
        dsl.put("sort", sortList);


        List<AtlasEntityHeader> entities = indexSearchPaginated(dsl, attributes, discovery);

        return entities;
    }

    public static List<AtlasEntityHeader> indexSearchPaginated(Map<String, Object> dsl, Set<String> attributes, EntityDiscoveryService discovery) throws AtlasBaseException {
        IndexSearchParams searchParams = new IndexSearchParams();
        List<AtlasEntityHeader> ret = new ArrayList<>();

        List<Map> sortList = new ArrayList<>(0);
        sortList.add(mapOf("__timestamp", mapOf("order", "asc")));
        sortList.add(mapOf("__guid", mapOf("order", "asc")));
        dsl.put("sort", sortList);

        int from = 0;
        int size = 100;
        boolean hasMore = true;
        do {
            dsl.put("from", from);
            dsl.put("size", size);
            searchParams.setDsl(dsl);

            if (CollectionUtils.isNotEmpty(attributes)) {
                searchParams.setAttributes(attributes);
            }

            List<AtlasEntityHeader> headers = discovery.directIndexSearch(searchParams).getEntities();

            if (CollectionUtils.isNotEmpty(headers)) {
                ret.addAll(headers);
            } else {
                hasMore = false;
            }

            from += size;

        } while (hasMore);

        return ret;
    }

    /**
     * Record the updated child entities, it will be used to send notification and store audit logs
     * @param entityVertex Child entity vertex
     * @param updatedAttributes Updated attributes while updating required attributes on updating collection
     */
    protected void recordUpdatedChildEntities(AtlasVertex entityVertex, Map<String, Object> updatedAttributes) {
        RequestContext requestContext = RequestContext.get();

        AtlasEntity entity = new AtlasEntity();
        entity = entityRetriever.mapSystemAttributes(entityVertex, entity);
        entity.setAttributes(updatedAttributes);
        requestContext.cacheDifferentialEntity(new AtlasEntity(entity), entityVertex);

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

        //Add the min info attributes to entity header to be sent as part of notification
        if(entityType != null) {
            AtlasEntity finalEntity = entity;
            entityType.getMinInfoAttributes().values().stream().filter(attribute -> !updatedAttributes.containsKey(attribute.getName())).forEach(attribute -> {
                Object attrValue = null;
                try {
                    attrValue = entityRetriever.getVertexAttribute(entityVertex, attribute);
                } catch (AtlasBaseException e) {
                    this.errorOccured = true;
                    LOG.error("Error while getting vertex attribute", e);
                }
                if(attrValue != null) {
                    finalEntity.setAttribute(attribute.getName(), attrValue);
                }
            });
            requestContext.recordEntityUpdate(new AtlasEntityHeader(finalEntity));
        }

    }

    public static Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    @Override
    public void run() {
        try {
            LOG.info("Starting migration: {}", DATA_MESH_QN);
            startMigration();
            LOG.info("Finished migration: {}", DATA_MESH_QN);
        } catch (Exception e) {
            LOG.error("Error running migration : {}",e.toString());
            throw new RuntimeException(e);
        }
    }
}
