package org.apache.atlas.repository.store.graph.v2.preprocessor ;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import org.apache.atlas.authorizer.store.UsersStore;
import org.apache.atlas.plugin.util.RangerUserStore;

import java.util.*;
import java.util.regex.Pattern;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class AssetPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AssetPreProcessor.class);

    private EntityMutationContext context;
    private AtlasTypeRegistry typeRegistry;
    private EntityGraphRetriever entityRetriever;
    private EntityGraphRetriever retrieverNoRelation = null;
    private EntityDiscoveryService discovery;
    private final Set<String> referenceAttributeNames = new HashSet<>(Arrays.asList(OUTPUT_PORT_GUIDS_ATTR, INPUT_PORT_GUIDS_ATTR));
    private final Set<String> referencingEntityTypes = new HashSet<>(Arrays.asList(DATA_PRODUCT_ENTITY_TYPE));


    private static final Set<String> excludedTypes = new HashSet<>(Arrays.asList(ATLAS_GLOSSARY_ENTITY_TYPE, ATLAS_GLOSSARY_TERM_ENTITY_TYPE, ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE, DATA_PRODUCT_ENTITY_TYPE, DATA_DOMAIN_ENTITY_TYPE));

    private static final Pattern SSI_TAG_PATTERN = Pattern.compile("<!--#\\s*\\w+.*-->", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    public AssetPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph) {
        this(typeRegistry, entityRetriever, graph, null);
    }

    public AssetPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, EntityDiscoveryService discovery) {
        this(typeRegistry, entityRetriever, graph, discovery, new EntityGraphRetriever(entityRetriever, true));
    }

    AssetPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph, EntityDiscoveryService discovery, EntityGraphRetriever retrieverNoRelation) {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.retrieverNoRelation = retrieverNoRelation;

        if (discovery != null) {
            this.discovery = discovery;
        } else {
            try {
                this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null);
            } catch (AtlasException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            Object qualifiedName = entityStruct.getAttribute(QUALIFIED_NAME);
            LOG.debug("AssetPreProcessor.processAttributes: pre processing {}, {}",
                    qualifiedName != null ? qualifiedName.toString().replaceAll("[\r\n]", "_") : "null", operation);
        }
        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;

        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreateAsset(entity, vertex, operation);
                break;
            case UPDATE:
                processUpdateAsset(entity, vertex, operation);
                break;
        }
    }

    private void processCreateAsset(AtlasEntity entity, AtlasVertex vertex, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateAsset");

        validateUserAndGroupAttributes(entity);
        processDomainLinkAttribute(entity, vertex, operation);

        RequestContext.get().endMetricRecord(metricRecorder);
    }


    private void processUpdateAsset(AtlasEntity entity, AtlasVertex vertex, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateAsset");

        validateUserAndGroupAttributes(entity);
        processDomainLinkAttribute(entity, vertex, operation);

        RequestContext.get().endMetricRecord(metricRecorder);

    }

    private void processDomainLinkAttribute(AtlasEntity entity, AtlasVertex vertex, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if(entity.hasAttribute(DOMAIN_GUIDS)){
            validateDomainAssetLinks(entity);
            isAuthorized(vertex, operation, entity);
        }
    }

    private void validateDomainAssetLinks(AtlasEntity entity) throws AtlasBaseException {
        List<String> domainGuids = ( List<String>) entity.getAttribute(DOMAIN_GUIDS);

        if(CollectionUtils.isNotEmpty(domainGuids)){
            if(domainGuids.size() > 1) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Asset can be linked to only one domain");
            }

            if (excludedTypes.contains(entity.getTypeName())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "This AssetType is not allowed to link with Domain", entity.getTypeName());
            }

            for(String domainGuid : domainGuids) {
                AtlasVertex domainVertex = entityRetriever.getEntityVertex(domainGuid);
                if(domainVertex == null) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, domainGuid);
                }

                String domainEntityType = domainVertex.getProperty(TYPE_NAME_PROPERTY_KEY, String.class);

                if (!Objects.equals(domainEntityType, DATA_DOMAIN_ENTITY_TYPE)){
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Asset can be linked to only domain");
                }
            }
        }
    }

    private void isAuthorized(AtlasVertex vertex, EntityMutations.EntityOperation operation, AtlasEntity entity) throws AtlasBaseException {
        AtlasEntityHeader sourceEntity;

        if (operation == EntityMutations.EntityOperation.CREATE) {
            sourceEntity = new AtlasEntityHeader(entity);
        } else {
            sourceEntity = retrieverNoRelation.toAtlasEntityHeaderWithClassifications(vertex);
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, sourceEntity),
                "update on source Entity, link/unlink operation denied: ", sourceEntity.getAttribute(NAME));

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, sourceEntity),
                "read on source Entity, link/unlink operation denied: ", sourceEntity.getAttribute(NAME));
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processDeleteAsset");

        try {
            DeleteType deleteType = RequestContext.get().getDeleteType();
            if (deleteType.equals(DeleteType.HARD) || deleteType.equals(DeleteType.PURGE)) {
                removeAssetGuidFromAttributeReferences(vertex, referencingEntityTypes, referenceAttributeNames);
            } else {
                LOG.info("processDeleteAsset: Skipping cleanup for soft delete of asset: {}", GraphHelper.getGuid(vertex));
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void removeAssetGuidFromAttributeReferences(AtlasVertex vertex, Set<String> referencingEntityTypes, Set<String> referenceAttributeNames) {
        try {
            if (isAssetType(vertex)) {
                String guid = GraphHelper.getGuid(vertex);
                int totalAttributeRefsRemoved = 0;

                if (CollectionUtils.isEmpty(referencingEntityTypes) || CollectionUtils.isEmpty(referenceAttributeNames)) {
                    LOG.warn("removeAssetGuidFromAttributeReferences: Empty entity types or attribute names set for asset: {}", guid);
                    return;
                }

                for (String entityType: referencingEntityTypes) {
                    int currentEntityRefcount = 0;

                    for (String attributeName: referenceAttributeNames) {
                        int currentAttributeRefcount = 0;

                        try {
                            List<AtlasVertex> entityVertices = fetchEntityVerticesUsingIndexSearch(entityType, attributeName, guid);

                            for (AtlasVertex entityVertex: entityVertices) {

                                AtlasGraphUtilsV2.removeItemFromListPropertyValue(
                                        entityVertex,
                                        attributeName,
                                        guid
                                );
                                currentAttributeRefcount += 1;
                                currentEntityRefcount += 1;
                                totalAttributeRefsRemoved += 1;
                            }
                        } catch (Exception e) {
                            LOG.error("removeAssetGuidFromAttributeReferences: failed to cleanup attribute reference for asset {} from individual entity", guid, e);
                        }

                        if (currentAttributeRefcount > 0) {
                            LOG.info("removeAssetGuidFromAttributeReferences: removed {} references for attribute {} in entity type {} for asset: {}", 
                                currentAttributeRefcount, attributeName, entityType, guid);
                        }
                    }

                    if (currentEntityRefcount > 0) {
                        LOG.info("removeAssetGuidFromAttributeReferences: removed {} total references for entity type {} for asset: {}", 
                            currentEntityRefcount, entityType, guid);
                    }
                }

                if (totalAttributeRefsRemoved > 0) {
                    LOG.info("removeAssetGuidFromAttributeReferences: successfully cleaned up {} total attribute references for asset: {}", 
                        totalAttributeRefsRemoved, guid);
                }
            }
        }
        catch (Exception e) {
            LOG.error("removeAssetGuidFromAttributeReferences: unexpected error during cleanup", e);
        }
    }

    private List<AtlasVertex> fetchEntityVerticesUsingIndexSearch(String typeName, String attributeName, String guid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("findProductsWithPortGuid");
        try {
            List<Map<String, Object>> mustClauses = new ArrayList<>();
            mustClauses.add(mapOf("term", mapOf("__typeName.keyword", typeName)));
            mustClauses.add(mapOf("term", mapOf(attributeName, guid)));

            Map<String, Object> bool = new HashMap<>();
            bool.put("must", mustClauses);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

            return retrieveVerticesFromIndexSearchPaginated(dsl, null, discovery);

        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private boolean isAssetType(AtlasVertex vertex) {
        String typeName = GraphHelper.getTypeName(vertex);
        if (excludedTypes.contains(typeName)) {
            return false;
        }

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

        return entityType != null && entityType.getTypeAndAllSuperTypes().contains("Asset");
    }

    private void validateUserAndGroupAttributes(AtlasEntity entity) throws AtlasBaseException {
        validateGroupAttributes(entity);
        validateUserAttributes(entity);
        validateAnnouncementMessage(entity);
    }

    private void validateGroupAttributes(AtlasEntity entity) throws AtlasBaseException {
        RangerUserStore userStore = UsersStore.getInstance().getUserStore();
        Set<String> validGroups = null;

        if (userStore != null) {
            Map<String, Map<String, String>> groupAttrMapping = userStore.getGroupAttrMapping();
            if (groupAttrMapping != null) {
                validGroups = groupAttrMapping.keySet();
            }

            if (validGroups != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[DEBUG_SECURITY] Valid group count: {}", validGroups.size());
                }
            } else {
                LOG.warn("[DEBUG_SECURITY] Group mapping is null");
            }
        } else {
            LOG.warn("[DEBUG_SECURITY] RangerUserStore is null. Cannot validate groups.");
        }

        validateAttribute(entity, ATTR_OWNER_GROUPS, "group", validGroups);
        validateAttribute(entity, ATTR_ADMIN_GROUPS, "group", validGroups);
        validateAttribute(entity, ATTR_VIEWER_GROUPS, "group", validGroups);
    }

    private void validateUserAttributes(AtlasEntity entity) throws AtlasBaseException {
        RangerUserStore userStore = UsersStore.getInstance().getUserStore();
        Set<String> validUsers = null;

        if (userStore != null) {
            Map<String, Set<String>> userGroupMapping = userStore.getUserGroupMapping();
            if (userGroupMapping != null) {
                validUsers = userGroupMapping.keySet();
            }

            if (validUsers != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[DEBUG_SECURITY] Valid user count: {}", validUsers.size());
                }
            } else {
                LOG.warn("[DEBUG_SECURITY] User mapping is null");
            }
        } else {
            LOG.warn("[DEBUG_SECURITY] RangerUserStore is null. Cannot validate users.");
        }

        validateAttribute(entity, OWNER_ATTRIBUTE, "user", validUsers);
        validateAttribute(entity, ATTR_OWNER_USERS, "user", validUsers);
        validateAttribute(entity, ATTR_ADMIN_USERS, "user", validUsers);
        validateAttribute(entity, ATTR_VIEWER_USERS, "user", validUsers);
    }

    private void validateAnnouncementMessage(AtlasEntity entity) throws AtlasBaseException {
        if (entity.hasAttribute(ATTR_ANNOUNCEMENT_MESSAGE)) {
            Object attributeValue = entity.getAttribute(ATTR_ANNOUNCEMENT_MESSAGE);
            if (attributeValue != null) {
                if (!(attributeValue instanceof String message)) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid announcementMessage: must be string");
                }
                if (StringUtils.isNotEmpty(message) && SSI_TAG_PATTERN.matcher(message).find()) {
                    String assetIdentifier = getAssetIdentifier(entity);
                    LOG.warn("SSI tags detected in announcementMessage for asset: {}, message: {}", assetIdentifier, sanitizeForLogging(message));
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid announcementMessage: SSI tags are not allowed");
                }
            }
        }
    }

    private String getAssetIdentifier(AtlasEntity entity) {
        Object qualifiedName = entity.getAttribute(QUALIFIED_NAME);
        if (qualifiedName != null) {
            return sanitizeForLogging(qualifiedName.toString());
        }
        return entity.getGuid() != null ? entity.getGuid() : "unknown";
    }

    /**
     * Sanitizes a string for safe logging by replacing newline characters.
     * This prevents log forging attacks where an attacker could inject fake log entries.
     */
    private String sanitizeForLogging(String value) {
        if (value == null) {
            return null;
        }
        return value.replaceAll("[\r\n]", "_");
    }

    private void validateAttribute(AtlasEntity entity, String attributeName, String type, Set<String> validNames) throws AtlasBaseException {
        if (!entity.hasAttribute(attributeName)) {
            return;
        }

        Object attributeValue = entity.getAttribute(attributeName);
        if (attributeValue == null) {
            return;
        }

        if (attributeValue instanceof Collection) {
            Collection<?> values = (Collection<?>) attributeValue;
            List<String> validValues = new ArrayList<>();

            for (Object itemObj : values) {
                if (!(itemObj instanceof String)) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, 
                            "Invalid " + type + " name: must be string, got " + (itemObj == null ? "null" : itemObj.getClass().getSimpleName()));
                }
                String item = ((String) itemObj).trim();
                if (isValidAndExists(item, type, validNames)) {
                    validValues.add(item);
                } else {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid " + type + " name: " + sanitizeForLogging(item));
                }
            }
            entity.setAttribute(attributeName, validValues);

        } else {
            if (!(attributeValue instanceof String)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, 
                        "Invalid " + type + " attribute: must be string or collection of strings");
            }
            String value = ((String) attributeValue).trim();
            if (!isValidAndExists(value, type, validNames)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid " + type + " name: " + sanitizeForLogging(value));
            }
            entity.setAttribute(attributeName, value);
        }
    }

    private boolean isValidAndExists(String name, String type, Set<String> validNames) throws AtlasBaseException {
        if (StringUtils.isEmpty(name)) {
            return false;
        }

        name = name.trim();

        // 1. Sanitization (Security) - Fail Fast
        if (SSI_TAG_PATTERN.matcher(name).find()) {
             throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid " + type + " name: SSI tags are not allowed");
        }
        if (name.contains("<") || name.contains(">")) {
             throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid " + type + " name: Special characters < > are not allowed");
        }
        if (name.toLowerCase().startsWith("http:") || name.toLowerCase().startsWith("https:")) {
             throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid " + type + " name: URLs are not allowed");
        }

        // 2. Existence Check (Cleanup)
        // If we have a list of valid names, and the name is NOT in it, return false (filter it out).
        // Also skip validation if validNames is empty - this indicates a failed load from Heracles/auth service,
        // and we should not block requests due to transient API failures.
        if (validNames != null && !validNames.isEmpty() && !validNames.contains(name)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[DEBUG_SECURITY] Invalid/Non-existent {} rejected.", type);
            }
            
            return false;
        }
        
        if (validNames == null || validNames.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[DEBUG_SECURITY] validNames is null or empty for {}. Skipping existence check.", type);
            }
            
        }

        return true;
    }
}
