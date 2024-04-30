package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.NanoIdUtils;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.QUERY_COLLECTION_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class PreProcessorUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PreProcessorUtils.class);

    private static final char[] invalidNameChars = {'@'};

    //Glossary models constants
    public static final String ANCHOR            = "anchor";
    public static final String CATEGORY_TERMS    = "terms";
    public static final String CATEGORY_PARENT   = "parentCategory";
    public static final String CATEGORY_CHILDREN = "childrenCategories";
    public static final String GLOSSARY_TERM_REL_TYPE = "AtlasGlossaryTermAnchor";
    public static final String GLOSSARY_CATEGORY_REL_TYPE = "AtlasGlossaryCategoryAnchor";

    //DataMesh models constants
    public static final String DATA_PRODUCT_TYPE = "DataProduct";
    public static final String PARENT_DOMAIN = "parentDomain";
    public static final String PARENT_DOMAIN_QN = "parentDomainQualifiedName";
    public static final String SUPER_DOMAIN_QN = "superDomainQualifiedName";
    public static final String DATA_DOMAIN = "dataDomain";
    public static final String MIGRATION_CUSTOM_ATTRIBUTE = "isQualifiedNameMigrated";

    //Query models constants
    public static final String PREFIX_QUERY_QN   = "default/collection/";
    public static final String COLLECTION_QUALIFIED_NAME = "collectionQualifiedName";
    public static final String PARENT_QUALIFIED_NAME = "parentQualifiedName";
    public static final String PARENT_ATTRIBUTE_NAME    = "parent";

    /**
     * Folder,Collection, Query relations
     */

    public static final String CHILDREN_QUERIES = "__Namespace.childrenQueries";
    public static final String CHILDREN_FOLDERS = "__Namespace.childrenFolders";

    public static String getUUID(){
        return NanoIdUtils.randomNanoId();
    }

    public static String getUserName(){
        return NanoIdUtils.randomNanoId();
    }

    public static boolean isNameInvalid(String name) {
        return StringUtils.containsAny(name, invalidNameChars);
    }

    public static String getCollectionPropertyName(AtlasVertex parentVertex) {
        return QUERY_COLLECTION_ENTITY_TYPE.equals(parentVertex.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class)) ? QUALIFIED_NAME : COLLECTION_QUALIFIED_NAME;
    }

    public static String updateQueryResourceAttributes(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                                                                       AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        AtlasEntityType entityType      = typeRegistry.getEntityTypeByName(entity.getTypeName());
        AtlasObjectId newParentObjectId = (AtlasObjectId) entity.getRelationshipAttribute(PARENT_ATTRIBUTE_NAME);
        String relationshipType         = AtlasEntityUtil.getRelationshipType(newParentObjectId);
        AtlasStructType.AtlasAttribute parentAttribute  = entityType.getRelationshipAttribute(PARENT_ATTRIBUTE_NAME, relationshipType);
        AtlasObjectId currentParentObjectId = (AtlasObjectId) entityRetriever.getEntityAttribute(vertex, parentAttribute);
        //Qualified name of the folder/query will not be updated if parent attribute is not changed
        String qualifiedName      = vertex.getProperty(QUALIFIED_NAME, String.class);
        entity.setAttribute(QUALIFIED_NAME, qualifiedName);

        //Check if parent attribute is changed
        if (currentParentObjectId == null || parentAttribute.getAttributeType().areEqualValues(currentParentObjectId, newParentObjectId, context.getGuidAssignments())) {
            return null;
        }

        AtlasVertex currentParentVertex         = entityRetriever.getEntityVertex(currentParentObjectId);
        AtlasVertex newParentVertex             = entityRetriever.getEntityVertex(newParentObjectId);

        if (currentParentVertex == null || newParentVertex == null) {
            LOG.error("Current or New parent vertex is null");
            throw new AtlasBaseException("Current or New parent vertex is null");
        }

        String currentCollectionQualifiedName   = currentParentVertex.getProperty(getCollectionPropertyName(currentParentVertex), String.class);
        String newCollectionQualifiedName       = newParentVertex.getProperty(getCollectionPropertyName(newParentVertex), String.class);
        String updatedParentQualifiedName       = newParentVertex.getProperty(QUALIFIED_NAME, String.class);

        if (StringUtils.isEmpty(newCollectionQualifiedName) || StringUtils.isEmpty(currentCollectionQualifiedName)) {
            LOG.error("Collection qualified name in parent or current entity is empty or null");
            throw new AtlasBaseException("Collection qualified name in parent or current entity is empty or null");
        }

        entity.setAttribute(PARENT_QUALIFIED_NAME, updatedParentQualifiedName);

        if(currentCollectionQualifiedName.equals(newCollectionQualifiedName)) {
            return null;
        }

        String updatedQualifiedName = qualifiedName.replaceAll(currentCollectionQualifiedName, newCollectionQualifiedName);
        //Update this values into AtlasEntity
        entity.setAttribute(QUALIFIED_NAME, updatedQualifiedName);
        entity.setAttribute(COLLECTION_QUALIFIED_NAME, newCollectionQualifiedName);

        return newCollectionQualifiedName;
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
}
