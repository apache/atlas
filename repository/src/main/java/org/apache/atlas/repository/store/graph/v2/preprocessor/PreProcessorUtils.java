package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
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
import org.apache.atlas.util.lexoRank.LexoRank;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.atlas.repository.Constants.QUERY_COLLECTION_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;
import static org.apache.atlas.type.Constants.LEXICOGRAPHICAL_SORT_ORDER;

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
    public static final String INIT_LEXORANK_OFFSET = "0|100000:";

    //DataMesh models constants
    public static final String PARENT_DOMAIN_REL_TYPE = "parentDomain";
    public static final String SUB_DOMAIN_REL_TYPE = "subDomains";
    public static final String DATA_PRODUCT_REL_TYPE = "dataProducts";
    public static final String MIGRATION_CUSTOM_ATTRIBUTE = "isQualifiedNameMigrated";
    public static final String DATA_DOMAIN_REL_TYPE = "dataDomain";
    public static final String STAKEHOLDER_REL_TYPE = "stakeholders";

    public static final String MESH_POLICY_CATEGORY = "datamesh";

    public static final String DATA_PRODUCT_EDGE_LABEL     = "__DataDomain.dataProducts";
    public static final String DOMAIN_PARENT_EDGE_LABEL    = "__DataDomain.subDomains";

    public static final String PARENT_DOMAIN_QN_ATTR = "parentDomainQualifiedName";
    public static final String SUPER_DOMAIN_QN_ATTR = "superDomainQualifiedName";
    public static  final String DAAP_VISIBILITY_ATTR = "daapVisibility";
    public static  final String DAAP_VISIBILITY_USERS_ATTR = "daapVisibilityUsers";
    public static  final String DAAP_VISIBILITY_GROUPS_ATTR = "daapVisibilityGroups";
    public static final String OUTPUT_PORT_GUIDS_ATTR = "daapOutputPortGuids";
    public static final String INPUT_PORT_GUIDS_ATTR = "daapInputPortGuids";
    public static final String DAAP_STATUS_ATTR = "daapStatus";
    public static final String DAAP_ARCHIVED_STATUS = "Archived";

    //Migration Constants
    public static final String MIGRATION_TYPE_PREFIX = "MIGRATION:";
    public static final String DATA_MESH_QN = MIGRATION_TYPE_PREFIX + "DATA_MESH_QN";

    public enum MigrationStatus {
        IN_PROGRESS,
        SUCCESSFUL,
        FAILED;
    }

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
    public static final int REBALANCING_TRIGGER = 119;
    public static final int PRE_DELIMITER_LENGTH = 9;
    public static final String LEXORANK_HARD_LIMIT = "" + (256 - PRE_DELIMITER_LENGTH);

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

        if (CollectionUtils.isNotEmpty(attributes)) {
            searchParams.setAttributes(attributes);
        }

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

    public static void verifyDuplicateAssetByName(String typeName, String assetName, EntityDiscoveryService discovery, String errorMessage) throws AtlasBaseException {
        List<Map<String, Object>> mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", typeName)));
        mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
        mustClauseList.add(mapOf("term", mapOf("name.keyword", assetName)));


        Map<String, Object> bool = mapOf("must", mustClauseList);

        Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

        List<AtlasEntityHeader> assets = indexSearchPaginated(dsl, null, discovery);

        if (CollectionUtils.isNotEmpty(assets)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, errorMessage);
        }
    }

    public static void isValidLexoRank(String input, String glossaryQualifiedName, String parentQualifiedName, EntityDiscoveryService discovery) throws AtlasBaseException {
        String pattern = "^0\\|[0-9a-z]{6}:(?:[0-9a-z]{0," + LEXORANK_HARD_LIMIT + "})?$";
        // TODO : To remove this after migration is successful on all tenants and custom-sort is successfully GA
        Boolean requestFromMigration = RequestContext.get().getRequestContextHeaders().getOrDefault("x-atlan-request-id", "").contains("custom-sort-migration");
        Pattern regex = Pattern.compile(pattern);

        Matcher matcher = regex.matcher(input);

        if(!matcher.matches()){
            throw new AtlasBaseException("Invalid LexicographicSortOrder");
        }
        if(!requestFromMigration) {
            Map<String, Object> dslQuery = createDSLforCheckingPreExistingLexoRank(input, glossaryQualifiedName, parentQualifiedName);
            List<AtlasEntityHeader> categories = new ArrayList<>();
            try {
                IndexSearchParams searchParams = new IndexSearchParams();
                searchParams.setAttributes(new HashSet<>());
                searchParams.setDsl(dslQuery);
                categories = discovery.directIndexSearch(searchParams).getEntities();
            } catch (AtlasBaseException e) {
                e.printStackTrace();
            }

            if (!CollectionUtils.isEmpty(categories)) {
                throw new AtlasBaseException("Invalid LexicographicSortOrder");
            }
        }
        // TODO : Add the rebalancing logic here
        int colonIndex = input.indexOf(":");
        if (colonIndex != -1 && input.substring(colonIndex + 1).length() >= REBALANCING_TRIGGER) {
            // Rebalancing trigger
        }
    }

    public static void assignNewLexicographicalSortOrder(AtlasEntity entity, String glossaryQualifiedName, String parentQualifiedName, EntityDiscoveryService discovery) {
        Map<String, String> lexoRankCache = RequestContext.get().getLexoRankCache();

        if(Objects.isNull(lexoRankCache)) {
            lexoRankCache = new HashMap<>();
        }
        String lexoRank = "";
        String lastLexoRank = "";

        if(lexoRankCache.containsKey(glossaryQualifiedName + "-" + parentQualifiedName)) {
            lastLexoRank = lexoRankCache.get(glossaryQualifiedName + "-" + parentQualifiedName);

        } else {
            Set<String> attributes = new HashSet<>();
            attributes.add(LEXICOGRAPHICAL_SORT_ORDER);
            List<AtlasEntityHeader> categories = null;
            Map<String, Object> dslQuery = generateDSLQueryForLastCategory(glossaryQualifiedName, parentQualifiedName);
            try {
                IndexSearchParams searchParams = new IndexSearchParams();
                searchParams.setAttributes(attributes);
                searchParams.setDsl(dslQuery);
                categories = discovery.directIndexSearch(searchParams).getEntities();
            } catch (AtlasBaseException e) {
                e.printStackTrace();
            }

            if (CollectionUtils.isNotEmpty(categories)) {
                for (AtlasEntityHeader category : categories) {
                    String lexicographicalSortOrder = (String) category.getAttribute(LEXICOGRAPHICAL_SORT_ORDER);
                    if (StringUtils.isNotEmpty(lexicographicalSortOrder)) {
                        lastLexoRank = lexicographicalSortOrder;
                    } else {
                        lastLexoRank = INIT_LEXORANK_OFFSET;
                    }
                }
            } else {
                lastLexoRank = INIT_LEXORANK_OFFSET;
            }
        }

        LexoRank parsedLexoRank = LexoRank.parse(lastLexoRank);
        LexoRank nextLexoRank = parsedLexoRank.genNext().genNext();
        lexoRank = nextLexoRank.toString();

        entity.setAttribute(LEXICOGRAPHICAL_SORT_ORDER, lexoRank);
        lexoRankCache.put(glossaryQualifiedName + "-" + parentQualifiedName, lexoRank);
        RequestContext.get().setLexoRankCache(lexoRankCache);
    }

    public static Map<String, Object> createDSLforCheckingPreExistingLexoRank(String lexoRank, String glossaryQualifiedName, String parentQualifiedName) {

        Map<String, Object> sortKeyOrder = mapOf(LEXICOGRAPHICAL_SORT_ORDER, mapOf("order", "desc"));
        Map<String, Object> scoreSortOrder = mapOf("_score", mapOf("order", "desc"));
        Map<String, Object> displayNameSortOrder = mapOf("displayName.keyword", mapOf("order", "desc"));

        Object[] sortArray = {sortKeyOrder, scoreSortOrder, displayNameSortOrder};

        Map<String, Object> functionScore = mapOf("query", buildBoolQueryDuplicateLexoRank(lexoRank, glossaryQualifiedName, parentQualifiedName));

        Map<String, Object> dsl = new HashMap<>();
        dsl.put("from", 0);
        dsl.put("size", 100);
        dsl.put("sort", sortArray);
        dsl.put("query", mapOf("function_score", functionScore));

        return dsl;
    }

    private static Map<String, Object> buildBoolQueryDuplicateLexoRank(String lexoRank, String glossaryQualifiedName, String parentQualifiedName) {
        Map<String, Object> boolQuery = new HashMap<>();
        int mustArrayLength = 0;
        if(StringUtils.isEmpty(parentQualifiedName) && StringUtils.isEmpty(glossaryQualifiedName)){
            mustArrayLength = 3;
        } else if(StringUtils.isEmpty(parentQualifiedName) && StringUtils.isNotEmpty(glossaryQualifiedName)){
            mustArrayLength = 4;
        } else {
            mustArrayLength = 5;
        }
        Map<String, Object>[] mustArray = new Map[mustArrayLength];
        Map<String, Object> boolFilter = new HashMap<>();
        Map<String, Object>[] mustNotArray = new Map[2];

        mustArray[0] = mapOf("term", mapOf("__state", "ACTIVE"));
        mustArray[1] = mapOf("term", mapOf(LEXICOGRAPHICAL_SORT_ORDER, lexoRank));
        if(StringUtils.isNotEmpty(glossaryQualifiedName)) {
            mustArray[2] = mapOf("terms", mapOf("__typeName.keyword", Arrays.asList("AtlasGlossaryTerm", "AtlasGlossaryCategory")));
            mustArray[3] = mapOf("term", mapOf("__glossary", glossaryQualifiedName));
        } else{
            mustArray[2] = mapOf("terms", mapOf("__typeName.keyword", Arrays.asList("AtlasGlossary")));
        }

        if(StringUtils.isEmpty(parentQualifiedName)) {
            mustNotArray[0] = mapOf("exists", mapOf("field", "__categories"));
            mustNotArray[1] = mapOf("exists", mapOf("field", "__parentCategory"));
            boolFilter.put("must_not", mustNotArray);
        }
        else {
            Map<String, Object>[] shouldParentArray = new Map[2];
            shouldParentArray[0] = mapOf("term", mapOf("__categories", parentQualifiedName));
            shouldParentArray[1] = mapOf("term", mapOf("__parentCategory", parentQualifiedName));
            mustArray[4] = mapOf("bool",mapOf("should", shouldParentArray));
        }

        boolFilter.put("must", mustArray);

        Map<String, Object> nestedBoolQuery = mapOf("bool", boolFilter);

        Map<String, Object> topBoolFilter = mapOf("filter", nestedBoolQuery);

        boolQuery.put("bool", topBoolFilter);

        return boolQuery;
    }
    public static Map<String, Object> generateDSLQueryForLastCategory(String glossaryQualifiedName, String parentQualifiedName) {

        Map<String, Object> sortKeyOrder = mapOf(LEXICOGRAPHICAL_SORT_ORDER, mapOf("order", "desc"));
        Map<String, Object> scoreSortOrder = mapOf("_score", mapOf("order", "desc"));
        Map<String, Object> displayNameSortOrder = mapOf("displayName.keyword", mapOf("order", "desc"));

        Object[] sortArray = {sortKeyOrder, scoreSortOrder, displayNameSortOrder};

        Map<String, Object> functionScore = mapOf("query", buildBoolQuery(glossaryQualifiedName, parentQualifiedName));

        Map<String, Object> dsl = new HashMap<>();
        dsl.put("from", 0);
        dsl.put("size", 1);
        dsl.put("sort", sortArray);
        dsl.put("query", mapOf("function_score", functionScore));

        return dsl;
    }

    private static Map<String, Object> buildBoolQuery(String glossaryQualifiedName, String parentQualifiedName) {
        Map<String, Object> boolQuery = new HashMap<>();
        int mustArrayLength = 0;
        if(StringUtils.isEmpty(parentQualifiedName) && StringUtils.isEmpty(glossaryQualifiedName)){
            mustArrayLength = 2;
        } else if(StringUtils.isEmpty(parentQualifiedName) && StringUtils.isNotEmpty(glossaryQualifiedName)){
            mustArrayLength = 3;
        } else {
            mustArrayLength = 4;
        }
        Map<String, Object>[] mustArray = new Map[mustArrayLength];
        Map<String, Object> boolFilter = new HashMap<>();
        Map<String, Object>[] mustNotArray = new Map[2];

        mustArray[0] = mapOf("term", mapOf("__state", "ACTIVE"));
        if(StringUtils.isNotEmpty(glossaryQualifiedName)) {
            mustArray[1] = mapOf("terms", mapOf("__typeName.keyword", Arrays.asList("AtlasGlossaryTerm", "AtlasGlossaryCategory")));
            mustArray[2] = mapOf("term", mapOf("__glossary", glossaryQualifiedName));
        } else{
            mustArray[1] = mapOf("terms", mapOf("__typeName.keyword", Arrays.asList("AtlasGlossary")));
        }

        if(StringUtils.isEmpty(parentQualifiedName)) {
            mustNotArray[0] = mapOf("exists", mapOf("field", "__categories"));
            mustNotArray[1] = mapOf("exists", mapOf("field", "__parentCategory"));
            boolFilter.put("must_not", mustNotArray);
        }
        else {
            Map<String, Object>[] shouldParentArray = new Map[2];
            shouldParentArray[0] = mapOf("term", mapOf("__categories", parentQualifiedName));
            shouldParentArray[1] = mapOf("term", mapOf("__parentCategory", parentQualifiedName));
            mustArray[3] = mapOf("bool",mapOf("should", shouldParentArray));
        }

        boolFilter.put("must", mustArray);

        Map<String, Object> nestedBoolQuery = mapOf("bool", boolFilter);

        Map<String, Object> topBoolFilter = mapOf("filter", nestedBoolQuery);

        boolQuery.put("bool", topBoolFilter);

        return boolQuery;
    }
}
