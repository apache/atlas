package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
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

import static org.apache.atlas.glossary.GlossaryUtils.ATLAS_GLOSSARY_CATEGORY_TYPENAME;
import static org.apache.atlas.glossary.GlossaryUtils.ATLAS_GLOSSARY_TERM_TYPENAME;
import static org.apache.atlas.repository.Constants.*;
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
    public static final String STAKEHOLDER_EDGE_LABEL =  "__DataDomain.stakeholders";


    public static final String PARENT_DOMAIN_QN_ATTR = "parentDomainQualifiedName";
    public static final String SUPER_DOMAIN_QN_ATTR = "superDomainQualifiedName";
    public static  final String DAAP_VISIBILITY_ATTR = "daapVisibility";
    public static  final String DAAP_VISIBILITY_USERS_ATTR = "daapVisibilityUsers";
    public static  final String DAAP_VISIBILITY_GROUPS_ATTR = "daapVisibilityGroups";
    public static final String OUTPUT_PORT_GUIDS_ATTR = "daapOutputPortGuids";
    public static final String INPUT_PORT_GUIDS_ATTR = "daapInputPortGuids";
    public static final String DAAP_STATUS_ATTR = "daapStatus";
    public static final String DAAP_ARCHIVED_STATUS = "Archived";
    public static final String DAAP_ACTIVE_STATUS = "Active";
    public static final String DAAP_ASSET_DSL_ATTR = "dataProductAssetsDSL";
    public static final String DAAP_LINEAGE_STATUS_ATTR = "daapLineageStatus";
    public static final String DAAP_LINEAGE_STATUS_IN_PROGRESS = "InProgress";
    public static final String DAAP_LINEAGE_STATUS_COMPLETED = "Completed";
    public static final String DAAP_LINEAGE_STATUS_PENDING = "Pending";

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
    public static final String LEXORANK_VALID_REGEX = "^0\\|[0-9a-z]{6}:(?:[0-9a-z]{0," + LEXORANK_HARD_LIMIT + "})?$";
    public static final Set<String> ATTRIBUTES = new HashSet<>(Arrays.asList("lexicographicalSortOrder"));

    public static final Pattern LEXORANK_VALIDITY_PATTERN = Pattern.compile(LEXORANK_VALID_REGEX);

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

    public static List<AtlasVertex> retrieveVerticesFromIndexSearchPaginated(Map<String, Object> dsl, Set<String> attributes, EntityDiscoveryService discovery) throws AtlasBaseException {
        IndexSearchParams searchParams = new IndexSearchParams();
        List<AtlasVertex> ret = new ArrayList<>();

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

            List<AtlasVertex> vertices = discovery.directVerticesIndexSearch(searchParams);

            if (CollectionUtils.isNotEmpty(vertices)) {
                ret.addAll(vertices);
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

    public static void isValidLexoRank(String inputLexorank, String glossaryQualifiedName, String parentQualifiedName, EntityDiscoveryService discovery) throws AtlasBaseException {

        Matcher matcher = LEXORANK_VALIDITY_PATTERN.matcher(inputLexorank);

        if(!matcher.matches() || StringUtils.isEmpty(inputLexorank)){
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid value for lexicographicalSortOrder attribute");
        }
        // TODO : Need to discuss either to remove this after migration is successful on all tenants and custom-sort is successfully GA or keep it for re-balancing WF
        Boolean requestFromMigration = RequestContext.get().getRequestContextHeaders().getOrDefault("x-atlan-request-id", "").contains("custom-sort-migration");
        if(requestFromMigration) {
            return;
        }
        Map<String, String> lexoRankCache = RequestContext.get().getLexoRankCache();
        if(Objects.isNull(lexoRankCache)) {
            lexoRankCache = new HashMap<>();
        }
        String cacheKey = glossaryQualifiedName + "-" + parentQualifiedName;
        if(lexoRankCache.containsKey(cacheKey) && lexoRankCache.get(cacheKey).equals(inputLexorank)){
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Duplicate value for the attribute :" + LEXICOGRAPHICAL_SORT_ORDER +" found");
        }
        Map<String, Object> dslQuery = createDSLforCheckingPreExistingLexoRank(inputLexorank, glossaryQualifiedName, parentQualifiedName);
        List<AtlasEntityHeader> assetsWithDuplicateRank = new ArrayList<>();
        try {
            IndexSearchParams searchParams = new IndexSearchParams();
            searchParams.setDsl(dslQuery);
            assetsWithDuplicateRank = discovery.directIndexSearch(searchParams).getEntities();
        } catch (AtlasBaseException e) {
            LOG.error("IndexSearch Error Occured : " + e.getMessage());
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Something went wrong with IndexSearch");
        }

        if (!CollectionUtils.isEmpty(assetsWithDuplicateRank)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Duplicate Lexorank found");
        }

        lexoRankCache.put(cacheKey, inputLexorank);
        RequestContext.get().setLexoRankCache(lexoRankCache);
        // TODO : Add the rebalancing logic here
//        int colonIndex = inputLexorank.indexOf(":");
//        if (colonIndex != -1 && inputLexorank.substring(colonIndex + 1).length() >= REBALANCING_TRIGGER) {
            // Rebalancing trigger
//        }
    }

    public static void assignNewLexicographicalSortOrder(AtlasEntity entity, String glossaryQualifiedName, String parentQualifiedName, EntityDiscoveryService discovery) throws AtlasBaseException{
        Map<String, String> lexoRankCache = RequestContext.get().getLexoRankCache();

        if(Objects.isNull(lexoRankCache)) {
            lexoRankCache = new HashMap<>();
        }
        String lexoRank = "";
        String lastLexoRank = "";
        String cacheKey = glossaryQualifiedName + "-" + parentQualifiedName;

        if(lexoRankCache.containsKey(cacheKey)) {
            lastLexoRank = lexoRankCache.get(cacheKey);
        } else {

            List<AtlasEntityHeader> categories = null;
            Map<String, Object> dslQuery = generateDSLQueryForLastChild(glossaryQualifiedName, parentQualifiedName);
            try {
                IndexSearchParams searchParams = new IndexSearchParams();
                searchParams.setAttributes(ATTRIBUTES);
                searchParams.setDsl(dslQuery);
                AtlasSearchResult searchResult = discovery.directIndexSearch(searchParams);
                categories = searchResult != null ? searchResult.getEntities() : null;
            } catch (Exception e) {
                LOG.warn("Failed to search for existing lexicographicalSortOrder, using default offset: {}", e.getMessage());
                categories = null;
            }

            if (CollectionUtils.isNotEmpty(categories)) {
                AtlasEntityHeader category = categories.get(0);
                String lexicographicalSortOrder = (String) category.getAttribute(LEXICOGRAPHICAL_SORT_ORDER);
                if (StringUtils.isNotEmpty(lexicographicalSortOrder)) {
                    lastLexoRank = lexicographicalSortOrder;
                } else {
                    lastLexoRank = INIT_LEXORANK_OFFSET;
                }
            } else {
                lastLexoRank = INIT_LEXORANK_OFFSET;
            }
        }

        LexoRank parsedLexoRank = LexoRank.parse(lastLexoRank);
        LexoRank nextLexoRank = parsedLexoRank.genNext().genNext();
        lexoRank = nextLexoRank.toString();

        entity.setAttribute(LEXICOGRAPHICAL_SORT_ORDER, lexoRank);
        lexoRankCache.put(cacheKey, lexoRank);
        RequestContext.get().setLexoRankCache(lexoRankCache);
    }

    public static Map<String, Object> createDSLforCheckingPreExistingLexoRank(String lexoRank, String glossaryQualifiedName, String parentQualifiedName) {

        Map<String, Object> boolMap = buildBoolQueryDuplicateLexoRank(lexoRank, glossaryQualifiedName, parentQualifiedName);

        Map<String, Object> dsl = new HashMap<>();
        dsl.put("from", 0);
        dsl.put("size", 1);
        dsl.put("query", mapOf("bool", boolMap));

        return dsl;
    }

    private static Map<String, Object> buildBoolQueryDuplicateLexoRank(String lexoRank, String glossaryQualifiedName, String parentQualifiedName) {
        Map<String, Object> boolFilter = new HashMap<>();
        List<Map<String, Object>> mustArray = new ArrayList<>();
        mustArray.add(mapOf("term", mapOf("__state", "ACTIVE")));
        mustArray.add(mapOf("term", mapOf(LEXICOGRAPHICAL_SORT_ORDER, lexoRank)));
        if(StringUtils.isNotEmpty(glossaryQualifiedName)) {
            mustArray.add(mapOf("terms", mapOf("__typeName.keyword", Arrays.asList(ATLAS_GLOSSARY_TERM_TYPENAME, ATLAS_GLOSSARY_CATEGORY_TYPENAME))));
            mustArray.add(mapOf("term", mapOf("__glossary", glossaryQualifiedName)));
            if(StringUtils.isEmpty(parentQualifiedName)) {
                boolFilter.put("must_not", Arrays.asList(mapOf("exists", mapOf("field", "__categories")),mapOf("exists", mapOf("field", "__parentCategory"))));
            } else {
                List<Map<String, Object>> shouldParentArray = new ArrayList<>();
                shouldParentArray.add(mapOf("term", mapOf("__categories", parentQualifiedName)));
                shouldParentArray.add(mapOf("term", mapOf("__parentCategory", parentQualifiedName)));
                mustArray.add(mapOf("bool",mapOf("should", shouldParentArray)));
            }
        } else{
            mustArray.add(mapOf("terms", mapOf("__typeName.keyword", Arrays.asList(ATLAS_GLOSSARY_ENTITY_TYPE))));
        }

        boolFilter.put("must", mustArray);

        return boolFilter;
    }

    public static Map<String, Object> generateDSLQueryForLastChild(String glossaryQualifiedName, String parentQualifiedName) {

        Map<String, Object> sortKeyOrder = mapOf(LEXICOGRAPHICAL_SORT_ORDER, mapOf("order", "desc"));

        Object[] sortArray = {sortKeyOrder};

        Map<String, Object> boolMap = buildBoolQuery(glossaryQualifiedName, parentQualifiedName);

        Map<String, Object> dsl = new HashMap<>();
        dsl.put("from", 0);
        dsl.put("size", 1);
        dsl.put("sort", sortArray);
        dsl.put("query", mapOf("bool", boolMap));

        return dsl;
    }

    private static Map<String, Object> buildBoolQuery(String glossaryQualifiedName, String parentQualifiedName) {
        Map<String, Object> boolFilter = new HashMap<>();
        List<Map<String, Object>> mustArray = new ArrayList<>();
        mustArray.add(mapOf("term", mapOf("__state", "ACTIVE")));
        if(StringUtils.isNotEmpty(glossaryQualifiedName)) {
            mustArray.add(mapOf("terms", mapOf("__typeName.keyword", Arrays.asList("AtlasGlossaryTerm", "AtlasGlossaryCategory"))));
            mustArray.add(mapOf("term", mapOf("__glossary", glossaryQualifiedName)));
            if(StringUtils.isEmpty(parentQualifiedName)) {
                boolFilter.put("must_not", Arrays.asList(mapOf("exists", mapOf("field", "__categories")),mapOf("exists", mapOf("field", "__parentCategory"))));
            } else {
                List<Map<String, Object>> shouldParentArray = new ArrayList<>();
                shouldParentArray.add(mapOf("term", mapOf("__categories", parentQualifiedName)));
                shouldParentArray.add(mapOf("term", mapOf("__parentCategory", parentQualifiedName)));
                mustArray.add(mapOf("bool",mapOf("should", shouldParentArray)));
            }
        } else{
            mustArray.add(mapOf("terms", mapOf("__typeName.keyword", Arrays.asList("AtlasGlossary"))));
        }

        boolFilter.put("must", mustArray);

        return boolFilter;
    }
}
