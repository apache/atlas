package org.apache.atlas.repository.store.graph.v2.tags;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.Tag;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.*;

import static org.apache.atlas.repository.store.graph.v2.tags.CassandraTagConfig.*;
import static org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test class for TagDAOCassandraImpl.
 *
 * This class connects to a running Cassandra instance to test the DAO's functionality.
 * It tests all the public methods of the TagDAOCassandraImpl class, ensuring that the
 * data is correctly written to and read from the Cassandra tables.
 *
 * Prerequisites:
 * - A Cassandra instance running and accessible on localhost:9042.
 * - The keyspace 'tags_v2' should be accessible (the DAO will create it if not present).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TagDAOCassandraImplTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private TagDAOCassandraImpl tagDAO;
    private CqlSession testSession;


    @BeforeAll
    void setUp() throws AtlasBaseException, AtlasException {
        // Create an in-memory configuration to prevent the DAO from trying to load a file.
        // This resolves the "Failed to load application properties" error during tests.
        Configuration testConfig = new PropertiesConfiguration();
        testConfig.setProperty(CassandraTagConfig.CASSANDRA_HOSTNAME_PROPERTY, "localhost");

        // Inject the mock configuration into the ApplicationProperties class.
        ApplicationProperties.set(testConfig);

        // The DAO will now use the in-memory configuration instead of loading from a file.
        tagDAO = new TagDAOCassandraImpl();

        // Create a separate CqlSession for test utility functions like data setup and cleanup.
        testSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("localhost", CASSANDRA_PORT))
                .withLocalDatacenter(DATACENTER) // Use the same datacenter as the DAO
                .build();
    }

    @BeforeEach
    void clearTablesAndRequestContext() {
        // Truncate tables before each test to ensure a clean slate.
        testSession.execute(String.format("TRUNCATE TABLE %s.%s", KEYSPACE, EFFECTIVE_TAGS_TABLE_NAME));
        testSession.execute(String.format("TRUNCATE TABLE %s.%s", KEYSPACE, PROPAGATED_TAGS_TABLE_NAME));

        // Reset RequestContext before each test as the DAO uses it for timestamps.
        RequestContext.clear();
        //RequestContext.set(new RequestContext());
    }

    @AfterAll
    void tearDown() {
        if (tagDAO != null) {
            tagDAO.close();
        }
        if (testSession != null && !testSession.isClosed()) {
            testSession.close();
        }

        ApplicationProperties.forceReload();
    }

    // =================== Test Cases ===================

    @Test
    void testScenarioWithProvidedData() throws AtlasBaseException, IOException {
        // --- 1. Setup: Insert the provided sample data directly into the database ---
        String assetId = "40964208";
        String tagTypeName = "dZdVxX8U8hj19zDqhr81ZR";
        int bucket = TagDAOCassandraImpl.calculateBucket(assetId);

        String directTagMetaJson = "{\"typeName\":\"dZdVxX8U8hj19zDqhr81ZR\",\"attributes\":{\"GgKQQ5k7voU8kxBsIod7fw\":[{\"typeName\":\"SourceTagAttachment\",\"attributes\":{\"sourceTagGuid\":\"ce9b94c1-b8a1-4552-91da-2408434af9b4\",\"sourceTagConnectorName\":\"snowflake\",\"sourceTagName\":\"C5 Public\",\"sourceTagValue\":[{\"typeName\":\"SourceTagAttachmentValue\",\"attributes\":{\"tagAttachmentValue\":\"C4\"}}],\"sourceTagQualifiedName\":\"default/snowflake/1741259414/GOVERNANCE/PUBLIC/C4 Public\"}}],\"QT6ZIX14kPyUZmdUMaUYwI\":\"ABC\"},\"entityGuid\":\"02f551b1-5ad0-4718-af45-44d0213bf48b\",\"entityStatus\":\"ACTIVE\",\"propagate\":true,\"removePropagationsOnEntityDelete\":true,\"restrictPropagationThroughLineage\":false,\"restrictPropagationThroughHierarchy\":false}";
        String directAssetMetadata = "{\"__modifiedBy\":\"admin\",\"qualifiedName\":\"default/redshift/1234/table_000001\",\"__typeName\":\"Table\",\"__createdBy\":\"admin\",\"__modificationTimestamp\":1750760080584,\"name\":\"table_000001\",\"__guid\":\"02f551b1-5ad0-4718-af45-44d0213bf48b\",\"__timestamp\":1750756510291}";
        AtlasClassification expectedClassification = objectMapper.readValue(directTagMetaJson, AtlasClassification.class);

        SimpleStatement insert = SimpleStatement.builder(
                        "INSERT INTO tags.tags_by_id (bucket, id, is_propagated, source_id, tag_type_name, tag_meta_json, asset_metadata, is_deleted, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .addPositionalValue(bucket)
                .addPositionalValue(assetId)
                .addPositionalValue(false) // Direct tag
                .addPositionalValue(assetId) // For direct tags, source_id is the assetId itself
                .addPositionalValue(tagTypeName)
                .addPositionalValue(directTagMetaJson)
                .addPositionalValue(directAssetMetadata)
                .addPositionalValue(false)
                .addPositionalValue(Instant.now()) // Set the updated_at timestamp
                .build();
        testSession.execute(insert);

        // --- 2. Validation: Verify updated_at is set in the database ---
        SimpleStatement selectStmt = SimpleStatement.builder("SELECT updated_at FROM tags.tags_by_id WHERE bucket = ? AND id = ? AND is_propagated = false AND source_id = ? AND tag_type_name = ?")
                .addPositionalValue(bucket)
                .addPositionalValue(assetId)
                .addPositionalValue(assetId)
                .addPositionalValue(tagTypeName)
                .build();
        Row row = testSession.execute(selectStmt).one();
        assertNotNull(row, "The inserted row should be found in the database.");
        assertNotNull(row.getInstant("updated_at"), "The updated_at column should not be null.");


        // --- 3. Action & Assertion: Use the DAO to retrieve and verify the data ---

        // Get all *active* direct classifications for the asset
        List<AtlasClassification> directTags = tagDAO.getAllDirectTagsForVertex(assetId);
        assertEquals(1, directTags.size(), "Should find exactly one active direct tag.");

        AtlasClassification retrievedClassification = directTags.get(0);
        assertAll("Verify all properties of the retrieved direct tag",
                () -> assertEquals(expectedClassification.getTypeName(), retrievedClassification.getTypeName()),
                () -> assertEquals(expectedClassification.getEntityGuid(), retrievedClassification.getEntityGuid()),
                () -> assertEquals(expectedClassification.isPropagate(), retrievedClassification.isPropagate()),
                () -> assertEquals(expectedClassification.getAttributes(), retrievedClassification.getAttributes()),
                () -> assertEquals(expectedClassification.getEntityStatus(), retrievedClassification.getEntityStatus()),
                () -> assertEquals(expectedClassification.getRemovePropagationsOnEntityDelete(), retrievedClassification.getRemovePropagationsOnEntityDelete()),
                () -> assertEquals(expectedClassification.getRestrictPropagationThroughHierarchy(), retrievedClassification.getRestrictPropagationThroughHierarchy()),
                () -> assertEquals(expectedClassification.getRestrictPropagationThroughLineage(), retrievedClassification.getRestrictPropagationThroughLineage())
        );

        // Specifically check for propagated tags (should be none)
        List<AtlasClassification> propagatedTags = tagDAO.findByVertexIdAndPropagated(assetId);
        assertTrue(propagatedTags.isEmpty(), "Should find zero propagated tags.");
    }


    @Test
    void testPutAndGetDirectTag() throws AtlasBaseException {
        // --- Setup ---
        String assetId = "1001";
        String tagTypeName = "PII_CONFIDENTIAL";
        Map<String, Object> assetMetadata = createAssetMetadata("table_001", "default/db/table_001");
        AtlasClassification expectedTag = createClassification(tagTypeName, assetId);

        // --- Action ---
        tagDAO.putDirectTag(assetId, tagTypeName, expectedTag, assetMetadata);

        // --- Assertion 1: Test findDirectTagByVertexIdAndTagTypeName ---
        AtlasClassification retrievedClassification = tagDAO.findDirectTagByVertexIdAndTagTypeName(assetId, tagTypeName);

        assertNotNull(retrievedClassification, "The retrieved classification should not be null.");
        assertAll("Verify all properties of the retrieved AtlasClassification object",
                () -> assertEquals(expectedTag.getTypeName(), retrievedClassification.getTypeName()),
                () -> assertEquals(expectedTag.getEntityGuid(), retrievedClassification.getEntityGuid()),
                () -> assertEquals(expectedTag.isPropagate(), retrievedClassification.isPropagate()),
                () -> assertEquals(expectedTag.getAttributes(), retrievedClassification.getAttributes()),
                () -> assertEquals(expectedTag.getRemovePropagationsOnEntityDelete(), retrievedClassification.getRemovePropagationsOnEntityDelete()),
                () -> assertEquals(expectedTag.getRestrictPropagationThroughHierarchy(), retrievedClassification.getRestrictPropagationThroughHierarchy()),
                () -> assertEquals(expectedTag.getRestrictPropagationThroughLineage(), retrievedClassification.getRestrictPropagationThroughLineage())
        );

        // --- Assertion 2: Test findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata ---
        Tag retrievedTag = tagDAO.findDirectTagByVertexIdAndTagTypeNameWithAssetMetadata(assetId, tagTypeName);

        assertNotNull(retrievedTag, "The retrieved tag object should not be null.");
        assertEquals(assetId, retrievedTag.getVertexId(), "Vertex ID should match the asset ID.");
        assertEquals(tagTypeName, retrievedTag.getTagTypeName(), "Tag type name should match.");
        assertEquals(assetMetadata, retrievedTag.getAssetMetadata(), "Asset metadata should match.");
        assertNotNull(retrievedTag.getTagMetaJson(), "Tag metadata JSON should not be null.");

        // Convert the JSON map back to an AtlasClassification object for detailed comparison
        AtlasClassification retrievedMetaClassification = TagDAOCassandraImpl.toAtlasClassification(retrievedTag.getTagMetaJson());
        assertAll("Verify all properties of the classification stored in tag_meta_json",
                () -> assertEquals(expectedTag.getTypeName(), retrievedMetaClassification.getTypeName()),
                () -> assertEquals(expectedTag.getEntityGuid(), retrievedMetaClassification.getEntityGuid()),
                () -> assertEquals(expectedTag.isPropagate(), retrievedMetaClassification.isPropagate()),
                () -> assertEquals(expectedTag.getAttributes(), retrievedMetaClassification.getAttributes())
        );
    }

    @Test
    void testDeleteDirectTag() throws AtlasBaseException {
        // --- Setup ---
        String assetId = "1002";
        String tagTypeName = "SENSITIVE_DATA";
        Map<String, Object> assetMetadata = createAssetMetadata("table_002", "default/db/table_002");
        AtlasClassification tag = createClassification(tagTypeName, assetId);

        // --- Action ---
        tagDAO.putDirectTag(assetId, tagTypeName, tag, assetMetadata);
        tagDAO.deleteDirectTag(assetId, tag);

        // --- Assertion ---
        AtlasClassification activeTag = tagDAO.findDirectTagByVertexIdAndTagTypeName(assetId, tagTypeName);
        assertNull(activeTag, "Active tag should not be found after deletion.");

        AtlasClassification deletedTag = tagDAO.findDirectDeletedTagByVertexIdAndTagTypeName(assetId, tagTypeName);
        assertNotNull(deletedTag, "Deleted tag should be found.");
        assertAll("Verify properties of the soft-deleted tag",
                () -> assertEquals(tag.getTypeName(), deletedTag.getTypeName()),
                () -> assertEquals(tag.getEntityGuid(), deletedTag.getEntityGuid()),
                () -> assertEquals(tag.getAttributes(), deletedTag.getAttributes())
        );
    }

    @Test
    void testPutAndGetPropagatedTags() throws AtlasBaseException {
        // --- Setup ---
        String sourceAssetId = "2001";
        String tagTypeName = "FinanceDept";
        AtlasClassification expectedTag = createClassification(tagTypeName, sourceAssetId);

        Set<String> propagatedAssetIds = new HashSet<>(Arrays.asList("3001", "3002", "3003"));
        Map<String, Map<String, Object>> assetMinAttrsMap = new HashMap<>();
        assetMinAttrsMap.put("3001", createAssetMetadata("column1", "default/db/table/col1"));
        assetMinAttrsMap.put("3002", createAssetMetadata("column2", "default/db/table/col2"));
        assetMinAttrsMap.put("3003", createAssetMetadata("column3", "default/db/table/col3"));

        // --- Action ---
        tagDAO.putPropagatedTags(sourceAssetId, tagTypeName, propagatedAssetIds, assetMinAttrsMap, expectedTag);

        // --- Assertion 1: Verify tags exist on one of the propagated assets ---
        String testAssetId = "3002";
        List<AtlasClassification> classifications = tagDAO.getAllClassificationsForVertex(testAssetId);

        assertEquals(1, classifications.size(), "Should find one classification on the propagated asset.");

        AtlasClassification retrievedClassification = classifications.get(0);
        assertAll("Verify properties of the propagated classification",
                () -> assertEquals(expectedTag.getTypeName(), retrievedClassification.getTypeName()),
                () -> assertEquals(expectedTag.getEntityGuid(), retrievedClassification.getEntityGuid(), "Entity GUID should point to the source asset"),
                () -> assertEquals(expectedTag.isPropagate(), retrievedClassification.isPropagate()),
                () -> assertEquals(expectedTag.getAttributes(), retrievedClassification.getAttributes())
        );

        // --- Assertion 2: Verify all propagations can be retrieved from the source ---
        List<Tag> propagations = tagDAO.getTagPropagationsForAttachment(sourceAssetId, tagTypeName);
        assertEquals(3, propagations.size());

        Set<String> retrievedPropagatedIds = propagations.stream().map(Tag::getVertexId).collect(Collectors.toSet());
        assertEquals(propagatedAssetIds, retrievedPropagatedIds, "All propagated asset IDs should be retrieved.");

        // Check a specific retrieved tag in detail
        Tag specificPropagation = propagations.stream()
                .filter(p -> p.getVertexId().equals("3001"))
                .findFirst().orElse(null);
        assertNotNull(specificPropagation, "Propagation for asset 3001 should exist.");
        assertAll("Verify details of a specific propagated Tag object",
                () -> assertEquals(sourceAssetId, specificPropagation.getSourceVertexId()),
                () -> assertEquals(tagTypeName, specificPropagation.getTagTypeName()),
                () -> assertEquals(assetMinAttrsMap.get("3001"), specificPropagation.getAssetMetadata())
        );
    }

    @Test
    void testDeletePropagatedTags() throws AtlasBaseException {
        String sourceAssetId = "2002";
        String tagTypeName = "GDPR";
        AtlasClassification tag = createClassification(tagTypeName, sourceAssetId);

        Set<String> propagatedAssetIds = new HashSet<>(Arrays.asList("4001", "4002"));
        Map<String, Map<String, Object>> assetMinAttrsMap = new HashMap<>();
        assetMinAttrsMap.put("4001", createAssetMetadata("user_email", "q/db/t/user_email"));
        assetMinAttrsMap.put("4002", createAssetMetadata("user_address", "q/db/t/user_address"));

        // Action: Add tags, then create Tag objects to delete them
        tagDAO.putPropagatedTags(sourceAssetId, tagTypeName, propagatedAssetIds, assetMinAttrsMap, tag);

        List<Tag> tagsToDelete = new ArrayList<>();
        Tag tag1 = new Tag();
        tag1.setVertexId("4001");
        tag1.setSourceVertexId(sourceAssetId);
        tag1.setTagTypeName(tagTypeName);
        tag1.setPropagated(true);
        tag1.setBucket(TagDAOCassandraImpl.calculateBucket("4001"));
        tagsToDelete.add(tag1);

        tagDAO.deleteTags(tagsToDelete);

        // Assertion
        List<Tag> remainingPropagations = tagDAO.getTagPropagationsForAttachment(sourceAssetId, tagTypeName);
        assertEquals(1, remainingPropagations.size(), "Should only be one propagation remaining.");
        assertEquals("4002", remainingPropagations.get(0).getVertexId());

        List<AtlasClassification> deletedAssetTags = tagDAO.getAllClassificationsForVertex("4001");
        assertTrue(deletedAssetTags.isEmpty(), "Deleted asset should have no active tags.");
    }

    @Test
    void testPaginationForPropagations() throws AtlasBaseException {
        String sourceAssetId = "5000";
        String tagTypeName = "LegacyData";
        AtlasClassification tag = createClassification(tagTypeName, sourceAssetId);

        Set<String> propagatedAssetIds = new HashSet<>();
        Map<String, Map<String, Object>> assetMinAttrsMap = new HashMap<>();
        for (int i = 1; i <= 5; i++) {
            String assetId = "500" + i;
            propagatedAssetIds.add(assetId);
            assetMinAttrsMap.put(assetId, createAssetMetadata("asset_" + i, "q/db/t/asset" + i));
        }

        // Action: Add 5 propagated tags
        tagDAO.putPropagatedTags(sourceAssetId, tagTypeName, propagatedAssetIds, assetMinAttrsMap, tag);

        // Assertion: Fetch with pagination
        int pageSize = 2;
        String cacheKey = "test_pagination_key";

        // Page 1
        PaginatedTagResult page1 = tagDAO.getPropagationsForAttachmentBatchWithPagination(sourceAssetId, tagTypeName, null, pageSize, cacheKey);
        assertEquals(pageSize, page1.getTags().size());
        assertNotNull(page1.getPagingState());
        assertFalse(page1.isDone());

        // Page 2
        PaginatedTagResult page2 = tagDAO.getPropagationsForAttachmentBatchWithPagination(sourceAssetId, tagTypeName, page1.getPagingState(), pageSize, cacheKey);
        assertEquals(pageSize, page2.getTags().size());
        assertNotNull(page2.getPagingState());
        assertFalse(page2.isDone());

        // Page 3
        PaginatedTagResult page3 = tagDAO.getPropagationsForAttachmentBatchWithPagination(sourceAssetId, tagTypeName, page2.getPagingState(), pageSize, cacheKey);
        assertEquals(1, page3.getTags().size()); // Last page has the remainder
        assertTrue(page3.isDone(), "Paging should be done on the last page.");
    }

    @Test
    void testGetAllTagsByVertexId() throws AtlasBaseException {
        // --- Setup ---
        String assetId = "6001";
        String sourceAssetId = "7001";

        // Create and add a direct tag
        AtlasClassification directTag = createClassification("DIRECT_TAG", assetId);
        tagDAO.putDirectTag(assetId, directTag.getTypeName(), directTag, createAssetMetadata("asset_6001", "q/asset_6001"));
        Map<String, Object> expectedDirectTagMap = objectMapper.convertValue(directTag, new TypeReference<>() {});


        // Create and add a propagated tag from another source
        AtlasClassification propagatedTag = createClassification("PROPAGATED_TAG", sourceAssetId);
        tagDAO.putPropagatedTags(sourceAssetId, propagatedTag.getTypeName(), Collections.singleton(assetId),
                Collections.singletonMap(assetId, createAssetMetadata("asset_6001", "q/asset_6001")),
                propagatedTag);
        Map<String, Object> expectedPropagatedTagMap = objectMapper.convertValue(propagatedTag, new TypeReference<>() {});


        // --- Action ---
        List<Tag> retrievedTags = tagDAO.getAllTagsByVertexId(assetId);

        // --- Assertion ---
        assertEquals(2, retrievedTags.size(), "Should retrieve both direct and propagated tags.");

        Tag resultDirectTag = retrievedTags.stream().filter(t -> !t.isPropagated()).findFirst().orElse(null);
        Tag resultPropagatedTag = retrievedTags.stream().filter(Tag::isPropagated).findFirst().orElse(null);

        assertNotNull(resultDirectTag, "Direct tag should be present.");
        assertAll("Verify direct tag properties",
                () -> assertEquals(assetId, resultDirectTag.getVertexId()),
                () -> assertEquals("DIRECT_TAG", resultDirectTag.getTagTypeName()),
                () -> assertFalse(resultDirectTag.isPropagated()),
                () -> assertEquals(expectedDirectTagMap, resultDirectTag.getTagMetaJson())
        );

        assertNotNull(resultPropagatedTag, "Propagated tag should be present.");
        assertAll("Verify propagated tag properties",
                () -> assertEquals(assetId, resultPropagatedTag.getVertexId()),
                () -> assertEquals("PROPAGATED_TAG", resultPropagatedTag.getTagTypeName()),
                () -> assertTrue(resultPropagatedTag.isPropagated()),
                () -> assertEquals(expectedPropagatedTagMap, resultPropagatedTag.getTagMetaJson())
        );
    }

    @Test
    void testGetPropagationsForAttachmentWithSourceGuid() throws AtlasBaseException {
        // --- Setup ---
        String assetId = "8001";
        String sourceAssetId1 = "9001";
        String sourceAssetId2 = "9002";

        // Propagate a tag from source 1 to the asset
        AtlasClassification tagFromSource1 = createClassification("TAG_FROM_SOURCE_1", sourceAssetId1);
        tagDAO.putPropagatedTags(sourceAssetId1, tagFromSource1.getTypeName(), Collections.singleton(assetId),
                Collections.singletonMap(assetId, createAssetMetadata("asset_8001", "q/asset_8001")),
                tagFromSource1);

        // Propagate another tag from source 2 to the asset
        AtlasClassification tagFromSource2 = createClassification("TAG_FROM_SOURCE_2", sourceAssetId2);
        tagDAO.putPropagatedTags(sourceAssetId2, tagFromSource2.getTypeName(), Collections.singleton(assetId),
                Collections.singletonMap(assetId, createAssetMetadata("asset_8001", "q/asset_8001")),
                tagFromSource2);

        // --- Action ---
        // Retrieve only the propagations that originated from sourceAssetId1
        List<AtlasClassification> result = tagDAO.getPropagationsForAttachment(assetId, sourceAssetId1);

        // --- Assertion ---
        assertEquals(1, result.size(), "Should only retrieve the tag from the specified source GUID.");

        AtlasClassification retrievedTag = result.get(0);
        assertAll("Verify the correct propagated tag was filtered",
                () -> assertEquals("TAG_FROM_SOURCE_1", retrievedTag.getTypeName()),
                () -> assertEquals(sourceAssetId1, retrievedTag.getEntityGuid())
        );
    }

    // =================== Helper Methods ===================

    private AtlasClassification createClassification(String typeName, String entityGuid) {
        AtlasClassification classification = new AtlasClassification(typeName);
        classification.setEntityGuid(entityGuid);
        classification.setPropagate(true);
        classification.setRemovePropagationsOnEntityDelete(true);
        classification.setRestrictPropagationThroughLineage(false);
        classification.setRestrictPropagationThroughHierarchy(false);
        classification.setAttributes(Map.of("created_by", "test_user", "priority", "high"));
        return classification;
    }

    private Map<String, Object> createAssetMetadata(String name, String qualifiedName) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("name", name);
        metadata.put("qualifiedName", qualifiedName);
        metadata.put("__typeName", "Table");
        return metadata;
    }
}
