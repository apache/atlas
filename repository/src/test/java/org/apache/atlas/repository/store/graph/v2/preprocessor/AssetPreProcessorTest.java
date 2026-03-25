package org.apache.atlas.repository.store.graph.v2.preprocessor;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.store.UsersStore;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.Constants.ATTR_ADMIN_GROUPS;
import static org.apache.atlas.repository.Constants.ATTR_ADMIN_USERS;
import static org.apache.atlas.repository.Constants.ATTR_ANNOUNCEMENT_MESSAGE;
import static org.apache.atlas.repository.Constants.ATTR_OWNER_GROUPS;
import static org.apache.atlas.repository.Constants.ATTR_OWNER_USERS;
import static org.apache.atlas.repository.Constants.ATTR_VIEWER_GROUPS;
import static org.apache.atlas.repository.Constants.ATTR_VIEWER_USERS;
import static org.apache.atlas.repository.Constants.OWNER_ATTRIBUTE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class AssetPreProcessorTest {

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private EntityGraphRetriever entityRetriever;

    @Mock
    private AtlasGraph graph;

    @Mock
    private EntityMutationContext context;

    @Mock
    private EntityDiscoveryService discovery;

    @Mock
    private EntityGraphRetriever retrieverNoRelation;

    private AssetPreProcessor preProcessor;
    private RangerUserStore originalUserStore;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        
        // Create the validator with default UsersStore
        UserGroupAttributeValidator validator = new UserGroupAttributeValidator();
        preProcessor = new AssetPreProcessor(typeRegistry, entityRetriever, graph, discovery, retrieverNoRelation, validator);
        originalUserStore = UsersStore.getInstance().getUserStore();

        // Setup default valid users/groups
        RangerUserStore mockUserStore = mock(RangerUserStore.class);
        Map<String, Set<String>> userGroupMap = new HashMap<>();
        userGroupMap.put("validUser", Collections.emptySet());
        userGroupMap.put("adminUser", Collections.emptySet());

        Map<String, Map<String, String>> groupMap = new HashMap<>();
        groupMap.put("validGroup", new HashMap<>());
        groupMap.put("adminGroup", new HashMap<>());

        when(mockUserStore.getUserGroupMapping()).thenReturn(userGroupMap);
        when(mockUserStore.getGroupAttrMapping()).thenReturn(groupMap);

        UsersStore.getInstance().setUserStore(mockUserStore);
        RequestContext.clear();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        UsersStore.getInstance().setUserStore(originalUserStore);
        RequestContext.clear();
        if (closeable != null) {
            closeable.close();
        }
    }

    @Test
    public void testProcessAttributesValidUsersAndGroups() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(OWNER_ATTRIBUTE, "validUser");
        entity.setAttribute(ATTR_OWNER_GROUPS, List.of("validGroup"));
        entity.setAttribute(ATTR_ADMIN_USERS, List.of("adminUser"));
        entity.setAttribute(ATTR_ADMIN_GROUPS, List.of("adminGroup"));

        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertEquals(entity.getAttribute(OWNER_ATTRIBUTE), "validUser");
        assertTrue(((List<?>) entity.getAttribute(ATTR_OWNER_GROUPS)).contains("validGroup"));
        assertTrue(((List<?>) entity.getAttribute(ATTR_ADMIN_USERS)).contains("adminUser"));
    }

    @Test
    public void testProcessAttributesInvalidUserSkippedSilently() throws AtlasBaseException {
        // For backward compatibility, invalid/non-existent users are silently skipped
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(OWNER_ATTRIBUTE, "invalidUser");

        // Should not throw exception - invalid user is silently skipped
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        // Attribute value is preserved (validator only logs a warning)
        assertEquals(entity.getAttribute(OWNER_ATTRIBUTE), "invalidUser");
    }

    @Test
    public void testProcessAttributesInvalidGroupSkippedSilently() throws AtlasBaseException {
        // For backward compatibility, invalid/non-existent groups are silently skipped
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_OWNER_GROUPS, Arrays.asList("validGroup", "invalidGroup"));

        // Should not throw exception - invalid group is silently skipped
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        // Attribute values are preserved (validator only logs a warning)
        List<?> groups = (List<?>) entity.getAttribute(ATTR_OWNER_GROUPS);
        assertTrue(groups.contains("validGroup"));
        assertTrue(groups.contains("invalidGroup"));
    }

    @Test
    public void testSSIDetectionInAnnouncement() {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_ANNOUNCEMENT_MESSAGE, "Hello <!--#exec cmd=\"ls\" --> world");

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for SSI tag");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("SSI tags are not allowed"));
        }
    }

    @Test
    public void testAnnouncementMessageNonStringThrowsException() {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_ANNOUNCEMENT_MESSAGE, 123); // Non-string value

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for non-string announcementMessage");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("Invalid announcementMessage: must be string"));
        } catch (ClassCastException e) {
            fail("Should have thrown AtlasBaseException (BAD_REQUEST) but got ClassCastException");
        }
    }

    @Test
    public void testSSIDetectionInGroupName() {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        // Even if validation against store would fail it, security check comes first
        entity.setAttribute(ATTR_OWNER_GROUPS, Collections.singletonList("group<!--#exec-->"));

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for SSI tag in group name");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("SSI tags are not allowed"));
        }
    }

    @Test
    public void testXSSDetectionInUser() {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(OWNER_ATTRIBUTE, "<script>alert(1)</script>");

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for HTML chars");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("Special characters < > are not allowed"));
        }
    }

    @Test
    public void testURLDetectionInGroup() {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_OWNER_GROUPS, Collections.singletonList("http://malicious.com"));

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for URL");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("URLs are not allowed"));
        }
    }

    @Test
    public void testProcessAttributesWithNullValues() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(OWNER_ATTRIBUTE, null);
        entity.setAttribute(ATTR_OWNER_GROUPS, null);

        // Should not throw exception
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertNull(entity.getAttribute(OWNER_ATTRIBUTE));
        assertNull(entity.getAttribute(ATTR_OWNER_GROUPS));
    }

    @Test
    public void testProcessAttributesSingleValueInvalidUserPreservesValue() throws AtlasBaseException {
        // For backward compatibility, invalid users are skipped silently and values preserved
        AtlasEntity entity = new AtlasEntity();
        String invalidUser = "nonExistentUser";
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(OWNER_ATTRIBUTE, invalidUser);

        // Should not throw exception - invalid user is silently skipped
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        // Verify that the attribute value is preserved
        assertEquals(entity.getAttribute(OWNER_ATTRIBUTE), invalidUser, "Attribute value should be preserved");
    }

    @Test
    public void testEmptyAnnouncementMessageDoesNotThrow() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_ANNOUNCEMENT_MESSAGE, "");

        // Should not throw exception for empty message
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertEquals(entity.getAttribute(ATTR_ANNOUNCEMENT_MESSAGE), "");
    }

    @Test
    public void testValidAnnouncementMessagePasses() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_ANNOUNCEMENT_MESSAGE, "This is a valid announcement message.");

        // Should not throw exception for valid message
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertEquals(entity.getAttribute(ATTR_ANNOUNCEMENT_MESSAGE), "This is a valid announcement message.");
    }

    @Test
    public void testOwnerUsersAttributeValidation() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_OWNER_USERS, List.of("validUser"));

        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertTrue(((List<?>) entity.getAttribute(ATTR_OWNER_USERS)).contains("validUser"));
    }

    @Test
    public void testViewerGroupsAndUsersValidation() throws AtlasBaseException {
        // Setup additional valid viewer users/groups
        RangerUserStore mockUserStore = mock(RangerUserStore.class);
        Map<String, Set<String>> userGroupMap = new HashMap<>();
        userGroupMap.put("validUser", Collections.emptySet());
        userGroupMap.put("viewerUser", Collections.emptySet());

        Map<String, Map<String, String>> groupMap = new HashMap<>();
        groupMap.put("validGroup", new HashMap<>());
        groupMap.put("viewerGroup", new HashMap<>());

        when(mockUserStore.getUserGroupMapping()).thenReturn(userGroupMap);
        when(mockUserStore.getGroupAttrMapping()).thenReturn(groupMap);
        UsersStore.getInstance().setUserStore(mockUserStore);

        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_VIEWER_USERS, List.of("viewerUser"));
        entity.setAttribute(ATTR_VIEWER_GROUPS, List.of("viewerGroup"));

        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertTrue(((List<?>) entity.getAttribute(ATTR_VIEWER_USERS)).contains("viewerUser"));
        assertTrue(((List<?>) entity.getAttribute(ATTR_VIEWER_GROUPS)).contains("viewerGroup"));
    }

    @Test
    public void testInvalidViewerGroupSkippedSilently() throws AtlasBaseException {
        // For backward compatibility, invalid/non-existent groups are silently skipped
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_VIEWER_GROUPS, List.of("invalidViewerGroup"));

        // Should not throw exception - invalid group is silently skipped
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        // Attribute value is preserved (validator only logs a warning)
        assertTrue(((List<?>) entity.getAttribute(ATTR_VIEWER_GROUPS)).contains("invalidViewerGroup"));
    }

    @Test
    public void testEmptyGroupMappingSkipsExistenceCheck() throws AtlasBaseException {
        // Simulate a scenario where Heracles API failed and groupAttrMapping is empty
        RangerUserStore mockUserStore = mock(RangerUserStore.class);
        Map<String, Set<String>> userGroupMap = new HashMap<>();
        userGroupMap.put("validUser", Collections.emptySet());

        // Empty group mapping - simulates failed Heracles API load
        Map<String, Map<String, String>> emptyGroupMap = new HashMap<>();

        when(mockUserStore.getUserGroupMapping()).thenReturn(userGroupMap);
        when(mockUserStore.getGroupAttrMapping()).thenReturn(emptyGroupMap);
        UsersStore.getInstance().setUserStore(mockUserStore);

        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_OWNER_GROUPS, List.of("someGroup"));

        // Should NOT throw exception - validation should be skipped when group mapping is empty
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        // The group should be preserved since existence check is skipped
        assertTrue(((List<?>) entity.getAttribute(ATTR_OWNER_GROUPS)).contains("someGroup"));
    }

    @Test
    public void testEmptyUserMappingSkipsExistenceCheck() throws AtlasBaseException {
        // Simulate a scenario where user loading failed and userGroupMapping is empty
        RangerUserStore mockUserStore = mock(RangerUserStore.class);

        // Empty user mapping - simulates failed user load
        Map<String, Set<String>> emptyUserMap = new HashMap<>();

        Map<String, Map<String, String>> groupMap = new HashMap<>();
        groupMap.put("validGroup", new HashMap<>());

        when(mockUserStore.getUserGroupMapping()).thenReturn(emptyUserMap);
        when(mockUserStore.getGroupAttrMapping()).thenReturn(groupMap);
        UsersStore.getInstance().setUserStore(mockUserStore);

        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(OWNER_ATTRIBUTE, "someUser");

        // Should NOT throw exception - validation should be skipped when user mapping is empty
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        // The user should be preserved since existence check is skipped
        assertEquals(entity.getAttribute(OWNER_ATTRIBUTE), "someUser");
    }

    @Test
    public void testEmptyMappingStillRejectsSSITags() {
        // Even when mappings are empty, security checks should still work
        RangerUserStore mockUserStore = mock(RangerUserStore.class);
        Map<String, Set<String>> emptyUserMap = new HashMap<>();
        Map<String, Map<String, String>> emptyGroupMap = new HashMap<>();

        when(mockUserStore.getUserGroupMapping()).thenReturn(emptyUserMap);
        when(mockUserStore.getGroupAttrMapping()).thenReturn(emptyGroupMap);
        UsersStore.getInstance().setUserStore(mockUserStore);

        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_OWNER_GROUPS, List.of("group<!--#exec-->"));

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for SSI tag even with empty mapping");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("SSI tags are not allowed"));
        }
    }

    // =====================================================================================
    // Dataset Linking Tests
    // =====================================================================================

    @Test
    public void testDatasetLinkAttribute_emptyGuidSkipsValidation() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute(QUALIFIED_NAME, "test-table");
        entity.setAttribute("catalogDatasetGuid", ""); // Empty string

        // Should not throw exception - empty GUID is ignored
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertEquals("", entity.getAttribute("catalogDatasetGuid"));
    }

    @Test
    public void testDatasetLinkAttribute_nullGuidSkipsValidation() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute(QUALIFIED_NAME, "test-table");
        entity.setAttribute("catalogDatasetGuid", null); // Null value

        // Should not throw exception - null GUID is ignored
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertNull(entity.getAttribute("catalogDatasetGuid"));
    }

    @Test
    public void testDatasetLinkAttribute_missingAttributeSkipsValidation() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute(QUALIFIED_NAME, "test-table");
        // Don't set catalogDatasetGuid at all

        // Should not throw exception - missing attribute is ignored
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertNull(entity.getAttribute("catalogDatasetGuid"));
    }

    @Test
    public void testDatasetLinkAttribute_wrongEntityTypeThrows() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute(QUALIFIED_NAME, "test-table");
        entity.setAttribute("catalogDatasetGuid", "wrong-type-guid");

        // Mock entityRetriever to return a DataDomain vertex instead of Dataset
        org.apache.atlas.repository.graphdb.AtlasVertex wrongTypeVertex =
            mock(org.apache.atlas.repository.graphdb.AtlasVertex.class);
        when(wrongTypeVertex.getProperty("__typeName", String.class)).thenReturn("DataDomain");
        when(entityRetriever.getEntityVertex("wrong-type-guid")).thenReturn(wrongTypeVertex);

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for catalogDatasetGuid pointing to wrong entity type");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARAMETERS);
            assertTrue(e.getMessage().contains("Asset can be linked to only a Dataset entity"),
                    "Expected message about Dataset linking, got: " + e.getMessage());
        }
    }

    @Test
    public void testDatasetLinkAttribute_nonExistentGuidThrows() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute(QUALIFIED_NAME, "test-table");
        entity.setAttribute("catalogDatasetGuid", "non-existent-guid");

        // Mock entityRetriever to return null (entity not found)
        when(entityRetriever.getEntityVertex("non-existent-guid")).thenReturn(null);

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for non-existent catalogDatasetGuid");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INSTANCE_GUID_NOT_FOUND);
        }
    }

    @Test
    public void testDatasetLinkAttribute_excludedTypesCannotSetGuid_DataProduct() {
        AtlasEntity entity = new AtlasEntity("DataProduct");
        entity.setAttribute(QUALIFIED_NAME, "test-product");
        entity.setAttribute("catalogDatasetGuid", "some-dataset-guid");

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("DataProduct should not be allowed to link to Dataset");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARAMETERS);
            assertTrue(e.getMessage().contains("DataProduct is not allowed to link with Dataset"));
        }
    }

    @Test
    public void testDatasetLinkAttribute_excludedTypesCannotSetGuid_DataDomain() {
        AtlasEntity entity = new AtlasEntity("DataDomain");
        entity.setAttribute(QUALIFIED_NAME, "test-domain");
        entity.setAttribute("catalogDatasetGuid", "some-dataset-guid");

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("DataDomain should not be allowed to link to Dataset");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARAMETERS);
            assertTrue(e.getMessage().contains("DataDomain is not allowed to link with Dataset"));
        }
    }

    @Test
    public void testDatasetLinkAttribute_excludedTypesCannotSetGuid_DatasetSelfLink() {
        AtlasEntity entity = new AtlasEntity("DataMeshDataset");
        entity.setAttribute(QUALIFIED_NAME, "test-dataset");
        entity.setAttribute("catalogDatasetGuid", "another-dataset-guid");

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Dataset should not be allowed to link to another Dataset");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARAMETERS);
            assertTrue(e.getMessage().contains("Dataset is not allowed to link with Dataset"));
        }
    }

    @Test
    public void testDatasetLinkAttribute_excludedTypesCannotSetGuid_GlossaryTerm() {
        AtlasEntity entity = new AtlasEntity("AtlasGlossaryTerm");
        entity.setAttribute(QUALIFIED_NAME, "test-term");
        entity.setAttribute("catalogDatasetGuid", "some-dataset-guid");

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("AtlasGlossaryTerm should not be allowed to link to Dataset");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARAMETERS);
            assertTrue(e.getMessage().contains("AtlasGlossaryTerm is not allowed to link with Dataset"));
        }
    }

    @Test
    public void testDatasetLinkAttribute_excludedTypesCannotSetGuid_GlossaryCategory() {
        AtlasEntity entity = new AtlasEntity("AtlasGlossaryCategory");
        entity.setAttribute(QUALIFIED_NAME, "test-category");
        entity.setAttribute("catalogDatasetGuid", "some-dataset-guid");

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("AtlasGlossaryCategory should not be allowed to link to Dataset");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARAMETERS);
            assertTrue(e.getMessage().contains("AtlasGlossaryCategory is not allowed to link with Dataset"));
        }
    }

    @Test
    public void testDatasetLinkAttribute_excludedTypesCannotSetGuid_Glossary() {
        AtlasEntity entity = new AtlasEntity("AtlasGlossary");
        entity.setAttribute(QUALIFIED_NAME, "test-glossary");
        entity.setAttribute("catalogDatasetGuid", "some-dataset-guid");

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("AtlasGlossary should not be allowed to link to Dataset");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARAMETERS);
            assertTrue(e.getMessage().contains("AtlasGlossary is not allowed to link with Dataset"));
        }
    }

    @Test
    public void testDatasetLinkAttribute_validGuidPasses() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute(QUALIFIED_NAME, "test-table");
        entity.setAttribute("catalogDatasetGuid", "valid-dataset-guid");

        // Mock entityRetriever to return a valid Dataset vertex
        org.apache.atlas.repository.graphdb.AtlasVertex datasetVertex =
            mock(org.apache.atlas.repository.graphdb.AtlasVertex.class);
        when(datasetVertex.getProperty("__typeName", String.class)).thenReturn("DataMeshDataset");
        when(entityRetriever.getEntityVertex("valid-dataset-guid")).thenReturn(datasetVertex);

        // Should not throw exception - valid Dataset GUID
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertEquals("valid-dataset-guid", entity.getAttribute("catalogDatasetGuid"));
    }

    @Test
    public void testValidateLinkedEntity_refactoredDomainValidationRegression() throws AtlasBaseException {
        // Test that the refactored validateLinkedEntity method still works for domain linking
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute(QUALIFIED_NAME, "test-table");
        entity.setAttribute("__AtlasDomainGuids", Arrays.asList("valid-domain-guid"));

        // Mock entityRetriever to return a valid DataDomain vertex
        org.apache.atlas.repository.graphdb.AtlasVertex domainVertex =
            mock(org.apache.atlas.repository.graphdb.AtlasVertex.class);
        when(domainVertex.getProperty("__typeName", String.class)).thenReturn("DataDomain");
        when(entityRetriever.getEntityVertex("valid-domain-guid")).thenReturn(domainVertex);

        // Should not throw exception - the refactored method should still work for domains
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertTrue(((List<?>) entity.getAttribute("__AtlasDomainGuids")).contains("valid-domain-guid"));
    }

    @Test
    public void testDatasetLinkAttribute_onUpdateOperation() throws AtlasBaseException {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setGuid("table-guid-123");
        entity.setAttribute(QUALIFIED_NAME, "test-table");
        entity.setAttribute("catalogDatasetGuid", "valid-dataset-guid");

        // Mock the asset vertex
        org.apache.atlas.repository.graphdb.AtlasVertex assetVertex =
            mock(org.apache.atlas.repository.graphdb.AtlasVertex.class);
        when(assetVertex.getProperty("__typeName", String.class)).thenReturn("Table");
        when(context.getVertex(entity.getGuid())).thenReturn(assetVertex);

        // Mock entityRetriever to return a valid Dataset vertex
        org.apache.atlas.repository.graphdb.AtlasVertex datasetVertex =
            mock(org.apache.atlas.repository.graphdb.AtlasVertex.class);
        when(datasetVertex.getProperty("__typeName", String.class)).thenReturn("DataMeshDataset");
        when(entityRetriever.getEntityVertex("valid-dataset-guid")).thenReturn(datasetVertex);

        // Mock retrieverNoRelation to return an entity header for authorization check (UPDATE path)
        org.apache.atlas.model.instance.AtlasEntityHeader mockHeader =
            new org.apache.atlas.model.instance.AtlasEntityHeader("Table");
        mockHeader.setGuid("table-guid-123");
        mockHeader.setAttribute("name", "test-table");
        mockHeader.setAttribute(QUALIFIED_NAME, "test-table");
        when(retrieverNoRelation.toAtlasEntityHeaderWithClassifications(assetVertex))
            .thenReturn(mockHeader);

        // Should work for UPDATE operation as well
        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.UPDATE);

        assertEquals("valid-dataset-guid", entity.getAttribute("catalogDatasetGuid"));
    }

    @Test
    public void testDatasetLinkAttribute_multipleDifferentAssetsCanLinkToSameDataset() throws AtlasBaseException {
        // Test that multiple assets can link to the same dataset (N:1 relationship)
        String sharedDatasetGuid = "shared-dataset-guid";

        // Mock entityRetriever to return a valid Dataset vertex
        org.apache.atlas.repository.graphdb.AtlasVertex datasetVertex =
            mock(org.apache.atlas.repository.graphdb.AtlasVertex.class);
        when(datasetVertex.getProperty("__typeName", String.class)).thenReturn("DataMeshDataset");
        when(entityRetriever.getEntityVertex(sharedDatasetGuid)).thenReturn(datasetVertex);

        // First asset
        AtlasEntity asset1 = new AtlasEntity("Table");
        asset1.setAttribute(QUALIFIED_NAME, "test-table-1");
        asset1.setAttribute("catalogDatasetGuid", sharedDatasetGuid);
        preProcessor.processAttributes(asset1, context, EntityMutations.EntityOperation.CREATE);

        // Second asset
        AtlasEntity asset2 = new AtlasEntity("Table");
        asset2.setAttribute(QUALIFIED_NAME, "test-table-2");
        asset2.setAttribute("catalogDatasetGuid", sharedDatasetGuid);
        preProcessor.processAttributes(asset2, context, EntityMutations.EntityOperation.CREATE);

        // Both should succeed
        assertEquals(sharedDatasetGuid, asset1.getAttribute("catalogDatasetGuid"));
        assertEquals(sharedDatasetGuid, asset2.getAttribute("catalogDatasetGuid"));
    }
}
