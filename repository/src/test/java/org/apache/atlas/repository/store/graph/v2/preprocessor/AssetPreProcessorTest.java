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
    public void testProcessAttributesInvalidUserThrowsException() {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(OWNER_ATTRIBUTE, "invalidUser");

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for invalid user");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("Invalid user name: invalidUser"));
        }
    }

    @Test
    public void testProcessAttributesInvalidGroupThrowsException() {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_OWNER_GROUPS, Arrays.asList("validGroup", "invalidGroup"));

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for invalid group");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("Invalid group name: invalidGroup"));
        }
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
    public void testProcessAttributesSingleValueInvalidUserPreservesValue() {
        AtlasEntity entity = new AtlasEntity();
        String invalidUser = "nonExistentUser";
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(OWNER_ATTRIBUTE, invalidUser);

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for invalid user");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            // Verify that the attribute value is preserved and NOT set to null
            assertEquals(entity.getAttribute(OWNER_ATTRIBUTE), invalidUser, "Attribute value should be preserved even if invalid");
        }
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
    public void testInvalidViewerGroupThrowsException() {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        entity.setAttribute(ATTR_VIEWER_GROUPS, List.of("invalidViewerGroup"));

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for invalid viewer group");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("Invalid group name: invalidViewerGroup"));
        }
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
}
