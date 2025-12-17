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

import static org.apache.atlas.repository.Constants.ATTR_ADMIN_GROUPS;
import static org.apache.atlas.repository.Constants.ATTR_ADMIN_USERS;
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
        preProcessor = new AssetPreProcessor(typeRegistry, entityRetriever, graph, discovery, retrieverNoRelation);
        originalUserStore = UsersStore.getInstance().getUserStore();

        // Setup default valid users/groups
        RangerUserStore mockUserStore = mock(RangerUserStore.class);
        Map<String, Map<String, String>> userMap = new HashMap<>();
        userMap.put("validUser", new HashMap<>());
        userMap.put("adminUser", new HashMap<>());

        Map<String, Map<String, String>> groupMap = new HashMap<>();
        groupMap.put("validGroup", new HashMap<>());
        groupMap.put("adminGroup", new HashMap<>());

        when(mockUserStore.getUserAttrMapping()).thenReturn(userMap);
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
        entity.setAttribute("ownerGroups", Arrays.asList("validGroup"));
        entity.setAttribute(ATTR_ADMIN_USERS, Arrays.asList("adminUser"));
        entity.setAttribute(ATTR_ADMIN_GROUPS, Arrays.asList("adminGroup"));

        preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);

        assertEquals(entity.getAttribute(OWNER_ATTRIBUTE), "validUser");
        assertTrue(((List<?>) entity.getAttribute("ownerGroups")).contains("validGroup"));
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
        entity.setAttribute("ownerGroups", Arrays.asList("validGroup", "invalidGroup"));

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
        entity.setAttribute("announcementMessage", "Hello <!--#exec cmd=\"ls\" --> world");

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for SSI tag");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("SSI tags are not allowed"));
        }
    }

    @Test
    public void testSSIDetectionInGroupName() {
        AtlasEntity entity = new AtlasEntity();
        entity.setAttribute(QUALIFIED_NAME, "test-asset");
        // Even if validation against store would fail it, security check comes first
        entity.setAttribute("ownerGroups", Collections.singletonList("group<!--#exec-->"));

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
        entity.setAttribute("ownerGroups", Collections.singletonList("http://malicious.com"));

        try {
            preProcessor.processAttributes(entity, context, EntityMutations.EntityOperation.CREATE);
            fail("Should have thrown exception for URL");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
            assertTrue(e.getMessage().contains("URLs are not allowed"));
        }
    }
}

