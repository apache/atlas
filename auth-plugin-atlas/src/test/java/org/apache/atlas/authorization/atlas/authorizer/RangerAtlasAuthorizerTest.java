package org.apache.atlas.authorization.atlas.authorizer;

import org.apache.atlas.authorize.AtlasAccessResult;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.authorize.AtlasTypesDefFilterRequest;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.atlas.plugin.policyengine.RangerAccessResult;
import org.apache.atlas.plugin.service.RangerBasePlugin;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RangerAtlasAuthorizer} null-safety when the Ranger policy engine
 * fails to initialize (e.g., missing service-def, unreachable Ranger admin).
 *
 * The core invariant: isAccessAllowed() must NEVER return null.
 * When the policy engine cannot evaluate a request, it should return a DENY result.
 */
public class RangerAtlasAuthorizerTest {

    private RangerAtlasAuthorizer authorizer;
    private RangerBasePlugin originalPlugin;
    private RangerGroupUtil originalGroupUtil;

    @BeforeEach
    void setUp() throws Exception {
        authorizer = new RangerAtlasAuthorizer();
        originalPlugin = getStaticField("atlasPlugin");
        originalGroupUtil = getStaticField("groupUtil");
    }

    @AfterEach
    void tearDown() throws Exception {
        setStaticField("atlasPlugin", originalPlugin);
        setStaticField("groupUtil", originalGroupUtil);
    }

    // ========================================================================
    // Scenario 1: Plugin is null (never initialized)
    // ========================================================================

    @Test
    @DisplayName("Admin access returns deny when plugin is null")
    void adminAccess_pluginNull_returnsDeny() throws Exception {
        setStaticField("atlasPlugin", null);

        AtlasAdminAccessRequest request = new AtlasAdminAccessRequest(
                AtlasPrivilege.ADMIN_IMPORT, "testUser", Collections.emptySet());

        AtlasAccessResult result = authorizer.isAccessAllowed(request);

        assertNotNull(result, "isAccessAllowed must never return null");
        assertFalse(result.isAllowed(), "Access must be denied when plugin is null");
    }

    @Test
    @DisplayName("Type access returns deny when plugin is null")
    void typeAccess_pluginNull_returnsDeny() throws Exception {
        setStaticField("atlasPlugin", null);

        AtlasTypeAccessRequest request = new AtlasTypeAccessRequest(
                AtlasPrivilege.TYPE_READ, new AtlasEntityDef("TestType"),
                "testUser", Collections.emptySet());

        AtlasAccessResult result = authorizer.isAccessAllowed(request);

        assertNotNull(result, "isAccessAllowed must never return null");
        assertFalse(result.isAllowed(), "Access must be denied when plugin is null");
    }

    @Test
    @DisplayName("Entity access returns deny when plugin is null")
    void entityAccess_pluginNull_returnsDeny() throws Exception {
        setStaticField("atlasPlugin", null);

        AtlasEntityHeader entityHeader = new AtlasEntityHeader("Table");
        entityHeader.setGuid("test-guid");
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(
                typeRegistry, AtlasPrivilege.ENTITY_READ, entityHeader,
                "testUser", Collections.emptySet());

        AtlasAccessResult result = authorizer.isAccessAllowed(request, false);

        assertNotNull(result, "isAccessAllowed must never return null");
        assertFalse(result.isAllowed(), "Access must be denied when plugin is null");
    }

    // ========================================================================
    // Scenario 2: Plugin exists but returns null result (no policy engine)
    // ========================================================================

    @Test
    @DisplayName("Admin access returns deny when policy engine returns null")
    void adminAccess_policyEngineNull_returnsDeny() throws Exception {
        RangerBasePlugin mockPlugin = setupMockPlugin();
        when(mockPlugin.isAccessAllowed(any(RangerAccessRequestImpl.class)))
                .thenReturn(null);

        AtlasAdminAccessRequest request = new AtlasAdminAccessRequest(
                AtlasPrivilege.ADMIN_IMPORT, "testUser", Collections.emptySet());

        AtlasAccessResult result = authorizer.isAccessAllowed(request);

        assertNotNull(result, "isAccessAllowed must never return null when policy engine returns null");
        assertFalse(result.isAllowed(), "Access must be denied when policy engine returns null");
    }

    @Test
    @DisplayName("Type access returns deny when policy engine returns null")
    void typeAccess_policyEngineNull_returnsDeny() throws Exception {
        RangerBasePlugin mockPlugin = setupMockPlugin();
        when(mockPlugin.isAccessAllowed(any(RangerAccessRequestImpl.class)))
                .thenReturn(null);

        AtlasTypeAccessRequest request = new AtlasTypeAccessRequest(
                AtlasPrivilege.TYPE_READ, new AtlasEntityDef("TestType"),
                "testUser", Collections.emptySet());

        AtlasAccessResult result = authorizer.isAccessAllowed(request);

        assertNotNull(result, "isAccessAllowed must never return null when policy engine returns null");
        assertFalse(result.isAllowed(), "Access must be denied when policy engine returns null");
    }

    @Test
    @DisplayName("Entity access returns deny when policy engine returns null (audit handler overload)")
    void entityAccess_policyEngineNull_returnsDeny() throws Exception {
        RangerBasePlugin mockPlugin = setupMockPlugin();
        when(mockPlugin.isAccessAllowed(any(RangerAccessRequestImpl.class), any()))
                .thenReturn(null);

        AtlasEntityHeader entityHeader = new AtlasEntityHeader("Table");
        entityHeader.setGuid("test-guid");
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(
                typeRegistry, AtlasPrivilege.ENTITY_READ, entityHeader,
                "testUser", Collections.emptySet());

        AtlasAccessResult result = authorizer.isAccessAllowed(request, false);

        assertNotNull(result, "isAccessAllowed must never return null when policy engine returns null");
        assertFalse(result.isAllowed(), "Access must be denied when policy engine returns null");
    }

    // ========================================================================
    // Scenario 3: filterTypesDef should not NPE when policy engine returns null
    // ========================================================================

    @Test
    @DisplayName("filterTypesDef does not throw NPE, filters all types as denied")
    void filterTypesDef_policyEngineNull_doesNotThrowNPE() throws Exception {
        RangerBasePlugin mockPlugin = setupMockPlugin();
        when(mockPlugin.isAccessAllowed(any(RangerAccessRequestImpl.class)))
                .thenReturn(null);

        AtlasTypesDef typesDef = new AtlasTypesDef();
        List<AtlasEntityDef> entityDefs = new ArrayList<>();
        entityDefs.add(new AtlasEntityDef("Table"));
        entityDefs.add(new AtlasEntityDef("Column"));
        typesDef.setEntityDefs(entityDefs);

        AtlasTypesDefFilterRequest request = new AtlasTypesDefFilterRequest(
                typesDef, "testUser", Collections.emptySet());

        assertDoesNotThrow(() -> authorizer.filterTypesDef(request),
                "filterTypesDef must not throw NPE when policy engine returns null");

        assertTrue(typesDef.getEntityDefs().isEmpty(),
                "All types should be filtered out when policy engine returns null (deny)");
    }

    // ========================================================================
    // Scenario 4: Normal flow - plugin returns allow
    // ========================================================================

    @Test
    @DisplayName("Admin access allowed when policy engine allows")
    void adminAccess_policyEngineAllows_returnsAllow() throws Exception {
        RangerBasePlugin mockPlugin = setupMockPlugin();
        RangerAccessResult mockRangerResult = mock(RangerAccessResult.class);
        when(mockRangerResult.getIsAllowed()).thenReturn(true);
        when(mockRangerResult.getPolicyId()).thenReturn("policy-1");
        when(mockRangerResult.getPolicyPriority()).thenReturn(0);
        when(mockPlugin.isAccessAllowed(any(RangerAccessRequestImpl.class)))
                .thenReturn(mockRangerResult);

        AtlasAdminAccessRequest request = new AtlasAdminAccessRequest(
                AtlasPrivilege.ADMIN_IMPORT, "testUser", Collections.emptySet());

        AtlasAccessResult result = authorizer.isAccessAllowed(request);

        assertNotNull(result);
        assertTrue(result.isAllowed(), "Access must be allowed when policy engine allows");
    }

    @Test
    @DisplayName("Type access allowed when policy engine allows")
    void typeAccess_policyEngineAllows_returnsAllow() throws Exception {
        RangerBasePlugin mockPlugin = setupMockPlugin();
        RangerAccessResult mockRangerResult = mock(RangerAccessResult.class);
        when(mockRangerResult.getIsAllowed()).thenReturn(true);
        when(mockRangerResult.getPolicyId()).thenReturn("policy-1");
        when(mockRangerResult.getPolicyPriority()).thenReturn(0);
        // TYPE_READ routes to checkAccess(request, null) — the two-arg overload
        when(mockPlugin.isAccessAllowed(any(RangerAccessRequestImpl.class), any()))
                .thenReturn(mockRangerResult);

        AtlasTypeAccessRequest request = new AtlasTypeAccessRequest(
                AtlasPrivilege.TYPE_READ, new AtlasEntityDef("TestType"),
                "testUser", Collections.emptySet());

        AtlasAccessResult result = authorizer.isAccessAllowed(request);

        assertNotNull(result);
        assertTrue(result.isAllowed(), "Access must be allowed when policy engine allows");
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    private RangerBasePlugin setupMockPlugin() throws Exception {
        RangerBasePlugin mockPlugin = Mockito.mock(RangerBasePlugin.class);
        RangerGroupUtil mockGroupUtil = Mockito.mock(RangerGroupUtil.class);
        RangerUserStore mockUserStore = mock(RangerUserStore.class);

        when(mockUserStore.getUserGroupMapping()).thenReturn(new HashMap<>());
        when(mockPlugin.getUserStore()).thenReturn(mockUserStore);
        lenient().when(mockGroupUtil.getContainedGroups(any())).thenReturn(new HashSet<>());

        setStaticField("atlasPlugin", mockPlugin);
        setStaticField("groupUtil", mockGroupUtil);

        return mockPlugin;
    }

    @SuppressWarnings("unchecked")
    private <T> T getStaticField(String fieldName) throws Exception {
        Field field = RangerAtlasAuthorizer.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(null);
    }

    private void setStaticField(String fieldName, Object value) throws Exception {
        Field field = RangerAtlasAuthorizer.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, value);
    }
}
