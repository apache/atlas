/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.type.RenamePropagationTarget;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link EntityRenameHandler}.
 *
 * <p>Tests are split into two groups:
 * <ol>
 *   <li><b>Pure-logic tests</b>: {@code parseUniqueAttribute}, {@code buildUniqueAttribute},
 *       {@code recomputeUniqueAttribute}, {@code buildMappedAttrs}, and {@code findQualifiedNameOverrideKey}
 *       are private methods
 *       exercised directly via reflection — no graph or type-registry mocking required.</li>
 *   <li><b>Integration tests</b>: {@code addDependentsToContext} (and its private
 *       {@code collectDependents} worker) are exercised end-to-end by mocking the graph
 *       layer, the type registry, and the static helpers in {@link GraphHelper} and
 *       {@link AtlasGraphUtilsV2}.</li>
 * </ol>
 */
public class EntityRenameHandlerTest {
    // ─── shared mocks ────────────────────────────────────────────────────────────

    @Mock private AtlasTypeRegistry typeRegistry;

    private EntityRenameHandler handler;

    private MockedStatic<GraphHelper>       graphHelperMock;
    private MockedStatic<AtlasGraphUtilsV2> graphUtilsMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        handler = new EntityRenameHandler(typeRegistry);
    }

    @AfterMethod
    public void tearDown() {
        if (graphHelperMock != null) {
            graphHelperMock.close();
            graphHelperMock = null;
        }

        if (graphUtilsMock != null) {
            graphUtilsMock.close();
            graphUtilsMock = null;
        }
    }

    // =========================================================================
    // 1. parseUniqueAttribute — pure parsing logic
    // =========================================================================

    @DataProvider(name = "parseUniqueAttributeData")
    public Object[][] parseUniqueAttributeData() {
        return new Object[][] {
            // template, uniqueAttr, expectedSlots
            {
                "{db.name}.{name}@{clusterName}",
                "mydb.mytable@cluster1",
                map("db.name", "mydb", "name", "mytable", "clusterName", "cluster1")
            },
            {
                "{name}",
                "standalone",
                map("name", "standalone")
            },
            {
                "{db.name}.{table.name}.{name}@{clusterName}",
                "db1.tbl1.col1@prod",
                map("db.name", "db1", "table.name", "tbl1", "name", "col1", "clusterName", "prod")
            },
            {
                "{catalog.name}.{schema.name}.{name}@{clusterName}",
                "cat1.sch1.tbl1@prod",
                map("catalog.name", "cat1", "schema.name", "sch1", "name", "tbl1", "clusterName", "prod")
            },
        };
    }

    @Test(dataProvider = "parseUniqueAttributeData")
    public void parseUniqueAttribute_standardTemplates_parsedCorrectly(
            String template, String uniqueAttr, Map<String, String> expectedSlots) throws Exception {
        Map<String, String> result = invokeParseUniqueAttribute(uniqueAttr, template);

        assertEquals(result, expectedSlots);
    }

    @Test
    public void parseUniqueAttribute_delimiterNotFound_capturesRemainder() throws Exception {
        // Template expects '@' after {name} but the QN string has no '@'.
        Map<String, String> result = invokeParseUniqueAttribute("mydb.orphan", "{db.name}.{name}@{clusterName}");

        assertEquals(result.get("db.name"), "mydb");
        assertEquals(result.get("name"), "orphan");
    }

    @Test
    public void parseUniqueAttribute_malformedTemplate_stopsParsingGracefully() throws Exception {
        // Missing closing brace — should not throw, just stop parsing.
        Map<String, String> result = invokeParseUniqueAttribute("mydb.mytable", "{db.name}.{name");

        assertEquals(result.get("db.name"), "mydb");
        assertTrue(!result.containsKey("name"));
    }

    // =========================================================================
    // 2. buildUniqueAttribute — pure rebuild logic
    // =========================================================================

    @Test
    public void buildUniqueAttribute_allSlotsFilled_rebuildsCorrectly() throws Exception {
        String              template = "{db.name}.{name}@{clusterName}";
        Map<String, String> slots    = map("db.name", "mydb", "name", "new_table", "clusterName", "prod");

        String result = invokeBuildUniqueAttribute(template, slots);

        assertEquals(result, "mydb.new_table@prod");
    }

    @Test
    public void buildUniqueAttribute_missingSlot_substituteEmptyString() throws Exception {
        String              template = "{db.name}.{name}@{clusterName}";
        Map<String, String> slots    = map("db.name", "mydb", "name", "tbl"); // "clusterName" absent

        String result = invokeBuildUniqueAttribute(template, slots);

        assertEquals(result, "mydb.tbl@");
    }

    @Test
    public void buildUniqueAttribute_noSlots_returnsLiterals() throws Exception {
        String result = invokeBuildUniqueAttribute("literal_only", Collections.emptyMap());

        assertEquals(result, "literal_only");
    }

    // =========================================================================
    // 3. recomputeUniqueAttribute — parse + override + rebuild
    // =========================================================================

    @Test
    public void recomputeUniqueAttribute_overridesCorrectSlot() throws Exception {
        String result = invokeRecomputeUniqueAttribute(
                "old_db.my_table@cluster1",
                "{db.name}.{name}@{clusterName}",
                "db.name",
                "new_db");

        assertEquals(result, "new_db.my_table@cluster1");
    }

    @Test
    public void recomputeUniqueAttribute_nullOverrideKey_qnUnchanged() throws Exception {
        String original = "old_db.my_table@cluster1";
        String result   = invokeRecomputeUniqueAttribute(
                original, "{db.name}.{name}@{clusterName}", null, "new_db");

        assertEquals(result, original);
    }

    @Test
    public void recomputeUniqueAttribute_multiLevelTemplate_onlyTargetedSlotChanges() throws Exception {
        // hive_column template: {table.db.name}.{table.name}@{clusterName}.{name}
        String result = invokeRecomputeUniqueAttribute(
                "old_db.my_table@prod.col1",
                "{table.db.name}.{table.name}@{clusterName}.{name}",
                "table.db.name",
                "new_db");

        assertEquals(result, "new_db.my_table@prod.col1");
    }

    // =========================================================================
    // 4. buildMappedAttrs — propagateAttributes → dependent stub attribute map
    // =========================================================================

    @Test
    public void buildMappedAttrs_nameToNameMapping_setsNameOnDependent() throws Exception {
        List<Map<String, String>> propagateAttributes = Collections.singletonList(
                map("source", "name", "target", "name"));

        Map<String, Object> result = invokeBuildMappedAttrs(propagateAttributes, "new_source_name");

        assertEquals(result.get("name"), "new_source_name");
        assertEquals(result.size(), 1);
    }

    @Test
    public void buildMappedAttrs_nameToCustomTargetMapping_putsCustomTargetKey() throws Exception {
        // Mapping source "name" to a differently named target attribute on the dependent.
        List<Map<String, String>> propagateAttributes = Collections.singletonList(
                map("source", "name", "target", "sourceCluster"));

        Map<String, Object> result = invokeBuildMappedAttrs(propagateAttributes, "new_name");

        assertEquals(result.get("sourceCluster"), "new_name");
        assertTrue(!result.containsKey("name"));
    }

    @Test
    public void buildMappedAttrs_multipleNameMappings_allTargetsFilled() throws Exception {
        List<Map<String, String>> propagateAttributes = Arrays.asList(
                map("source", "name",        "target", "name"),
                map("source", "clusterName", "target", "sourceCluster"), // non-name source — skipped
                map("source", "name",        "target", "mirrorName"));   // second name mapping

        Map<String, Object> result = invokeBuildMappedAttrs(propagateAttributes, "entity_name");

        assertEquals(result.get("name"),       "entity_name");
        assertEquals(result.get("mirrorName"), "entity_name");
        assertTrue(!result.containsKey("sourceCluster")); // clusterName source not available here
    }

    @Test
    public void buildMappedAttrs_noNameSourceMapping_returnsEmptyMap() throws Exception {
        List<Map<String, String>> propagateAttributes = Collections.singletonList(
                map("source", "owner", "target", "schemaOwner"));

        Map<String, Object> result = invokeBuildMappedAttrs(propagateAttributes, "ignored");

        assertTrue(result.isEmpty());
    }

    // =========================================================================
    // 4b. findQualifiedNameOverrideKey — propagateAttributes → QN slot key
    // =========================================================================

    @Test
    public void findQualifiedNameOverrideKey_nameToName_returnsName() throws Exception {
        List<Map<String, String>> propagateAttributes = Collections.singletonList(
                map("source", "name", "target", "name"));

        assertEquals(invokeFindQualifiedNameOverrideKey(propagateAttributes), "name");
    }

    @Test
    public void findQualifiedNameOverrideKey_nameToCustomTarget_returnsTarget() throws Exception {
        List<Map<String, String>> propagateAttributes = Collections.singletonList(
                map("source", "name", "target", "sourceCluster"));

        assertEquals(invokeFindQualifiedNameOverrideKey(propagateAttributes), "sourceCluster");
    }

    @Test
    public void findQualifiedNameOverrideKey_firstNameMappingWins() throws Exception {
        List<Map<String, String>> propagateAttributes = Arrays.asList(
                map("source", "owner", "target", "schemaOwner"),
                map("source", "name", "target", "mirrorName"));

        assertEquals(invokeFindQualifiedNameOverrideKey(propagateAttributes), "mirrorName");
    }

    @Test
    public void findQualifiedNameOverrideKey_emptyList_returnsNull() throws Exception {
        assertNull(invokeFindQualifiedNameOverrideKey(Collections.emptyList()));
    }

    @Test
    public void findQualifiedNameOverrideKey_noNameSource_returnsNull() throws Exception {
        List<Map<String, String>> propagateAttributes = Collections.singletonList(
                map("source", "clusterName", "target", "sourceCluster"));

        assertNull(invokeFindQualifiedNameOverrideKey(propagateAttributes));
    }

    // =========================================================================
    // 5. addDependentsToContext — integration tests with graph mocking
    // =========================================================================

    @Test
    public void addDependentsToContext_noPropagationTargets_contextEmpty() throws AtlasBaseException {
        AtlasEntityType sourceType = mock(AtlasEntityType.class);
        AtlasVertex     srcVertex  = mock(AtlasVertex.class);
        AtlasEntity     srcEntity  = new AtlasEntity("hive_db");

        srcEntity.setAttribute(AtlasTypeUtil.ATTRIBUTE_NAME, "new_db");
        when(sourceType.getTypeName()).thenReturn("hive_db");
        when(sourceType.getRenamePropagationTargets()).thenReturn(Collections.emptyList());

        EntityMutationContext context = new EntityMutationContext();

        handler.addDependentsToContext(context, sourceType, srcVertex, srcEntity);

        assertTrue(context.getUpdatedEntities().isEmpty());
    }

    @Test
    public void addDependentsToContext_propagationTargetWithNullRelAttr_skipped() throws AtlasBaseException {
        RenamePropagationTarget propagationTarget = mock(RenamePropagationTarget.class);

        when(propagationTarget.getRelAttr()).thenReturn(null);

        AtlasEntityType sourceType = mock(AtlasEntityType.class);
        AtlasVertex     srcVertex  = mock(AtlasVertex.class);
        AtlasEntity     srcEntity  = new AtlasEntity("hive_db");

        srcEntity.setAttribute(AtlasTypeUtil.ATTRIBUTE_NAME, "new_db");
        when(sourceType.getTypeName()).thenReturn("hive_db");
        when(sourceType.getRenamePropagationTargets()).thenReturn(Collections.singletonList(propagationTarget));

        EntityMutationContext context = new EntityMutationContext();

        handler.addDependentsToContext(context, sourceType, srcVertex, srcEntity);

        assertTrue(context.getUpdatedEntities().isEmpty());
    }

    @Test
    public void addDependentsToContext_noEdgesFromSource_contextEmpty() throws AtlasBaseException {
        openStaticMocks();

        AtlasAttribute          relAttr          = mock(AtlasAttribute.class);
        RenamePropagationTarget propagationTarget = mock(RenamePropagationTarget.class);
        AtlasEntityType         sourceType        = mock(AtlasEntityType.class);
        AtlasVertex             srcVertex         = mock(AtlasVertex.class);
        AtlasEntity             srcEntity         = new AtlasEntity("hive_db");

        srcEntity.setAttribute(AtlasTypeUtil.ATTRIBUTE_NAME, "new_db");
        when(sourceType.getTypeName()).thenReturn("hive_db");
        when(sourceType.getRenamePropagationTargets()).thenReturn(Collections.singletonList(propagationTarget));
        when(propagationTarget.getRelAttr()).thenReturn(relAttr);
        when(propagationTarget.getPropagateAttributes()).thenReturn(Collections.emptyList());
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__hive_table.db");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.<AtlasEdge>emptyList().iterator());

        EntityMutationContext context = new EntityMutationContext();

        handler.addDependentsToContext(context, sourceType, srcVertex, srcEntity);

        assertTrue(context.getUpdatedEntities().isEmpty());
    }

    @Test
    public void addDependentsToContext_templateMapPath_dependentQnRecomputedCorrectly() throws AtlasBaseException {
        openStaticMocks();

        final String dependentGuid = "dependent-guid-001";
        final String dependentType = "hive_table";
        final String oldQn         = "old_db.my_table@cluster1";
        final String expectedQn    = "new_db.my_table@cluster1";
        final String template       = "{db.name}.{name}@{clusterName}";
        final String qnPropKey    = "Referenceable.qualifiedName";

        AtlasAttribute          relAttr              = mock(AtlasAttribute.class);
        RenamePropagationTarget propagationTarget     = mock(RenamePropagationTarget.class);
        AtlasEntityType         sourceType            = mock(AtlasEntityType.class);
        AtlasEntityType         dependentEntityType   = mock(AtlasEntityType.class);
        AtlasVertex             srcVertex             = mock(AtlasVertex.class);
        AtlasVertex             dependentVertex       = mock(AtlasVertex.class);
        AtlasEdge               edge                  = mock(AtlasEdge.class);
        AtlasAttribute          qnAttr                = mock(AtlasAttribute.class);
        org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef attrDef =
                mock(org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.class);

        // source setup
        when(sourceType.getTypeName()).thenReturn("hive_db");
        when(sourceType.getRenamePropagationTargets()).thenReturn(Collections.singletonList(propagationTarget));

        // propagation target — no propagateAttributes → uses template-map path
        when(propagationTarget.getRelAttr()).thenReturn(relAttr);
        when(propagationTarget.getPropagateAttributes()).thenReturn(Collections.emptyList());
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__hive_table.db");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        // edge → dependentVertex
        when(edge.getOutVertex()).thenReturn(srcVertex);
        when(edge.getInVertex()).thenReturn(dependentVertex);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.singletonList(edge).iterator());

        // dependent vertex and type setup
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(dependentVertex)).thenReturn(dependentGuid);
        graphHelperMock.when(() -> GraphHelper.getTypeName(dependentVertex)).thenReturn(dependentType);
        when(typeRegistry.getEntityTypeByName(dependentType)).thenReturn(dependentEntityType);
        when(dependentEntityType.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnAttr);
        when(qnAttr.getAttributeDef()).thenReturn(attrDef);
        when(attrDef.getAutoComputeFormat()).thenReturn(template);
        when(dependentEntityType.getVertexPropertyName(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnPropKey);
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getProperty(dependentVertex, qnPropKey, String.class)).thenReturn(oldQn);
        when(dependentEntityType.getAutoComputeFormatPathByRefTypeNameMap()).thenReturn(Collections.singletonMap("hive_db", "db.name"));
        when(dependentEntityType.getRenamePropagationTargets()).thenReturn(Collections.emptyList());

        AtlasEntity srcEntity = new AtlasEntity("hive_db");

        srcEntity.setAttribute(AtlasTypeUtil.ATTRIBUTE_NAME, "new_db");

        EntityMutationContext context = new EntityMutationContext();

        handler.addDependentsToContext(context, sourceType, srcVertex, srcEntity);

        assertEquals(context.getUpdatedEntities().size(), 1);

        AtlasEntity updatedStub = context.getUpdatedEntities().iterator().next();

        assertEquals(updatedStub.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME), expectedQn);
        assertEquals(updatedStub.getGuid(), dependentGuid);
        assertEquals(updatedStub.getTypeName(), dependentType);
    }

    @Test
    public void addDependentsToContext_templateMapPath_noOverrideKeyForRenamedType_dependentSkipped() throws AtlasBaseException {
        openStaticMocks();

        final String dependentGuid = "dependent-guid-002";
        final String dependentType = "hive_table";
        final String qnPropKey    = "Referenceable.qualifiedName";
        final String template       = "{db.name}.{name}@{clusterName}";

        AtlasAttribute          relAttr            = mock(AtlasAttribute.class);
        RenamePropagationTarget propagationTarget   = mock(RenamePropagationTarget.class);
        AtlasEntityType         sourceType          = mock(AtlasEntityType.class);
        AtlasEntityType         dependentEntityType = mock(AtlasEntityType.class);
        AtlasVertex             srcVertex           = mock(AtlasVertex.class);
        AtlasVertex             dependentVertex     = mock(AtlasVertex.class);
        AtlasEdge               edge                = mock(AtlasEdge.class);
        AtlasAttribute          qnAttr              = mock(AtlasAttribute.class);
        org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef attrDef =
                mock(org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.class);

        when(sourceType.getTypeName()).thenReturn("some_other_type");
        when(sourceType.getRenamePropagationTargets()).thenReturn(Collections.singletonList(propagationTarget));
        when(propagationTarget.getRelAttr()).thenReturn(relAttr);
        when(propagationTarget.getPropagateAttributes()).thenReturn(Collections.emptyList());
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__hive_table.db");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        when(edge.getOutVertex()).thenReturn(srcVertex);
        when(edge.getInVertex()).thenReturn(dependentVertex);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.singletonList(edge).iterator());

        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(dependentVertex)).thenReturn(dependentGuid);
        graphHelperMock.when(() -> GraphHelper.getTypeName(dependentVertex)).thenReturn(dependentType);
        when(typeRegistry.getEntityTypeByName(dependentType)).thenReturn(dependentEntityType);
        when(dependentEntityType.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnAttr);
        when(qnAttr.getAttributeDef()).thenReturn(attrDef);
        when(attrDef.getAutoComputeFormat()).thenReturn(template);
        when(dependentEntityType.getVertexPropertyName(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnPropKey);
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getProperty(dependentVertex, qnPropKey, String.class))
                .thenReturn("old_db.my_table@cluster1");
        // Template map does NOT contain "some_other_type" — overrideKey will be null → dependent skipped.
        when(dependentEntityType.getAutoComputeFormatPathByRefTypeNameMap()).thenReturn(Collections.singletonMap("hive_db", "db.name"));

        AtlasEntity srcEntity = new AtlasEntity("some_other_type");

        srcEntity.setAttribute(AtlasTypeUtil.ATTRIBUTE_NAME, "new_name");

        EntityMutationContext context = new EntityMutationContext();

        handler.addDependentsToContext(context, sourceType, srcVertex, srcEntity);

        assertTrue(context.getUpdatedEntities().isEmpty());
    }

    @Test
    public void addDependentsToContext_newQnEqualsOldQn_dependentNotAddedToContext() throws AtlasBaseException {
        openStaticMocks();

        final String dependentGuid = "dependent-guid-003";
        final String dependentType = "hive_table";
        final String sameQn        = "same_db.my_table@cluster1";
        final String qnPropKey    = "Referenceable.qualifiedName";
        final String template       = "{db.name}.{name}@{clusterName}";

        AtlasAttribute          relAttr            = mock(AtlasAttribute.class);
        RenamePropagationTarget propagationTarget   = mock(RenamePropagationTarget.class);
        AtlasEntityType         sourceType          = mock(AtlasEntityType.class);
        AtlasEntityType         dependentEntityType = mock(AtlasEntityType.class);
        AtlasVertex             srcVertex           = mock(AtlasVertex.class);
        AtlasVertex             dependentVertex     = mock(AtlasVertex.class);
        AtlasEdge               edge                = mock(AtlasEdge.class);
        AtlasAttribute          qnAttr              = mock(AtlasAttribute.class);
        org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef attrDef =
                mock(org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.class);

        when(sourceType.getTypeName()).thenReturn("hive_db");
        when(sourceType.getRenamePropagationTargets()).thenReturn(Collections.singletonList(propagationTarget));
        when(propagationTarget.getRelAttr()).thenReturn(relAttr);
        when(propagationTarget.getPropagateAttributes()).thenReturn(Collections.emptyList());
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__hive_table.db");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        when(edge.getOutVertex()).thenReturn(srcVertex);
        when(edge.getInVertex()).thenReturn(dependentVertex);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.singletonList(edge).iterator());

        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(dependentVertex)).thenReturn(dependentGuid);
        graphHelperMock.when(() -> GraphHelper.getTypeName(dependentVertex)).thenReturn(dependentType);
        when(typeRegistry.getEntityTypeByName(dependentType)).thenReturn(dependentEntityType);
        when(dependentEntityType.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnAttr);
        when(qnAttr.getAttributeDef()).thenReturn(attrDef);
        when(attrDef.getAutoComputeFormat()).thenReturn(template);
        when(dependentEntityType.getVertexPropertyName(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnPropKey);
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getProperty(dependentVertex, qnPropKey, String.class)).thenReturn(sameQn);
        // "db.name" slot already equals "same_db" — and new name is also "same_db" → newQN == oldQN.
        when(dependentEntityType.getAutoComputeFormatPathByRefTypeNameMap()).thenReturn(Collections.singletonMap("hive_db", "db.name"));
        when(dependentEntityType.getRenamePropagationTargets()).thenReturn(Collections.emptyList());

        AtlasEntity srcEntity = new AtlasEntity("hive_db");

        srcEntity.setAttribute(AtlasTypeUtil.ATTRIBUTE_NAME, "same_db");

        EntityMutationContext context = new EntityMutationContext();

        handler.addDependentsToContext(context, sourceType, srcVertex, srcEntity);

        assertTrue(context.getUpdatedEntities().isEmpty());
    }

    @Test
    public void addDependentsToContext_propagateAttributesPath_dependentQnAndMappedAttrsUpdated() throws AtlasBaseException {
        openStaticMocks();

        final String dependentGuid = "assoc-dependent-guid";
        final String dependentType = "trino_table";
        final String oldQn         = "cat1.pub.old_table@prod";
        final String expectedQn    = "cat1.pub.new_table@prod";
        final String template       = "{catalog.name}.{schema.name}.{name}@{clusterName}";
        final String qnPropKey    = "trino_table.qualifiedName";

        AtlasAttribute          relAttr            = mock(AtlasAttribute.class);
        RenamePropagationTarget propagationTarget   = mock(RenamePropagationTarget.class);
        AtlasEntityType         sourceType          = mock(AtlasEntityType.class);
        AtlasEntityType         dependentEntityType = mock(AtlasEntityType.class);
        AtlasVertex             srcVertex           = mock(AtlasVertex.class);
        AtlasVertex             dependentVertex     = mock(AtlasVertex.class);
        AtlasEdge               edge                = mock(AtlasEdge.class);
        AtlasAttribute          qnAttr              = mock(AtlasAttribute.class);
        org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef attrDef =
                mock(org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.class);

        List<Map<String, String>> propagateAttributes = Collections.singletonList(
                map("source", "name", "target", "name"));

        when(sourceType.getTypeName()).thenReturn("hive_table");
        when(sourceType.getRenamePropagationTargets()).thenReturn(Collections.singletonList(propagationTarget));
        when(propagationTarget.getRelAttr()).thenReturn(relAttr);
        when(propagationTarget.getPropagateAttributes()).thenReturn(propagateAttributes);
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__trino_table_hive_table");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        when(edge.getOutVertex()).thenReturn(srcVertex);
        when(edge.getInVertex()).thenReturn(dependentVertex);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.singletonList(edge).iterator());

        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(dependentVertex)).thenReturn(dependentGuid);
        graphHelperMock.when(() -> GraphHelper.getTypeName(dependentVertex)).thenReturn(dependentType);
        when(typeRegistry.getEntityTypeByName(dependentType)).thenReturn(dependentEntityType);
        when(dependentEntityType.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnAttr);
        when(qnAttr.getAttributeDef()).thenReturn(attrDef);
        when(attrDef.getAutoComputeFormat()).thenReturn(template);
        when(dependentEntityType.getVertexPropertyName(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnPropKey);
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getProperty(dependentVertex, qnPropKey, String.class)).thenReturn(oldQn);
        when(dependentEntityType.getRenamePropagationTargets()).thenReturn(Collections.emptyList());

        AtlasEntity srcEntity = new AtlasEntity("hive_table");

        srcEntity.setAttribute(AtlasTypeUtil.ATTRIBUTE_NAME, "new_table");

        EntityMutationContext context = new EntityMutationContext();

        handler.addDependentsToContext(context, sourceType, srcVertex, srcEntity);

        assertEquals(context.getUpdatedEntities().size(), 1);

        AtlasEntity stub = context.getUpdatedEntities().iterator().next();

        assertEquals(stub.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME), expectedQn);
        assertEquals(stub.getAttribute("name"), "new_table");
        assertEquals(stub.getGuid(), dependentGuid);
    }

    @Test
    public void addDependentsToContext_propagateAttributesPath_nameMappedToCustomQnSlot_updatesCorrectSegment() throws AtlasBaseException {
        openStaticMocks();

        final String dependentGuid = "assoc-dependent-guid-2";
        final String dependentType = "trino_schema";
        final String oldQn         = "c1.oldCluster.n1@prod";
        final String expectedQn    = "c1.newCluster.n1@prod";
        final String template       = "{catalog}.{sourceCluster}.{name}@prod";
        final String qnPropKey    = "trino_schema.qualifiedName";

        AtlasAttribute          relAttr             = mock(AtlasAttribute.class);
        RenamePropagationTarget propagationTarget   = mock(RenamePropagationTarget.class);
        AtlasEntityType         sourceType          = mock(AtlasEntityType.class);
        AtlasEntityType         dependentEntityType = mock(AtlasEntityType.class);
        AtlasVertex             srcVertex           = mock(AtlasVertex.class);
        AtlasVertex             dependentVertex     = mock(AtlasVertex.class);
        AtlasEdge               edge                = mock(AtlasEdge.class);
        AtlasAttribute          qnAttr              = mock(AtlasAttribute.class);
        org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef attrDef =
                mock(org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.class);

        List<Map<String, String>> propagateAttributes = Collections.singletonList(
                map("source", "name", "target", "sourceCluster"));

        when(sourceType.getTypeName()).thenReturn("hive_db");
        when(sourceType.getRenamePropagationTargets()).thenReturn(Collections.singletonList(propagationTarget));
        when(propagationTarget.getRelAttr()).thenReturn(relAttr);
        when(propagationTarget.getPropagateAttributes()).thenReturn(propagateAttributes);
        when(relAttr.getRelationshipEdgeLabel()).thenReturn("__trino_schema_hive_db");
        when(relAttr.getRelationshipEdgeDirection()).thenReturn(null);

        when(edge.getOutVertex()).thenReturn(srcVertex);
        when(edge.getInVertex()).thenReturn(dependentVertex);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.singletonList(edge).iterator());

        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(dependentVertex)).thenReturn(dependentGuid);
        graphHelperMock.when(() -> GraphHelper.getTypeName(dependentVertex)).thenReturn(dependentType);
        when(typeRegistry.getEntityTypeByName(dependentType)).thenReturn(dependentEntityType);
        when(dependentEntityType.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnAttr);
        when(qnAttr.getAttributeDef()).thenReturn(attrDef);
        when(attrDef.getAutoComputeFormat()).thenReturn(template);
        when(dependentEntityType.getVertexPropertyName(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnPropKey);
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getProperty(dependentVertex, qnPropKey, String.class)).thenReturn(oldQn);
        when(dependentEntityType.getRenamePropagationTargets()).thenReturn(Collections.emptyList());

        AtlasEntity srcEntity = new AtlasEntity("hive_db");

        srcEntity.setAttribute(AtlasTypeUtil.ATTRIBUTE_NAME, "newCluster");

        EntityMutationContext context = new EntityMutationContext();

        handler.addDependentsToContext(context, sourceType, srcVertex, srcEntity);

        assertEquals(context.getUpdatedEntities().size(), 1);

        AtlasEntity stub = context.getUpdatedEntities().iterator().next();

        assertEquals(stub.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME), expectedQn);
        assertEquals(stub.getAttribute("sourceCluster"), "newCluster");
    }

    @Test
    public void addDependentsToContext_visitedGuard_sameDependentNotProcessedTwice() throws AtlasBaseException {
        openStaticMocks();

        final String dependentGuid = "shared-dependent-guid";
        final String dependentType = "hive_table";
        final String oldQn         = "old_db.tbl@cluster1";
        final String qnPropKey    = "Referenceable.qualifiedName";
        final String template       = "{db.name}.{name}@{clusterName}";

        AtlasAttribute          relAttr1            = mock(AtlasAttribute.class);
        AtlasAttribute          relAttr2            = mock(AtlasAttribute.class);
        RenamePropagationTarget propagationTarget1  = mock(RenamePropagationTarget.class);
        RenamePropagationTarget propagationTarget2  = mock(RenamePropagationTarget.class);
        AtlasEntityType         sourceType          = mock(AtlasEntityType.class);
        AtlasEntityType         dependentEntityType = mock(AtlasEntityType.class);
        AtlasVertex             srcVertex           = mock(AtlasVertex.class);
        AtlasVertex             dependentVertex     = mock(AtlasVertex.class);
        AtlasEdge               edge                = mock(AtlasEdge.class);
        AtlasAttribute          qnAttr              = mock(AtlasAttribute.class);
        org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef attrDef =
                mock(org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.class);

        // Two propagation targets both resolve to the same dependent vertex/guid.
        when(sourceType.getTypeName()).thenReturn("hive_db");
        when(sourceType.getRenamePropagationTargets()).thenReturn(Arrays.asList(propagationTarget1, propagationTarget2));

        for (RenamePropagationTarget propagationTarget : Arrays.asList(propagationTarget1, propagationTarget2)) {
            when(propagationTarget.getPropagateAttributes()).thenReturn(Collections.emptyList());
        }

        when(propagationTarget1.getRelAttr()).thenReturn(relAttr1);
        when(relAttr1.getRelationshipEdgeLabel()).thenReturn("edge_label_1");
        when(relAttr1.getRelationshipEdgeDirection()).thenReturn(null);

        when(propagationTarget2.getRelAttr()).thenReturn(relAttr2);
        when(relAttr2.getRelationshipEdgeLabel()).thenReturn("edge_label_2");
        when(relAttr2.getRelationshipEdgeDirection()).thenReturn(null);

        when(edge.getOutVertex()).thenReturn(srcVertex);
        when(edge.getInVertex()).thenReturn(dependentVertex);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(srcVertex), anyString(), any()))
                .thenReturn(Collections.singletonList(edge).iterator());

        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(dependentVertex)).thenReturn(dependentGuid);
        graphHelperMock.when(() -> GraphHelper.getTypeName(dependentVertex)).thenReturn(dependentType);
        when(typeRegistry.getEntityTypeByName(dependentType)).thenReturn(dependentEntityType);
        when(dependentEntityType.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnAttr);
        when(qnAttr.getAttributeDef()).thenReturn(attrDef);
        when(attrDef.getAutoComputeFormat()).thenReturn(template);
        when(dependentEntityType.getVertexPropertyName(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnPropKey);
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getProperty(dependentVertex, qnPropKey, String.class)).thenReturn(oldQn);
        when(dependentEntityType.getAutoComputeFormatPathByRefTypeNameMap()).thenReturn(Collections.singletonMap("hive_db", "db.name"));
        when(dependentEntityType.getRenamePropagationTargets()).thenReturn(Collections.emptyList());

        AtlasEntity srcEntity = new AtlasEntity("hive_db");

        srcEntity.setAttribute(AtlasTypeUtil.ATTRIBUTE_NAME, "new_db");

        EntityMutationContext context = new EntityMutationContext();

        handler.addDependentsToContext(context, sourceType, srcVertex, srcEntity);

        // Even though two targets both reach the same dependent, it must appear only once.
        assertEquals(context.getUpdatedEntities().size(), 1);
    }

    @Test
    public void addDependentsToContext_twoLevelStructuralCascade_bothLevelsUpdated() throws AtlasBaseException {
        openStaticMocks();

        final String tableGuid  = "table-guid";
        final String tableType  = "hive_table";
        final String tableOld   = "old_db.my_table@cluster1";
        final String tableNew   = "new_db.my_table@cluster1";
        final String tableTmpl  = "{db.name}.{name}@{clusterName}";

        final String colGuid    = "col-guid";
        final String colType    = "hive_column";
        final String colOld     = "old_db.my_table@cluster1.col1";
        final String colNew     = "new_db.my_table@cluster1.col1";
        final String colTmpl    = "{table.db.name}.{table.name}@{clusterName}.{name}";

        final String qnPropKey = "Referenceable.qualifiedName";

        // Source: hive_db
        AtlasEntityType         dbType              = mock(AtlasEntityType.class);
        AtlasVertex             dbVertex            = mock(AtlasVertex.class);
        RenamePropagationTarget dbToTableTarget     = mock(RenamePropagationTarget.class);
        AtlasAttribute          dbRelAttr           = mock(AtlasAttribute.class);

        // Level 1 dependent: hive_table
        AtlasEntityType         tableEntityType      = mock(AtlasEntityType.class);
        AtlasVertex             tableVertex          = mock(AtlasVertex.class);
        AtlasEdge               dbToTableEdge        = mock(AtlasEdge.class);
        AtlasAttribute          tableQnAttr          = mock(AtlasAttribute.class);
        org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef tableAttrDef =
                mock(org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.class);
        RenamePropagationTarget tableToColTarget     = mock(RenamePropagationTarget.class);
        AtlasAttribute          tableRelAttr         = mock(AtlasAttribute.class);

        // Level 2 dependent: hive_column
        AtlasEntityType         colEntityType        = mock(AtlasEntityType.class);
        AtlasVertex             colVertex            = mock(AtlasVertex.class);
        AtlasEdge               tableToColEdge       = mock(AtlasEdge.class);
        AtlasAttribute          colQnAttr            = mock(AtlasAttribute.class);
        org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef colAttrDef =
                mock(org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.class);

        // ── source (hive_db) ──
        when(dbType.getTypeName()).thenReturn("hive_db");
        when(dbType.getRenamePropagationTargets()).thenReturn(Collections.singletonList(dbToTableTarget));
        when(dbToTableTarget.getRelAttr()).thenReturn(dbRelAttr);
        when(dbToTableTarget.getPropagateAttributes()).thenReturn(Collections.emptyList());
        when(dbRelAttr.getRelationshipEdgeLabel()).thenReturn("__hive_table.db");
        when(dbRelAttr.getRelationshipEdgeDirection()).thenReturn(null);

        when(dbToTableEdge.getOutVertex()).thenReturn(dbVertex);
        when(dbToTableEdge.getInVertex()).thenReturn(tableVertex);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(dbVertex), eq("__hive_table.db"), any()))
                .thenReturn(Collections.singletonList(dbToTableEdge).iterator());

        // ── level 1 dependent: hive_table ──
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(tableVertex)).thenReturn(tableGuid);
        graphHelperMock.when(() -> GraphHelper.getTypeName(tableVertex)).thenReturn(tableType);
        when(typeRegistry.getEntityTypeByName(tableType)).thenReturn(tableEntityType);
        when(tableEntityType.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(tableQnAttr);
        when(tableQnAttr.getAttributeDef()).thenReturn(tableAttrDef);
        when(tableAttrDef.getAutoComputeFormat()).thenReturn(tableTmpl);
        when(tableEntityType.getVertexPropertyName(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnPropKey);
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getProperty(tableVertex, qnPropKey, String.class)).thenReturn(tableOld);
        when(tableEntityType.getAutoComputeFormatPathByRefTypeNameMap()).thenReturn(Collections.singletonMap("hive_db", "db.name"));
        when(tableEntityType.getRenamePropagationTargets()).thenReturn(Collections.singletonList(tableToColTarget));

        when(tableToColTarget.getRelAttr()).thenReturn(tableRelAttr);
        when(tableToColTarget.getPropagateAttributes()).thenReturn(Collections.emptyList());
        when(tableRelAttr.getRelationshipEdgeLabel()).thenReturn("__hive_column.table");
        when(tableRelAttr.getRelationshipEdgeDirection()).thenReturn(null);

        when(tableToColEdge.getOutVertex()).thenReturn(tableVertex);
        when(tableToColEdge.getInVertex()).thenReturn(colVertex);
        graphHelperMock.when(() -> GraphHelper.getEdgesForLabel(eq(tableVertex), eq("__hive_column.table"), any()))
                .thenReturn(Collections.singletonList(tableToColEdge).iterator());

        // ── level 2 dependent: hive_column ──
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getIdFromVertex(colVertex)).thenReturn(colGuid);
        graphHelperMock.when(() -> GraphHelper.getTypeName(colVertex)).thenReturn(colType);
        when(typeRegistry.getEntityTypeByName(colType)).thenReturn(colEntityType);
        when(colEntityType.getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(colQnAttr);
        when(colQnAttr.getAttributeDef()).thenReturn(colAttrDef);
        when(colAttrDef.getAutoComputeFormat()).thenReturn(colTmpl);
        when(colEntityType.getVertexPropertyName(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME)).thenReturn(qnPropKey);
        graphUtilsMock.when(() -> AtlasGraphUtilsV2.getProperty(colVertex, qnPropKey, String.class)).thenReturn(colOld);
        when(colEntityType.getAutoComputeFormatPathByRefTypeNameMap()).thenReturn(Collections.singletonMap("hive_db", "table.db.name"));
        when(colEntityType.getRenamePropagationTargets()).thenReturn(Collections.emptyList());

        // ── execute ──
        AtlasEntity srcEntity = new AtlasEntity("hive_db");

        srcEntity.setAttribute(AtlasTypeUtil.ATTRIBUTE_NAME, "new_db");

        EntityMutationContext context = new EntityMutationContext();

        handler.addDependentsToContext(context, dbType, dbVertex, srcEntity);

        assertEquals(context.getUpdatedEntities().size(), 2);

        Map<String, AtlasEntity> byGuid = new HashMap<>();

        for (AtlasEntity entity : context.getUpdatedEntities()) {
            byGuid.put(entity.getGuid(), entity);
        }

        assertNotNull(byGuid.get(tableGuid));
        assertNotNull(byGuid.get(colGuid));
        assertEquals(byGuid.get(tableGuid).getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME), tableNew);
        assertEquals(byGuid.get(colGuid).getAttribute(AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME), colNew);
    }

    // =========================================================================
    // helpers
    // =========================================================================

    private void openStaticMocks() {
        graphHelperMock = mockStatic(GraphHelper.class);
        graphUtilsMock  = mockStatic(AtlasGraphUtilsV2.class);
    }

    // ── reflection wrappers for private methods ───────────────────────────────

    @SuppressWarnings("unchecked")
    private Map<String, String> invokeParseUniqueAttribute(String uniqueAttr, String template) throws Exception {
        Method m = EntityRenameHandler.class.getDeclaredMethod("parseUniqueAttribute", String.class, String.class);

        m.setAccessible(true);

        return (Map<String, String>) m.invoke(handler, uniqueAttr, template);
    }

    private String invokeBuildUniqueAttribute(String template, Map<String, String> slots) throws Exception {
        Method m = EntityRenameHandler.class.getDeclaredMethod("buildUniqueAttribute", String.class, Map.class);

        m.setAccessible(true);

        return (String) m.invoke(handler, template, slots);
    }

    private String invokeRecomputeUniqueAttribute(String currentQN, String template,
                                                   String overrideKey, String newValue) throws Exception {
        Method m = EntityRenameHandler.class.getDeclaredMethod(
                "recomputeUniqueAttribute", String.class, String.class, String.class, String.class);

        m.setAccessible(true);

        return (String) m.invoke(handler, currentQN, template, overrideKey, newValue);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> invokeBuildMappedAttrs(List<Map<String, String>> propagateAttributes,
                                                        String newSourceEntityName) throws Exception {
        Method m = EntityRenameHandler.class.getDeclaredMethod(
                "buildMappedAttrs", RenamePropagationTarget.class, String.class);

        m.setAccessible(true);

        RenamePropagationTarget propagationTarget = mock(RenamePropagationTarget.class);

        when(propagationTarget.getPropagateAttributes()).thenReturn(propagateAttributes);

        return (Map<String, Object>) m.invoke(handler, propagationTarget, newSourceEntityName);
    }

    private String invokeFindQualifiedNameOverrideKey(List<Map<String, String>> propagateAttributes) throws Exception {
        Method m = EntityRenameHandler.class.getDeclaredMethod("findQualifiedNameOverrideKey", List.class);

        m.setAccessible(true);

        return (String) m.invoke(null, propagateAttributes);
    }

    // ── Map factory helper ────────────────────────────────────────────────────

    private static <K, V> Map<K, V> map(Object... keysAndValues) {
        Map<K, V> result = new HashMap<>();

        for (int i = 0; i < keysAndValues.length; i += 2) {
            //noinspection unchecked
            result.put((K) keysAndValues[i], (V) keysAndValues[i + 1]);
        }

        return result;
    }
}
