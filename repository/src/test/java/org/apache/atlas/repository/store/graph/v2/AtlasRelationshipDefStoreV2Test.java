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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.RelationshipCategory;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

/**
 * Tests for AtlasRelationshipStoreV1
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class AtlasRelationshipDefStoreV2Test extends AtlasTestBase {
    @Inject
    private AtlasRelationshipDefStoreV2 relationshipDefStore;

    @Mock
    private AtlasTypeDefGraphStoreV2 mockTypeDefStore;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasVertex mockVertex;

    @Mock
    private AtlasVertex mockEnd1TypeVertex;

    @Mock
    private AtlasVertex mockEnd2TypeVertex;

    @Mock
    private AtlasEdge mockEdge1;

    @Mock
    private AtlasEdge mockEdge2;

    @Mock
    private AtlasRelationshipType mockRelationshipType;

    @Mock
    private AtlasType mockAtlasType;

    private AtlasRelationshipDefStoreV2 mockRelationshipDefStore;

    @BeforeMethod
    public void setupMocks() {
        MockitoAnnotations.initMocks(this);
        mockRelationshipDefStore = new AtlasRelationshipDefStoreV2(mockTypeDefStore, mockTypeRegistry);
    }

    @DataProvider
    public Object[][] invalidAttributeNameWithReservedKeywords() {
        AtlasRelationshipDef invalidAttrNameType =
                AtlasTypeUtil.createRelationshipTypeDef("Invalid_Attribute_Type", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeA", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeB", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("order", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("limit", "string"));

        return new Object[][] {
                new Object[] {
                        invalidAttrNameType
                }
        };
    }

    @DataProvider
    public Object[][] updateValidProperties() {
        AtlasRelationshipDef existingType =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "0",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.ONE_TO_TWO,
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));
        AtlasRelationshipDef newType =
                AtlasTypeUtil.createRelationshipTypeDef("basicType",
                        "description1", // updated
                        "1", // updated
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH, // updated
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));

        return new Object[][] {
                new Object[] {
                        existingType,
                        newType
                }
        };
    }

    @DataProvider
    public Object[][] updateRename() {
        AtlasRelationshipDef existingType =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));
        AtlasRelationshipDef newType =
                AtlasTypeUtil.createRelationshipTypeDef("basicType2", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));

        return new Object[][] {
                new Object[] {
                        existingType, newType
                }
        };
    }

    @DataProvider
    public Object[][] updateRelCat() {
        AtlasRelationshipDef existingType =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));
        AtlasRelationshipDef newType =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "",
                        RelationshipCategory.AGGREGATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));

        return new Object[][] {
                new Object[] {
                        existingType, newType
                }
        };
    }

    @DataProvider
    public Object[][] updateEnd1() {
        AtlasRelationshipDef existingType =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));
        AtlasRelationshipDef changeType =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeE", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));
        AtlasRelationshipDef changeAttr =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeC", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));
        AtlasRelationshipDef changeCardinality =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.LIST),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));

        return new Object[][] {
                {
                        existingType,
                        changeType
                },
                {
                        existingType,
                        changeAttr
                },
                {
                        existingType,
                        changeCardinality
                }
        };
    }

    @DataProvider
    public Object[][] updateEnd2() {
        AtlasRelationshipDef existingType =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));

        AtlasRelationshipDef changeType =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeE", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));
        AtlasRelationshipDef changeAttr =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));
        AtlasRelationshipDef changeCardinality =
                AtlasTypeUtil.createRelationshipTypeDef("basicType", "description", "",
                        RelationshipCategory.ASSOCIATION,
                        PropagateTags.BOTH,
                        new AtlasRelationshipEndDef("typeC", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE),
                        new AtlasRelationshipEndDef("typeD", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.LIST),
                        AtlasTypeUtil.createRequiredAttrDef("aaaa", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("bbbb", "string"));

        return new Object[][] {
                {
                        existingType,
                        changeType
                },
                {
                        existingType,
                        changeAttr
                },
                {
                        existingType,
                        changeCardinality
                }
        };
    }

    @Test(dataProvider = "invalidAttributeNameWithReservedKeywords")
    public void testCreateTypeWithReservedKeywords(AtlasRelationshipDef atlasRelationshipDef) throws AtlasException {
        try {
            ApplicationProperties.get().setProperty(AtlasAbstractDefStoreV2.ALLOW_RESERVED_KEYWORDS, false);

            relationshipDefStore.create(atlasRelationshipDef, null);
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.ATTRIBUTE_NAME_INVALID);
        }
    }

    @Test(dataProvider = "updateValidProperties")
    public void testupdateVertexPreUpdatepropagateTags(AtlasRelationshipDef existingRelationshipDef, AtlasRelationshipDef newRelationshipDef) throws AtlasBaseException {
        AtlasRelationshipDefStoreV2.preUpdateCheck(existingRelationshipDef, newRelationshipDef);
    }

    @Test(dataProvider = "updateRename")
    public void testupdateVertexPreUpdateRename(AtlasRelationshipDef existingRelationshipDef, AtlasRelationshipDef newRelationshipDef) {
        try {
            AtlasRelationshipDefStoreV2.preUpdateCheck(existingRelationshipDef, newRelationshipDef);
            fail("expected error");
        } catch (AtlasBaseException e) {
            if (!e.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_INVALID_NAME_UPDATE)) {
                fail("unexpected AtlasErrorCode " + e.getAtlasErrorCode());
            }
        }
    }

    @Test(dataProvider = "updateRelCat")
    public void testupdateVertexPreUpdateRelcat(AtlasRelationshipDef existingRelationshipDef, AtlasRelationshipDef newRelationshipDef) {
        try {
            AtlasRelationshipDefStoreV2.preUpdateCheck(existingRelationshipDef, newRelationshipDef);
            fail("expected error");
        } catch (AtlasBaseException e) {
            if (!e.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_INVALID_CATEGORY_UPDATE)) {
                fail("unexpected AtlasErrorCode " + e.getAtlasErrorCode());
            }
        }
    }

    @Test(dataProvider = "updateEnd1")
    public void testupdateVertexPreUpdateEnd1(AtlasRelationshipDef existingRelationshipDef, AtlasRelationshipDef newRelationshipDef) {
        try {
            AtlasRelationshipDefStoreV2.preUpdateCheck(existingRelationshipDef, newRelationshipDef);
            fail("expected error");
        } catch (AtlasBaseException e) {
            if (!e.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_INVALID_END1_UPDATE)) {
                fail("unexpected AtlasErrorCode " + e.getAtlasErrorCode());
            }
        }
    }

    @Test(dataProvider = "updateEnd2")
    public void testupdateVertexPreUpdateEnd2(AtlasRelationshipDef existingRelationshipDef, AtlasRelationshipDef newRelationshipDef) {
        try {
            AtlasRelationshipDefStoreV2.preUpdateCheck(existingRelationshipDef, newRelationshipDef);

            fail("expected error");
        } catch (AtlasBaseException e) {
            if (!e.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIPDEF_INVALID_END2_UPDATE)) {
                fail("unexpected AtlasErrorCode " + e.getAtlasErrorCode());
            }
        }
    }

    @Test
    public void testSetVertexPropertiesFromRelationshipDef() {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();
        AtlasVertex vertex = mock(AtlasVertex.class);

        AtlasRelationshipDefStoreV2.setVertexPropertiesFromRelationshipDef(relationshipDef, vertex);

        verify(vertex).setProperty(eq(Constants.RELATIONSHIPTYPE_END1_KEY), anyString());
        verify(vertex).setProperty(eq(Constants.RELATIONSHIPTYPE_END2_KEY), anyString());
        verify(vertex).setProperty(eq(Constants.RELATIONSHIPTYPE_CATEGORY_KEY), eq(RelationshipCategory.ASSOCIATION.name()));
        verify(vertex).setProperty(eq(Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY), eq(PropagateTags.BOTH.name()));
    }

    @Test
    public void testSetVertexPropertiesFromRelationshipDefWithNullCategory() {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();
        relationshipDef.setRelationshipCategory(null);
        AtlasVertex vertex = mock(AtlasVertex.class);

        AtlasRelationshipDefStoreV2.setVertexPropertiesFromRelationshipDef(relationshipDef, vertex);

        verify(vertex).setProperty(eq(Constants.RELATIONSHIPTYPE_CATEGORY_KEY), eq(RelationshipCategory.ASSOCIATION.name()));
    }

    @Test
    public void testSetVertexPropertiesFromRelationshipDefWithNullPropagateTags() {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();
        relationshipDef.setPropagateTags(null);
        AtlasVertex vertex = mock(AtlasVertex.class);

        AtlasRelationshipDefStoreV2.setVertexPropertiesFromRelationshipDef(relationshipDef, vertex);

        verify(vertex).setProperty(eq(Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY), eq(PropagateTags.NONE.name()));
    }

    @Test
    public void testSetVertexPropertiesFromRelationshipDefWithNullLabel() {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();
        relationshipDef.setRelationshipLabel(null);
        AtlasVertex vertex = mock(AtlasVertex.class);

        AtlasRelationshipDefStoreV2.setVertexPropertiesFromRelationshipDef(relationshipDef, vertex);

        verify(vertex).removeProperty(Constants.RELATIONSHIPTYPE_LABEL_KEY);
    }

    @Test
    public void testSetVertexPropertiesFromRelationshipDefWithLabel() {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();
        relationshipDef.setRelationshipLabel("testLabel");
        AtlasVertex vertex = mock(AtlasVertex.class);

        AtlasRelationshipDefStoreV2.setVertexPropertiesFromRelationshipDef(relationshipDef, vertex);

        verify(vertex).setProperty(eq(Constants.RELATIONSHIPTYPE_LABEL_KEY), eq("testLabel"));
    }

    @Test
    public void testGetAll() throws Exception {
        List<AtlasVertex> vertexList = Arrays.asList(mockVertex);
        Iterator<AtlasVertex> iterator = vertexList.iterator();
        when(mockTypeDefStore.findTypeVerticesByCategory(TypeCategory.RELATIONSHIP)).thenReturn(iterator);
        setupMocksForToRelationshipDef();

        List<AtlasRelationshipDef> result = mockRelationshipDefStore.getAll();

        assertNotNull(result);
        assertEquals(result.size(), 1);
        assertEquals(result.get(0).getName(), "TestRelationship");
    }

    @Test
    public void testGetByNameSuccess() throws Exception {
        String name = "TestRelationship";
        when(mockTypeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.RELATIONSHIP)).thenReturn(mockVertex);
        when(mockVertex.getProperty(eq(Constants.TYPE_CATEGORY_PROPERTY_KEY), eq(TypeCategory.class))).thenReturn(TypeCategory.RELATIONSHIP);
        setupMocksForToRelationshipDef();

        AtlasRelationshipDef result = mockRelationshipDefStore.getByName(name);

        assertNotNull(result);
        assertEquals(result.getName(), "TestRelationship");
    }

    @Test
    public void testGetByNameNotFound() throws Exception {
        String name = "NonExistentRelationship";
        when(mockTypeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.RELATIONSHIP)).thenReturn(null);

        try {
            mockRelationshipDefStore.getByName(name);
            fail("Expected AtlasBaseException");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.TYPE_NAME_NOT_FOUND);
        }
    }

    @Test
    public void testGetByGuidSuccess() throws Exception {
        String guid = "test-guid";
        when(mockTypeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.RELATIONSHIP)).thenReturn(mockVertex);
        setupMocksForToRelationshipDef();

        AtlasRelationshipDef result = mockRelationshipDefStore.getByGuid(guid);

        assertNotNull(result);
        assertEquals(result.getName(), "TestRelationship");
    }

    @Test
    public void testGetByGuidNotFound() throws Exception {
        String guid = "non-existent-guid";
        when(mockTypeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.RELATIONSHIP)).thenReturn(null);

        try {
            mockRelationshipDefStore.getByGuid(guid);
            fail("Expected AtlasBaseException");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.TYPE_GUID_NOT_FOUND);
        }
    }

    @Test
    public void testUpdateByName() throws Exception {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();
        relationshipDef.setGuid(null);
        setupMocksForUpdate(relationshipDef);

        AtlasRelationshipDef result = mockRelationshipDefStore.update(relationshipDef);

        assertNotNull(result);
        assertEquals(result.getName(), "TestRelationship");
    }

    @Test
    public void testUpdateByGuid() throws Exception {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();
        relationshipDef.setGuid("test-guid");
        setupMocksForUpdate(relationshipDef);

        AtlasRelationshipDef result = mockRelationshipDefStore.update(relationshipDef);

        assertNotNull(result);
        assertEquals(result.getName(), "TestRelationship");
    }

    @Test
    public void testUpdateByNameWithTypeMismatch() throws Exception {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();
        String name = relationshipDef.getName();

        when(mockTypeRegistry.getRelationshipDefByName(name)).thenReturn(relationshipDef);
        when(mockTypeRegistry.getType(relationshipDef.getName())).thenReturn(mockAtlasType);
        when(mockAtlasType.getTypeCategory()).thenReturn(org.apache.atlas.model.TypeCategory.ENTITY);

        try (MockedStatic<AtlasAuthorizationUtils> mockedAuthUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            mockedAuthUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasTypeAccessRequest.class), anyString(), anyString()))
                    .thenAnswer(invocation -> null);

            try {
                mockRelationshipDefStore.updateByName(name, relationshipDef);
                fail("Expected AtlasBaseException");
            } catch (AtlasBaseException e) {
                assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.TYPE_MATCH_FAILED);
            }
        }
    }

    @Test
    public void testUpdateByNameNotFound() throws Exception {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();
        String name = relationshipDef.getName();

        when(mockTypeRegistry.getRelationshipDefByName(name)).thenReturn(relationshipDef);
        when(mockTypeRegistry.getType(relationshipDef.getName())).thenReturn(mockRelationshipType);
        when(mockRelationshipType.getTypeCategory()).thenReturn(org.apache.atlas.model.TypeCategory.RELATIONSHIP);
        when(mockTypeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.RELATIONSHIP)).thenReturn(null);

        try (MockedStatic<AtlasAuthorizationUtils> mockedAuthUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            mockedAuthUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasTypeAccessRequest.class), anyString(), anyString()))
                    .thenAnswer(invocation -> null);

            try {
                mockRelationshipDefStore.updateByName(name, relationshipDef);
                fail("Expected AtlasBaseException");
            } catch (AtlasBaseException e) {
                assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.TYPE_NAME_NOT_FOUND);
            }
        }
    }

    @Test
    public void testUpdateByGuidWithTypeMismatch() throws Exception {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();
        String guid = "test-guid";

        when(mockTypeRegistry.getRelationshipDefByGuid(guid)).thenReturn(relationshipDef);
        when(mockTypeRegistry.getTypeByGuid(guid)).thenReturn(mockAtlasType);
        when(mockAtlasType.getTypeCategory()).thenReturn(org.apache.atlas.model.TypeCategory.ENTITY);

        try (MockedStatic<AtlasAuthorizationUtils> mockedAuthUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            mockedAuthUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasTypeAccessRequest.class), anyString(), anyString()))
                    .thenAnswer(invocation -> null);

            try {
                mockRelationshipDefStore.updateByGuid(guid, relationshipDef);
                fail("Expected AtlasBaseException");
            } catch (AtlasBaseException e) {
                assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.TYPE_MATCH_FAILED);
            }
        }
    }

    @Test
    public void testUpdateByGuidNotFound() throws Exception {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();
        String guid = "test-guid";

        when(mockTypeRegistry.getRelationshipDefByGuid(guid)).thenReturn(relationshipDef);
        when(mockTypeRegistry.getTypeByGuid(guid)).thenReturn(mockRelationshipType);
        when(mockRelationshipType.getTypeCategory()).thenReturn(org.apache.atlas.model.TypeCategory.RELATIONSHIP);
        when(mockTypeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.RELATIONSHIP)).thenReturn(null);

        try (MockedStatic<AtlasAuthorizationUtils> mockedAuthUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            mockedAuthUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasTypeAccessRequest.class), anyString(), anyString()))
                    .thenAnswer(invocation -> null);

            try {
                mockRelationshipDefStore.updateByGuid(guid, relationshipDef);
                fail("Expected AtlasBaseException");
            } catch (AtlasBaseException e) {
                assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.TYPE_GUID_NOT_FOUND);
            }
        }
    }

    @Test
    public void testPreDeleteByNameSuccess() throws Exception {
        String name = "TestRelationship";
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();

        when(mockTypeRegistry.getRelationshipDefByName(name)).thenReturn(relationshipDef);
        when(mockTypeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.RELATIONSHIP)).thenReturn(mockVertex);

        try (MockedStatic<AtlasAuthorizationUtils> mockedAuthUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<AtlasGraphUtilsV2> mockedGraphUtils = mockStatic(AtlasGraphUtilsV2.class)) {
            mockedAuthUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasTypeAccessRequest.class), anyString(), anyString()))
                    .thenAnswer(invocation -> null);
            mockedGraphUtils.when(() -> AtlasGraphUtilsV2.relationshipTypeHasInstanceEdges(name)).thenReturn(false);

            AtlasVertex result = mockRelationshipDefStore.preDeleteByName(name);

            assertNotNull(result);
            verify(mockTypeDefStore).deleteTypeVertexOutEdges(mockVertex);
        }
    }

    @Test
    public void testPreDeleteByNameNotFound() throws Exception {
        String name = "NonExistentRelationship";
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();

        when(mockTypeRegistry.getRelationshipDefByName(name)).thenReturn(relationshipDef);
        when(mockTypeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.RELATIONSHIP)).thenReturn(null);

        try (MockedStatic<AtlasAuthorizationUtils> mockedAuthUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            mockedAuthUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasTypeAccessRequest.class), anyString(), anyString()))
                    .thenAnswer(invocation -> null);

            try {
                mockRelationshipDefStore.preDeleteByName(name);
                fail("Expected AtlasBaseException");
            } catch (AtlasBaseException e) {
                assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.TYPE_NAME_NOT_FOUND);
            }
        }
    }

    @Test
    public void testPreDeleteByNameWithReferences() throws Exception {
        String name = "TestRelationship";
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();

        when(mockTypeRegistry.getRelationshipDefByName(name)).thenReturn(relationshipDef);
        when(mockTypeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.RELATIONSHIP)).thenReturn(mockVertex);

        try (MockedStatic<AtlasAuthorizationUtils> mockedAuthUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<AtlasGraphUtilsV2> mockedGraphUtils = mockStatic(AtlasGraphUtilsV2.class)) {
            mockedAuthUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasTypeAccessRequest.class), anyString(), anyString()))
                    .thenAnswer(invocation -> null);
            mockedGraphUtils.when(() -> AtlasGraphUtilsV2.relationshipTypeHasInstanceEdges(name)).thenReturn(true);

            try {
                mockRelationshipDefStore.preDeleteByName(name);
                fail("Expected AtlasBaseException");
            } catch (AtlasBaseException e) {
                assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.TYPE_HAS_REFERENCES);
            }
        }
    }

    @Test
    public void testPreDeleteByGuidSuccess() throws Exception {
        String guid = "test-guid";
        String typeName = "TestRelationship";
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();

        when(mockTypeRegistry.getRelationshipDefByGuid(guid)).thenReturn(relationshipDef);
        when(mockTypeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.RELATIONSHIP)).thenReturn(mockVertex);

        try (MockedStatic<AtlasAuthorizationUtils> mockedAuthUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<AtlasGraphUtilsV2> mockedGraphUtils = mockStatic(AtlasGraphUtilsV2.class)) {
            mockedAuthUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasTypeAccessRequest.class), anyString(), anyString()))
                    .thenAnswer(invocation -> null);
            mockedGraphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(mockVertex, Constants.TYPENAME_PROPERTY_KEY, String.class))
                    .thenReturn(typeName);
            mockedGraphUtils.when(() -> AtlasGraphUtilsV2.relationshipTypeHasInstanceEdges(typeName)).thenReturn(false);

            AtlasVertex result = mockRelationshipDefStore.preDeleteByGuid(guid);

            assertNotNull(result);
            verify(mockTypeDefStore).deleteTypeVertexOutEdges(mockVertex);
        }
    }

    @Test
    public void testPreDeleteByGuidNotFound() throws Exception {
        String guid = "non-existent-guid";
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();

        when(mockTypeRegistry.getRelationshipDefByGuid(guid)).thenReturn(relationshipDef);
        when(mockTypeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.RELATIONSHIP)).thenReturn(null);

        try (MockedStatic<AtlasAuthorizationUtils> mockedAuthUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            mockedAuthUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasTypeAccessRequest.class), anyString(), anyString()))
                    .thenAnswer(invocation -> null);

            try {
                mockRelationshipDefStore.preDeleteByGuid(guid);
                fail("Expected AtlasBaseException");
            } catch (AtlasBaseException e) {
                assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.TYPE_GUID_NOT_FOUND);
            }
        }
    }

    @Test
    public void testPreDeleteByGuidWithReferences() throws Exception {
        String guid = "test-guid";
        String typeName = "TestRelationship";
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();

        when(mockTypeRegistry.getRelationshipDefByGuid(guid)).thenReturn(relationshipDef);
        when(mockTypeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.RELATIONSHIP)).thenReturn(mockVertex);

        try (MockedStatic<AtlasAuthorizationUtils> mockedAuthUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<AtlasGraphUtilsV2> mockedGraphUtils = mockStatic(AtlasGraphUtilsV2.class)) {
            mockedAuthUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasTypeAccessRequest.class), anyString(), anyString()))
                    .thenAnswer(invocation -> null);
            mockedGraphUtils.when(() -> AtlasGraphUtilsV2.getEncodedProperty(mockVertex, Constants.TYPENAME_PROPERTY_KEY, String.class))
                    .thenReturn(typeName);
            mockedGraphUtils.when(() -> AtlasGraphUtilsV2.relationshipTypeHasInstanceEdges(typeName)).thenReturn(true);

            try {
                mockRelationshipDefStore.preDeleteByGuid(guid);
                fail("Expected AtlasBaseException");
            } catch (AtlasBaseException e) {
                assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.TYPE_HAS_REFERENCES);
            }
        }
    }

    @Test
    public void testToRelationshipDefPrivateMethod() throws Exception {
        setupMocksForToRelationshipDef();

        Method toRelationshipDefMethod = AtlasRelationshipDefStoreV2.class.getDeclaredMethod("toRelationshipDef", AtlasVertex.class);
        toRelationshipDefMethod.setAccessible(true);

        AtlasRelationshipDef result = (AtlasRelationshipDef) toRelationshipDefMethod.invoke(mockRelationshipDefStore, mockVertex);

        assertNotNull(result);
        assertEquals(result.getName(), "TestRelationship");
        assertEquals(result.getRelationshipCategory(), RelationshipCategory.ASSOCIATION);
        assertEquals(result.getPropagateTags(), PropagateTags.BOTH);
    }

    @Test
    public void testToRelationshipDefWithNullVertex() throws Exception {
        Method toRelationshipDefMethod = AtlasRelationshipDefStoreV2.class.getDeclaredMethod("toRelationshipDef", AtlasVertex.class);
        toRelationshipDefMethod.setAccessible(true);

        AtlasRelationshipDef result = (AtlasRelationshipDef) toRelationshipDefMethod.invoke(mockRelationshipDefStore, (AtlasVertex) null);

        assertNull(result);
    }

    @Test
    public void testToRelationshipDefWithInvalidVertex() throws Exception {
        when(mockTypeDefStore.isTypeVertex(mockVertex, TypeCategory.RELATIONSHIP)).thenReturn(false);

        Method toRelationshipDefMethod = AtlasRelationshipDefStoreV2.class.getDeclaredMethod("toRelationshipDef", AtlasVertex.class);
        toRelationshipDefMethod.setAccessible(true);

        AtlasRelationshipDef result = (AtlasRelationshipDef) toRelationshipDefMethod.invoke(mockRelationshipDefStore, mockVertex);

        assertNull(result);
    }

    @Test
    public void testPreUpdateCheckInTypePatching() throws Exception {
        AtlasRelationshipDef existingDef = createValidRelationshipDef();
        AtlasRelationshipDef newDef = createValidRelationshipDef();
        newDef.setRelationshipCategory(RelationshipCategory.AGGREGATION);

        try (MockedStatic<RequestContext> mockedRequestContext = mockStatic(RequestContext.class)) {
            RequestContext mockContext = mock(RequestContext.class);
            mockedRequestContext.when(RequestContext::get).thenReturn(mockContext);
            when(mockContext.isInTypePatching()).thenReturn(true);

            AtlasRelationshipDefStoreV2.preUpdateCheck(newDef, existingDef);
        }
    }

    @Test
    public void testPreUpdateCheckWithEndSwapping() throws Exception {
        AtlasRelationshipDef existingDef = new AtlasRelationshipDef();
        existingDef.setName("TestRelationship");
        existingDef.setRelationshipCategory(RelationshipCategory.ASSOCIATION);
        existingDef.setEndDef1(new AtlasRelationshipEndDef("typeA", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE));
        existingDef.setEndDef2(new AtlasRelationshipEndDef("typeB", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE));

        AtlasRelationshipDef newDef = new AtlasRelationshipDef();
        newDef.setName("TestRelationship");
        newDef.setRelationshipCategory(RelationshipCategory.ASSOCIATION);
        newDef.setEndDef1(new AtlasRelationshipEndDef("typeB", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE)); // Swapped
        newDef.setEndDef2(new AtlasRelationshipEndDef("typeA", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE)); // Swapped

        try (MockedStatic<RequestContext> mockedRequestContext = mockStatic(RequestContext.class)) {
            RequestContext mockContext = mock(RequestContext.class);
            mockedRequestContext.when(RequestContext::get).thenReturn(mockContext);
            when(mockContext.isInTypePatching()).thenReturn(true);

            AtlasRelationshipDefStoreV2.preUpdateCheck(newDef, existingDef);
        }
    }

    @Test
    public void testIsValidUpdatePrivateMethod() throws Exception {
        AtlasRelationshipEndDef currentDef = new AtlasRelationshipEndDef("typeA", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        currentDef.setIsContainer(true);
        currentDef.setDescription("original description");
        currentDef.setIsLegacyAttribute(true);

        AtlasRelationshipEndDef updatedDef = new AtlasRelationshipEndDef("typeA", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        updatedDef.setIsContainer(true);
        updatedDef.setDescription("updated description"); // Different description
        updatedDef.setIsLegacyAttribute(false); // Different legacy attribute

        Method isValidUpdateMethod = AtlasRelationshipDefStoreV2.class.getDeclaredMethod("isValidUpdate", AtlasRelationshipEndDef.class, AtlasRelationshipEndDef.class);
        isValidUpdateMethod.setAccessible(true);

        boolean result = (Boolean) isValidUpdateMethod.invoke(null, currentDef, updatedDef);

        assertEquals(result, true); // Should be valid since description and isLegacyAttribute can be updated
    }

    @Test
    public void testIsValidUpdateWithInvalidChanges() throws Exception {
        AtlasRelationshipEndDef currentDef = new AtlasRelationshipEndDef("typeA", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        AtlasRelationshipEndDef updatedDef = new AtlasRelationshipEndDef("typeB", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE); // Different type

        Method isValidUpdateMethod = AtlasRelationshipDefStoreV2.class.getDeclaredMethod("isValidUpdate", AtlasRelationshipEndDef.class, AtlasRelationshipEndDef.class);
        isValidUpdateMethod.setAccessible(true);

        boolean result = (Boolean) isValidUpdateMethod.invoke(null, currentDef, updatedDef);

        assertEquals(result, false);
    }

    @Test
    public void testVerifyTypeReadAccessPrivateMethod() throws Exception {
        AtlasRelationshipDef relationshipDef = createValidRelationshipDef();

        Method verifyTypeReadAccessMethod = AtlasRelationshipDefStoreV2.class.getDeclaredMethod("verifyTypeReadAccess", AtlasRelationshipDef.class);
        verifyTypeReadAccessMethod.setAccessible(true);

        verifyTypeReadAccessMethod.invoke(mockRelationshipDefStore, relationshipDef);
    }

    private AtlasRelationshipDef createValidRelationshipDef() {
        AtlasRelationshipDef relationshipDef = new AtlasRelationshipDef();
        relationshipDef.setName("TestRelationship");
        relationshipDef.setDescription("Test description");
        relationshipDef.setVersion(1L);
        relationshipDef.setRelationshipCategory(RelationshipCategory.ASSOCIATION);
        relationshipDef.setPropagateTags(PropagateTags.BOTH);
        relationshipDef.setEndDef1(new AtlasRelationshipEndDef("typeA", "attr1", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE));
        relationshipDef.setEndDef2(new AtlasRelationshipEndDef("typeB", "attr2", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE));
        return relationshipDef;
    }

    private void setupMocksForToRelationshipDef() throws Exception {
        when(mockTypeDefStore.isTypeVertex(mockVertex, TypeCategory.RELATIONSHIP)).thenReturn(true);
        when(mockVertex.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class)).thenReturn("TestRelationship");
        when(mockVertex.getProperty(Constants.TYPEDESCRIPTION_PROPERTY_KEY, String.class)).thenReturn("Test description");
        when(mockVertex.getProperty(Constants.TYPEVERSION_PROPERTY_KEY, String.class)).thenReturn("1.0");
        when(mockVertex.getProperty(Constants.RELATIONSHIPTYPE_LABEL_KEY, String.class)).thenReturn("testLabel");
        when(mockVertex.getProperty(Constants.RELATIONSHIPTYPE_END1_KEY, String.class)).thenReturn("{\"type\":\"typeA\",\"name\":\"attr1\",\"cardinality\":\"SINGLE\"}");
        when(mockVertex.getProperty(Constants.RELATIONSHIPTYPE_END2_KEY, String.class)).thenReturn("{\"type\":\"typeB\",\"name\":\"attr2\",\"cardinality\":\"SINGLE\"}");
        when(mockVertex.getProperty(Constants.RELATIONSHIPTYPE_CATEGORY_KEY, String.class)).thenReturn("ASSOCIATION");
        when(mockVertex.getProperty(Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, String.class)).thenReturn("BOTH");

        try (MockedStatic<AtlasStructDefStoreV2> mockedStructDefStore = mockStatic(AtlasStructDefStoreV2.class)) {
            mockedStructDefStore.when(() -> AtlasStructDefStoreV2.toStructDef(any(AtlasVertex.class), any(AtlasRelationshipDef.class), any(AtlasTypeDefGraphStoreV2.class)))
                    .thenAnswer(invocation -> null);
        }
    }

    private void setupMocksForUpdate(AtlasRelationshipDef relationshipDef) throws Exception {
        when(mockTypeRegistry.getRelationshipDefByName(relationshipDef.getName())).thenReturn(relationshipDef);
        when(mockTypeRegistry.getRelationshipDefByGuid(anyString())).thenReturn(relationshipDef);
        when(mockTypeRegistry.getType(relationshipDef.getName())).thenReturn(mockRelationshipType);
        when(mockTypeRegistry.getTypeByGuid(anyString())).thenReturn(mockRelationshipType);
        when(mockRelationshipType.getTypeCategory()).thenReturn(org.apache.atlas.model.TypeCategory.RELATIONSHIP);
        when(mockTypeDefStore.findTypeVertexByNameAndCategory(relationshipDef.getName(), TypeCategory.RELATIONSHIP)).thenReturn(mockVertex);
        when(mockTypeDefStore.findTypeVertexByGuidAndCategory(anyString(), eq(TypeCategory.RELATIONSHIP))).thenReturn(mockVertex);
        setupMocksForToRelationshipDef();

        try (MockedStatic<AtlasAuthorizationUtils> mockedAuthUtils = mockStatic(AtlasAuthorizationUtils.class); MockedStatic<AtlasStructDefStoreV2> mockedStructDefStore = mockStatic(AtlasStructDefStoreV2.class)) {
            mockedAuthUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasTypeAccessRequest.class), anyString(), anyString())).thenAnswer(invocation -> null);
            mockedStructDefStore.when(() -> AtlasStructDefStoreV2.updateVertexPreUpdate(any(AtlasRelationshipDef.class), any(AtlasRelationshipType.class), any(AtlasVertex.class), any(AtlasTypeDefGraphStoreV2.class))).thenAnswer(invocation -> null);
        }
    }

    @BeforeClass
    public void initialize() throws Exception {
        super.initialize();
    }

    @AfterClass
    public void clear() throws Exception {
        AtlasGraphProvider.cleanup();

        super.cleanup();
    }
}
