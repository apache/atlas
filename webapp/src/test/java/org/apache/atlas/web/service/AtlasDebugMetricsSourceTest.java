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

package org.apache.atlas.web.service;

import org.apache.hadoop.metrics2.lib.MutableRate;
import org.aspectj.lang.Signature;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AtlasDebugMetricsSourceTest {
    @Mock
    private Signature signature;

    private AtlasDebugMetricsSource debugMetricsSource;

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        debugMetricsSource = new AtlasDebugMetricsSource();

        // Initialize the MutableRate fields using reflection to avoid NullPointerExceptions
        initializeMutableRateFields();
    }

    private void initializeMutableRateFields() throws Exception {
        Field[] fields = AtlasDebugMetricsSource.class.getDeclaredFields();
        for (Field field : fields) {
            if (field.getType().equals(MutableRate.class)) {
                field.setAccessible(true);
                if (field.get(debugMetricsSource) == null) {
                    MutableRate mockRate = mock(MutableRate.class);
                    field.set(debugMetricsSource, mockRate);
                }
            }
        }
    }

    @Test
    public void testConstructor() throws Exception {
        assertNotNull(debugMetricsSource);

        // Verify that static fields are initialized
        Field fieldMapField = AtlasDebugMetricsSource.class.getDeclaredField("fieldLowerCaseUpperCaseMap");
        fieldMapField.setAccessible(true);
        Map<String, String> fieldMap = (Map<String, String>) fieldMapField.get(null);
        assertNotNull(fieldMap);
        assertFalse(fieldMap.isEmpty());

        Field debugMetricsAttributesField = AtlasDebugMetricsSource.class.getDeclaredField("debugMetricsAttributes");
        debugMetricsAttributesField.setAccessible(true);
        Set<String> debugMetricsAttributes = (Set<String>) debugMetricsAttributesField.get(null);
        assertNotNull(debugMetricsAttributes);
        assertFalse(debugMetricsAttributes.isEmpty());
        assertTrue(debugMetricsAttributes.contains("numops"));
        assertTrue(debugMetricsAttributes.contains("mintime"));
        assertTrue(debugMetricsAttributes.contains("maxtime"));
        assertTrue(debugMetricsAttributes.contains("stdevtime"));
        assertTrue(debugMetricsAttributes.contains("avgtime"));
    }

    @Test
    public void testUpdateWithFullSignature() throws Exception {
        when(signature.toString()).thenReturn("EntityMutationResponse org.apache.atlas.web.rest.EntityREST.createOrUpdate(AtlasEntitiesWithExtInfo)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("entityREST_createOrUpdateBulk");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithAddOrUpdateBMByName() throws Exception {
        when(signature.toString()).thenReturn("void org.apache.atlas.web.rest.EntityREST.addOrUpdateBusinessAttributes(String,String,Map)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("entityREST_addOrUpdateBusinessAttributesByName");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithRemoveBMByName() throws Exception {
        when(signature.toString()).thenReturn("void org.apache.atlas.web.rest.EntityREST.removeBusinessAttributes(String,String,Map)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("entityREST_removeBusinessAttributesByName");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithRemoveLabelsByTypeName() throws Exception {
        when(signature.toString()).thenReturn("void org.apache.atlas.web.rest.EntityREST.removeLabels(String,Set,HttpServletRequest)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("entityREST_removeLabelsByTypeName");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithSetLabelsByTypeName() throws Exception {
        when(signature.toString()).thenReturn("void org.apache.atlas.web.rest.EntityREST.setLabels(String,Set,HttpServletRequest)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("entityREST_setLabelsByTypeName");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithAddLabelsByTypeName() throws Exception {
        when(signature.toString()).thenReturn("void org.apache.atlas.web.rest.EntityREST.addLabels(String,Set,HttpServletRequest)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("entityREST_addLabelsByTypeName");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithQuickSearchQuickSearchParams() throws Exception {
        when(signature.toString()).thenReturn("AtlasQuickSearchResult org.apache.atlas.web.rest.DiscoveryREST.quickSearch(QuickSearchParameters)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("discoveryREST_quickSearchQuickSearchParams");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithShortSignature() throws Exception {
        when(signature.toString()).thenReturn("EntityREST.getById(..)");
        when(signature.toShortString()).thenReturn("EntityREST.getById(..)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("entityREST_getById");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithRelationshipRESTCreate() throws Exception {
        when(signature.toString()).thenReturn("RelationshipREST.create(..)");
        when(signature.toShortString()).thenReturn("RelationshipREST.create(..)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("relationshipREST_create");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithRelationshipRESTUpdate() throws Exception {
        when(signature.toString()).thenReturn("RelationshipREST.update(..)");
        when(signature.toShortString()).thenReturn("RelationshipREST.update(..)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("relationshipREST_update");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithRelationshipRESTGetById() throws Exception {
        when(signature.toString()).thenReturn("RelationshipREST.getById(..)");
        when(signature.toShortString()).thenReturn("RelationshipREST.getById(..)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("relationshipREST_getById");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithRelationshipRESTDeleteById() throws Exception {
        when(signature.toString()).thenReturn("RelationshipREST.deleteById(..)");
        when(signature.toShortString()).thenReturn("RelationshipREST.deleteById(..)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("relationshipREST_deleteById");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithDiscoveryRESTSearchUsingFullText() throws Exception {
        when(signature.toString()).thenReturn("DiscoveryREST.searchUsingFullText(..)");
        when(signature.toShortString()).thenReturn("DiscoveryREST.searchUsingFullText(..)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("discoveryREST_searchUsingFullText");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithGlossaryRESTGetGlossaries() throws Exception {
        when(signature.toString()).thenReturn("GlossaryREST.getGlossaries(..)");
        when(signature.toShortString()).thenReturn("GlossaryREST.getGlossaries(..)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("glossaryREST_getGlossaries");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithTypesRESTGetTypeDefByName() throws Exception {
        when(signature.toString()).thenReturn("TypesREST.getTypeDefByName(..)");
        when(signature.toShortString()).thenReturn("TypesREST.getTypeDefByName(..)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("typesREST_getTypeDefByName");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithNotificationHookConsumerDoWork() throws Exception {
        when(signature.toString()).thenReturn("NotificationHookConsumer.doWork(..)");
        when(signature.toShortString()).thenReturn("NotificationHookConsumer.doWork(..)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("notificationHookConsumer_doWork");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithLineageRESTGetLineageByUA() throws Exception {
        when(signature.toString()).thenReturn("LineageREST.getLineageByUniqueAttribute(..)");
        when(signature.toShortString()).thenReturn("LineageREST.getLineageByUniqueAttribute(..)");

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("lineageREST_getLineageByUniqAttr");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testUpdateWithUnknownSignature() throws Exception {
        when(signature.toString()).thenReturn("UnknownClass.unknownMethod(..)");
        when(signature.toShortString()).thenReturn("UnknownClass.unknownMethod(..)");

        // This should not throw an exception, but should log an error
        debugMetricsSource.update(signature, 100L);
    }

    @Test
    public void testInitAttrList() throws Exception {
        Method method = AtlasDebugMetricsSource.class.getDeclaredMethod("initAttrList");
        method.setAccessible(true);
        method.invoke(debugMetricsSource);

        Field debugMetricsAttributesField = AtlasDebugMetricsSource.class.getDeclaredField("debugMetricsAttributes");
        debugMetricsAttributesField.setAccessible(true);
        Set<String> debugMetricsAttributes = (Set<String>) debugMetricsAttributesField.get(null);

        assertTrue(debugMetricsAttributes.contains("numops"));
        assertTrue(debugMetricsAttributes.contains("mintime"));
        assertTrue(debugMetricsAttributes.contains("maxtime"));
        assertTrue(debugMetricsAttributes.contains("stdevtime"));
        assertTrue(debugMetricsAttributes.contains("avgtime"));
    }

    @Test
    public void testPopulateFieldList() throws Exception {
        Method method = AtlasDebugMetricsSource.class.getDeclaredMethod("populateFieldList");
        method.setAccessible(true);
        method.invoke(debugMetricsSource);

        Field fieldMapField = AtlasDebugMetricsSource.class.getDeclaredField("fieldLowerCaseUpperCaseMap");
        fieldMapField.setAccessible(true);
        Map<String, String> fieldMap = (Map<String, String>) fieldMapField.get(null);

        assertFalse(fieldMap.isEmpty());
        // Check that some expected mappings exist
        assertTrue(fieldMap.containsKey("entityrest_getbyidnumops"));
        assertTrue(fieldMap.containsKey("entityrest_getbyidmintime"));
    }

    @Test
    public void testUpdateStringMethod() throws Exception {
        Method method = AtlasDebugMetricsSource.class.getDeclaredMethod("update", String.class, Long.class);
        method.setAccessible(true);

        // Test that the method doesn't throw an exception
        method.invoke(debugMetricsSource, "EntityREST.getById(..)", 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("entityREST_getById");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testStaticFieldsInitialization() throws Exception {
        Field fieldMapField = AtlasDebugMetricsSource.class.getDeclaredField("fieldLowerCaseUpperCaseMap");
        fieldMapField.setAccessible(true);
        Map<String, String> fieldMap = (Map<String, String>) fieldMapField.get(null);
        assertNotNull(fieldMap);

        Field debugMetricsAttributesField = AtlasDebugMetricsSource.class.getDeclaredField("debugMetricsAttributes");
        debugMetricsAttributesField.setAccessible(true);
        Set<String> debugMetricsAttributes = (Set<String>) debugMetricsAttributesField.get(null);
        assertNotNull(debugMetricsAttributes);
        assertEquals(debugMetricsAttributes.size(), 5);
    }

    @Test
    public void testAllEntityRESTMethods() throws Exception {
        testEntityRESTMethod("EntityREST.createOrUpdate(..)", "entityREST_createOrUpdate");
        testEntityRESTMethod("EntityREST.partialUpdateEntityAttrByGuid(..)", "entityREST_partialUpdateEntityAttrByGuid");
        testEntityRESTMethod("EntityREST.deleteByGuid(..)", "entityREST_deleteByGuid");
        testEntityRESTMethod("EntityREST.getClassification(..)", "entityREST_getClassification");
        testEntityRESTMethod("EntityREST.getClassifications(..)", "entityREST_getClassifications");
        testEntityRESTMethod("EntityREST.addClassificationsByUniqueAttribute(..)", "entityREST_addClassificationsByUniqAttr");
        testEntityRESTMethod("EntityREST.addClassifications(..)", "entityREST_addClassifications");
        testEntityRESTMethod("EntityREST.deleteClassificationByUniqueAttribute(..)", "entityREST_deleteClassificationByUniqAttr");
        testEntityRESTMethod("EntityREST.deleteClassification(..)", "entityREST_deleteClassification");
        testEntityRESTMethod("EntityREST.getHeaderById(..)", "entityREST_getHeaderById");
        testEntityRESTMethod("EntityREST.getEntityHeaderByUniqueAttributes(..)", "entityREST_getEntityHeaderByUniqAttr");
        testEntityRESTMethod("EntityREST.getByUniqueAttributes(..)", "entityREST_getByUniqueAttributes");
        testEntityRESTMethod("EntityREST.partialUpdateEntityByUniqueAttrs(..)", "entityREST_partialUpdateEntityByUniqAttr");
        testEntityRESTMethod("EntityREST.deleteByUniqueAttribute(..)", "entityREST_deleteByUniqAttr");
        testEntityRESTMethod("EntityREST.updateClassificationsByUniqueAttribute(..)", "entityREST_updateClassificationsByUniqAttr");
        testEntityRESTMethod("EntityREST.updateClassifications(..)", "entityREST_updateClassifications");
        testEntityRESTMethod("EntityREST.getEntitiesByUniqueAttributes(..)", "entityREST_getEntitiesByUniqAttr");
        testEntityRESTMethod("EntityREST.getByGuids(..)", "entityREST_getByGuids");
        testEntityRESTMethod("EntityREST.deleteByGuids(..)", "entityREST_deleteByGuids");
        testEntityRESTMethod("EntityREST.addClassification(..)", "entityREST_addClassification");
        testEntityRESTMethod("EntityREST.getAuditEvents(..)", "entityREST_getAuditEvents");
        testEntityRESTMethod("EntityREST.getEntityHeaders(..)", "entityREST_getEntityHeaders");
        testEntityRESTMethod("EntityREST.setClassifications(..)", "entityREST_setClassifications");
        testEntityRESTMethod("EntityREST.addOrUpdateBusinessAttributes(..)", "entityREST_addOrUpdateBusinessAttributes");
        testEntityRESTMethod("EntityREST.removeBusinessAttributes(..)", "entityREST_removeBusinessAttributes");
        testEntityRESTMethod("EntityREST.setLabels(..)", "entityREST_setLabels");
        testEntityRESTMethod("EntityREST.addLabels(..)", "entityREST_addLabels");
        testEntityRESTMethod("EntityREST.removeLabels(..)", "entityREST_removeLabels");
        testEntityRESTMethod("EntityREST.importBMAttributes(..)", "entityREST_importBMAttributes");
    }

    private void testEntityRESTMethod(String methodName, String fieldName) throws Exception {
        when(signature.toString()).thenReturn(methodName);
        when(signature.toShortString()).thenReturn(methodName);

        // Test that the method doesn't throw an exception
        debugMetricsSource.update(signature, 100L);

        // Verify that the field exists and is accessible
        Field field = AtlasDebugMetricsSource.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testAllTypesRESTMethods() throws Exception {
        testTypesRESTMethod("TypesREST.getTypeDefByName(..)", "typesREST_getTypeDefByName");
        testTypesRESTMethod("TypesREST.getTypeDefByGuid(..)", "typesREST_getTypeDefByGuid");
        testTypesRESTMethod("TypesREST.getTypeDefHeaders(..)", "typesREST_getTypeDefHeaders");
        testTypesRESTMethod("TypesREST.getAllTypeDefs(..)", "typesREST_getAllTypeDefs");
        testTypesRESTMethod("TypesREST.getEnumDefByName(..)", "typesREST_getEnumDefByName");
        testTypesRESTMethod("TypesREST.getEnumDefByGuid(..)", "typesREST_getEnumDefByGuid");
        testTypesRESTMethod("TypesREST.getStructDefByName(..)", "typesREST_getStructDefByName");
        testTypesRESTMethod("TypesREST.getStructDefByGuid(..)", "typesREST_getStructDefByGuid");
        testTypesRESTMethod("TypesREST.getClassificationDefByName(..)", "typesREST_getClassificationDefByName");
        testTypesRESTMethod("TypesREST.getClassificationDefByGuid(..)", "typesREST_getClassificationDefByGuid");
        testTypesRESTMethod("TypesREST.getEntityDefByGuid(..)", "typesREST_getEntityDefByGuid");
        testTypesRESTMethod("TypesREST.getRelationshipDefByName(..)", "typesREST_getRelationshipDefByName");
        testTypesRESTMethod("TypesREST.getRelationshipDefByGuid(..)", "typesREST_getRelationshipDefByGuid");
        testTypesRESTMethod("TypesREST.getBusinessMetadataDefByGuid(..)", "typesREST_getBusinessMetadataDefByGuid");
        testTypesRESTMethod("TypesREST.getBusinessMetadataDefByName(..)", "typesREST_getBusinessMetadataDefByName");
        testTypesRESTMethod("TypesREST.createAtlasTypeDefs(..)", "typesREST_createAtlasTypeDefs");
        testTypesRESTMethod("TypesREST.updateAtlasTypeDefs(..)", "typesREST_updateAtlasTypeDefs");
        testTypesRESTMethod("TypesREST.deleteAtlasTypeDefs(..)", "typesREST_deleteAtlasTypeDefs");
        testTypesRESTMethod("TypesREST.deleteAtlasTypeByName(..)", "typesREST_deleteAtlasTypeByName");
    }

    private void testTypesRESTMethod(String methodName, String fieldName) throws Exception {
        when(signature.toString()).thenReturn(methodName);
        when(signature.toShortString()).thenReturn(methodName);

        debugMetricsSource.update(signature, 150L);

        Field field = AtlasDebugMetricsSource.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testAllGlossaryRESTMethods() throws Exception {
        testGlossaryRESTMethod("GlossaryREST.getGlossary(..)", "glossaryREST_getGlossary");
        testGlossaryRESTMethod("GlossaryREST.getDetailedGlossary(..)", "glossaryREST_getDetailedGlossary");
        testGlossaryRESTMethod("GlossaryREST.getGlossaryTerm(..)", "glossaryREST_getGlossaryTerm");
        testGlossaryRESTMethod("GlossaryREST.getGlossaryCategory(..)", "glossaryREST_getGlossaryCategory");
        testGlossaryRESTMethod("GlossaryREST.createGlossary(..)", "glossaryREST_createGlossary");
        testGlossaryRESTMethod("GlossaryREST.createGlossaryTerm(..)", "glossaryREST_createGlossaryTerm");
        testGlossaryRESTMethod("GlossaryREST.createGlossaryTerms(..)", "glossaryREST_createGlossaryTerms");
        testGlossaryRESTMethod("GlossaryREST.createGlossaryCategory(..)", "glossaryREST_createGlossaryCategory");
        testGlossaryRESTMethod("GlossaryREST.createGlossaryCategories(..)", "glossaryREST_createGlossaryCategories");
        testGlossaryRESTMethod("GlossaryREST.updateGlossary(..)", "glossaryREST_updateGlossary");
        testGlossaryRESTMethod("GlossaryREST.partialUpdateGlossary(..)", "glossaryREST_partialUpdateGlossary");
        testGlossaryRESTMethod("GlossaryREST.updateGlossaryTerm(..)", "glossaryREST_updateGlossaryTerm");
        testGlossaryRESTMethod("GlossaryREST.partialUpdateGlossaryTerm(..)", "glossaryREST_partialUpdateGlossaryTerm");
        testGlossaryRESTMethod("GlossaryREST.updateGlossaryCategory(..)", "glossaryREST_updateGlossaryCategory");
        testGlossaryRESTMethod("GlossaryREST.partialUpdateGlossaryCategory(..)", "glossaryREST_partialUpdateGlossaryCategory");
        testGlossaryRESTMethod("GlossaryREST.deleteGlossary(..)", "glossaryREST_deleteGlossary");
        testGlossaryRESTMethod("GlossaryREST.deleteGlossaryTerm(..)", "glossaryREST_deleteGlossaryTerm");
        testGlossaryRESTMethod("GlossaryREST.deleteGlossaryCategory(..)", "glossaryREST_deleteGlossaryCategory");
        testGlossaryRESTMethod("GlossaryREST.getGlossaryTerms(..)", "glossaryREST_getGlossaryTerms");
        testGlossaryRESTMethod("GlossaryREST.getGlossaryTermHeaders(..)", "glossaryREST_getGlossaryTermHeaders");
        testGlossaryRESTMethod("GlossaryREST.getGlossaryCategories(..)", "glossaryREST_getGlossaryCategories");
        testGlossaryRESTMethod("GlossaryREST.getGlossaryCategoriesHeaders(..)", "glossaryREST_getGlossaryCategoriesHeaders");
        testGlossaryRESTMethod("GlossaryREST.getCategoryTerms(..)", "glossaryREST_getCategoryTerms");
        testGlossaryRESTMethod("GlossaryREST.getRelatedTerms(..)", "glossaryREST_getRelatedTerms");
        testGlossaryRESTMethod("GlossaryREST.getEntitiesAssignedWithTerm(..)", "glossaryREST_getEntitiesAssignedWithTerm");
        testGlossaryRESTMethod("GlossaryREST.assignTermToEntities(..)", "glossaryREST_assignTermToEntities");
        testGlossaryRESTMethod("GlossaryREST.removeTermAssignmentFromEntities(..)", "glossaryREST_removeTermAssignmentFromEntities");
        testGlossaryRESTMethod("GlossaryREST.disassociateTermAssignmentFromEntities(..)", "glossaryREST_disassociateTermAssignmentFromEntities");
        testGlossaryRESTMethod("GlossaryREST.getRelatedCategories(..)", "glossaryREST_getRelatedCategories");
        testGlossaryRESTMethod("GlossaryREST.importGlossaryData(..)", "glossaryREST_importGlossaryData");
    }

    private void testGlossaryRESTMethod(String methodName, String fieldName) throws Exception {
        when(signature.toString()).thenReturn(methodName);
        when(signature.toShortString()).thenReturn(methodName);

        debugMetricsSource.update(signature, 200L);

        Field field = AtlasDebugMetricsSource.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testAllDiscoveryRESTMethods() throws Exception {
        testDiscoveryRESTMethod("DiscoveryREST.searchUsingAttribute(..)", "discoveryREST_searchUsingAttribute");
        testDiscoveryRESTMethod("DiscoveryREST.searchWithParameters(..)", "discoveryREST_searchWithParameters");
        testDiscoveryRESTMethod("DiscoveryREST.searchRelatedEntities(..)", "discoveryREST_searchRelatedEntities");
        testDiscoveryRESTMethod("DiscoveryREST.updateSavedSearch(..)", "discoveryREST_updateSavedSearch");
        testDiscoveryRESTMethod("DiscoveryREST.getSavedSearch(..)", "discoveryREST_getSavedSearch");
        testDiscoveryRESTMethod("DiscoveryREST.getSavedSearches(..)", "discoveryREST_getSavedSearches");
        testDiscoveryRESTMethod("DiscoveryREST.deleteSavedSearch(..)", "discoveryREST_deleteSavedSearch");
        testDiscoveryRESTMethod("DiscoveryREST.executeSavedSearchByName(..)", "discoveryREST_executeSavedSearchByName");
        testDiscoveryRESTMethod("DiscoveryREST.executeSavedSearchByGuid(..)", "discoveryREST_executeSavedSearchByGuid");
        testDiscoveryRESTMethod("DiscoveryREST.getSuggestions(..)", "discoveryREST_getSuggestions");
        testDiscoveryRESTMethod("DiscoveryREST.searchUsingDSL(..)", "discoveryREST_searchUsingDSL");
        testDiscoveryRESTMethod("DiscoveryREST.searchUsingBasic(..)", "discoveryREST_searchUsingBasic");
        testDiscoveryRESTMethod("DiscoveryREST.quickSearch(..)", "discoveryREST_quickSearch");
        testDiscoveryRESTMethod("DiscoveryREST.addSavedSearch(..)", "discoveryREST_addSavedSearch");
    }

    private void testDiscoveryRESTMethod(String methodName, String fieldName) throws Exception {
        when(signature.toString()).thenReturn(methodName);
        when(signature.toShortString()).thenReturn(methodName);

        debugMetricsSource.update(signature, 175L);

        Field field = AtlasDebugMetricsSource.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testPrivateUpdateMethod() throws Exception {
        Method method = AtlasDebugMetricsSource.class.getDeclaredMethod("update", String.class, Long.class);
        method.setAccessible(true);

        // Test all the cases in the private update method
        method.invoke(debugMetricsSource, "RelationshipREST.create(..)", 100L);
        method.invoke(debugMetricsSource, "RelationshipREST.update(..)", 100L);
        method.invoke(debugMetricsSource, "RelationshipREST.getById(..)", 100L);
        method.invoke(debugMetricsSource, "RelationshipREST.deleteById(..)", 100L);
        method.invoke(debugMetricsSource, "DiscoveryREST.searchUsingFullText(..)", 100L);
        method.invoke(debugMetricsSource, "DiscoveryREST.searchUsingAttribute(..)", 100L);
        method.invoke(debugMetricsSource, "DiscoveryREST.searchWithParameters(..)", 100L);
        method.invoke(debugMetricsSource, "DiscoveryREST.searchRelatedEntities(..)", 100L);
        method.invoke(debugMetricsSource, "DiscoveryREST.updateSavedSearch(..)", 100L);
        method.invoke(debugMetricsSource, "DiscoveryREST.getSavedSearch(..)", 100L);
        method.invoke(debugMetricsSource, "DiscoveryREST.getSavedSearches(..)", 100L);
        method.invoke(debugMetricsSource, "DiscoveryREST.deleteSavedSearch(..)", 100L);
        method.invoke(debugMetricsSource, "DiscoveryREST.executeSavedSearchByName(..)", 100L);
        method.invoke(debugMetricsSource, "DiscoveryREST.executeSavedSearchByGuid(..)", 100L);
        method.invoke(debugMetricsSource, "DiscoveryREST.getSuggestions(..)", 100L);

        // Verify some of the fields were updated
        Field field = AtlasDebugMetricsSource.class.getDeclaredField("relationshipREST_create");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testStaticFieldsAccess() throws Exception {
        Field fieldMapField = AtlasDebugMetricsSource.class.getDeclaredField("fieldLowerCaseUpperCaseMap");
        fieldMapField.setAccessible(true);
        Map<String, String> fieldMap = (Map<String, String>) fieldMapField.get(null);
        assertNotNull(fieldMap);
        assertFalse(fieldMap.isEmpty());

        Field debugMetricsAttributesField = AtlasDebugMetricsSource.class.getDeclaredField("debugMetricsAttributes");
        debugMetricsAttributesField.setAccessible(true);
        Set<String> debugMetricsAttributes = (Set<String>) debugMetricsAttributesField.get(null);
        assertNotNull(debugMetricsAttributes);
        assertEquals(debugMetricsAttributes.size(), 5);
    }

    @Test
    public void testLineageRESTMethods() throws Exception {
        when(signature.toString()).thenReturn("LineageREST.getLineageGraph(..)");
        when(signature.toShortString()).thenReturn("LineageREST.getLineageGraph(..)");

        debugMetricsSource.update(signature, 120L);

        Field field = AtlasDebugMetricsSource.class.getDeclaredField("lineageREST_getLineageGraph");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }

    @Test
    public void testConstructorAndInitialization() throws Exception {
        AtlasDebugMetricsSource newSource = new AtlasDebugMetricsSource();
        assertNotNull(newSource);

        // Verify initialization was called
        Field debugMetricsAttributesField = AtlasDebugMetricsSource.class.getDeclaredField("debugMetricsAttributes");
        debugMetricsAttributesField.setAccessible(true);
        Set<String> debugMetricsAttributes = (Set<String>) debugMetricsAttributesField.get(null);
        assertTrue(debugMetricsAttributes.contains("numops"));
        assertTrue(debugMetricsAttributes.contains("mintime"));
        assertTrue(debugMetricsAttributes.contains("maxtime"));
        assertTrue(debugMetricsAttributes.contains("stdevtime"));
        assertTrue(debugMetricsAttributes.contains("avgtime"));
    }

    @Test
    public void testMetricFieldsExist() throws Exception {
        // Test that all major metric fields exist and can be accessed
        String[] fieldNames = {
            "entityREST_getById", "entityREST_createOrUpdate", "entityREST_deleteByGuid",
            "typesREST_getTypeDefByName", "typesREST_getAllTypeDefs",
            "glossaryREST_getGlossaries", "glossaryREST_createGlossary",
            "discoveryREST_searchUsingFullText", "discoveryREST_quickSearch",
            "relationshipREST_create", "relationshipREST_update",
            "lineageREST_getLineageByUniqAttr", "notificationHookConsumer_doWork"
        };

        for (String fieldName : fieldNames) {
            Field field = AtlasDebugMetricsSource.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
            assertNotNull(mutableRate);
        }
    }

    @Test
    public void testUpdateWithVariousTimings() throws Exception {
        when(signature.toString()).thenReturn("EntityREST.getById(..)");
        when(signature.toShortString()).thenReturn("EntityREST.getById(..)");

        // Test with different timing values
        debugMetricsSource.update(signature, 0L);
        debugMetricsSource.update(signature, 1L);
        debugMetricsSource.update(signature, 1000L);
        debugMetricsSource.update(signature, Long.MAX_VALUE);

        Field field = AtlasDebugMetricsSource.class.getDeclaredField("entityREST_getById");
        field.setAccessible(true);
        MutableRate mutableRate = (MutableRate) field.get(debugMetricsSource);
        assertNotNull(mutableRate);
    }
}
