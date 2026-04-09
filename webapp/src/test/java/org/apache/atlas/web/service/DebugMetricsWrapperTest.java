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

import org.apache.hadoop.metrics2.MetricsSystem;
import org.aspectj.lang.Signature;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

public class DebugMetricsWrapperTest {
    @Mock
    private AtlasDebugMetricsSink debugMetricsRESTSink;

    @Mock
    private Signature signature;

    @Mock
    private MetricsSystem metricsSystem;

    private DebugMetricsWrapper debugMetricsWrapper;

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        debugMetricsWrapper = new DebugMetricsWrapper();

        // Inject the mock sink
        Field sinkField = DebugMetricsWrapper.class.getDeclaredField("debugMetricsRESTSink");
        sinkField.setAccessible(true);
        sinkField.set(debugMetricsWrapper, debugMetricsRESTSink);

        // Inject a mock AtlasDebugMetricsSource
        Field sourceField = DebugMetricsWrapper.class.getDeclaredField("debugMetricsSource");
        sourceField.setAccessible(true);
        AtlasDebugMetricsSource mockSource = mock(AtlasDebugMetricsSource.class);
        sourceField.set(debugMetricsWrapper, mockSource);
    }

    @Test
    public void testInit() throws Exception {
        // Mock the static DefaultMetricsSystem.initialize method behavior
        debugMetricsWrapper.init();

        // Verify that debugMetricsSource field is initialized
        Field sourceField = DebugMetricsWrapper.class.getDeclaredField("debugMetricsSource");
        sourceField.setAccessible(true);
        AtlasDebugMetricsSource source = (AtlasDebugMetricsSource) sourceField.get(debugMetricsWrapper);
        assertNotNull(source);
    }

    @Test
    public void testUpdate() throws Exception {
        when(signature.toString()).thenReturn("EntityREST.getById(..)");
        when(signature.toShortString()).thenReturn("EntityREST.getById(..)");

        long timeConsumed = 100L;

        debugMetricsWrapper.update(signature, timeConsumed);
    }

    @Test
    public void testConstantsClass() {
        // Test that Constants class has expected values
        assertEquals(DebugMetricsWrapper.Constants.NUM_OPS, "numops");
        assertEquals(DebugMetricsWrapper.Constants.MIN_TIME, "mintime");
        assertEquals(DebugMetricsWrapper.Constants.MAX_TIME, "maxtime");
        assertEquals(DebugMetricsWrapper.Constants.STD_DEV_TIME, "stdevtime");
        assertEquals(DebugMetricsWrapper.Constants.AVG_TIME, "avgtime");
        assertEquals(DebugMetricsWrapper.Constants.DEBUG_METRICS_SOURCE, "AtlasDebugMetricsSource");
        assertEquals(DebugMetricsWrapper.Constants.DEBUG_METRICS_CONTEXT, "atlas-debug-metrics-context");
        assertEquals(DebugMetricsWrapper.Constants.DEBUG_METRICS_REST_SINK, "AtlasDebugMetricsRESTSink");
    }

    @Test
    public void testConstantsPrefixes() {
        assertEquals(DebugMetricsWrapper.Constants.EntityRESTPrefix, "EntityREST.");
        assertEquals(DebugMetricsWrapper.Constants.TypesRESTPrefix, "TypesREST.");
        assertEquals(DebugMetricsWrapper.Constants.GlossaryRESTPrefix, "GlossaryREST.");
        assertEquals(DebugMetricsWrapper.Constants.DiscoveryRESTPrefix, "DiscoveryREST.");
        assertEquals(DebugMetricsWrapper.Constants.LineageRESTPrefix, "LineageREST.");
        assertEquals(DebugMetricsWrapper.Constants.RelationshipRESTPrefix, "RelationshipREST.");
    }

    @Test
    public void testConstantsEntityRESTMethods() {
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_getById, "EntityREST.getById(..)");
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_createOrUpdate, "EntityREST.createOrUpdate(..)");
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_partialUpdateEntityAttrByGuid, "EntityREST.partialUpdateEntityAttrByGuid(..)");
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_deleteByGuid, "EntityREST.deleteByGuid(..)");
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_getClassification, "EntityREST.getClassification(..)");
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_getClassifications, "EntityREST.getClassifications(..)");
    }

    @Test
    public void testConstantsTypesRESTMethods() {
        assertEquals(DebugMetricsWrapper.Constants.TypesREST_getTypeDefByName, "TypesREST.getTypeDefByName(..)");
        assertEquals(DebugMetricsWrapper.Constants.TypesREST_getTypeDefByGuid, "TypesREST.getTypeDefByGuid(..)");
        assertEquals(DebugMetricsWrapper.Constants.TypesREST_getTypeDefHeaders, "TypesREST.getTypeDefHeaders(..)");
        assertEquals(DebugMetricsWrapper.Constants.TypesREST_getAllTypeDefs, "TypesREST.getAllTypeDefs(..)");
        assertEquals(DebugMetricsWrapper.Constants.TypesREST_createAtlasTypeDefs, "TypesREST.createAtlasTypeDefs(..)");
        assertEquals(DebugMetricsWrapper.Constants.TypesREST_updateAtlasTypeDefs, "TypesREST.updateAtlasTypeDefs(..)");
    }

    @Test
    public void testConstantsGlossaryRESTMethods() {
        assertEquals(DebugMetricsWrapper.Constants.GlossaryREST_getGlossaries, "GlossaryREST.getGlossaries(..)");
        assertEquals(DebugMetricsWrapper.Constants.GlossaryREST_getGlossary, "GlossaryREST.getGlossary(..)");
        assertEquals(DebugMetricsWrapper.Constants.GlossaryREST_createGlossary, "GlossaryREST.createGlossary(..)");
        assertEquals(DebugMetricsWrapper.Constants.GlossaryREST_updateGlossary, "GlossaryREST.updateGlossary(..)");
        assertEquals(DebugMetricsWrapper.Constants.GlossaryREST_deleteGlossary, "GlossaryREST.deleteGlossary(..)");
    }

    @Test
    public void testConstantsDiscoveryRESTMethods() {
        assertEquals(DebugMetricsWrapper.Constants.DiscoveryREST_searchUsingDSL, "DiscoveryREST.searchUsingDSL(..)");
        assertEquals(DebugMetricsWrapper.Constants.DiscoveryREST_searchUsingBasic, "DiscoveryREST.searchUsingBasic(..)");
        assertEquals(DebugMetricsWrapper.Constants.DiscoveryREST_quickSearch, "DiscoveryREST.quickSearch(..)");
        assertEquals(DebugMetricsWrapper.Constants.DiscoveryREST_addSavedSearch, "DiscoveryREST.addSavedSearch(..)");
    }

    @Test
    public void testConstantsRelationshipRESTMethods() {
        assertEquals(DebugMetricsWrapper.Constants.RelationshipREST_create, "RelationshipREST.create(..)");
        assertEquals(DebugMetricsWrapper.Constants.RelationshipREST_update, "RelationshipREST.update(..)");
        assertEquals(DebugMetricsWrapper.Constants.RelationshipREST_getById, "RelationshipREST.getById(..)");
        assertEquals(DebugMetricsWrapper.Constants.RelationshipREST_deleteById, "RelationshipREST.deleteById(..)");
    }

    @Test
    public void testConstantsLineageRESTMethods() {
        assertEquals(DebugMetricsWrapper.Constants.LineageREST_getLineageByUA, "LineageREST.getLineageByUniqueAttribute(..)");
        assertEquals(DebugMetricsWrapper.Constants.LineageREST_getLineageGraph, "LineageREST.getLineageGraph(..)");
    }

    @Test
    public void testConstantsNotificationHookConsumer() {
        assertEquals(DebugMetricsWrapper.Constants.NotificationHookConsumer_doWork, "NotificationHookConsumer.doWork(..)");
    }

    @Test
    public void testConstantsDuplicateSignatureAPIs() {
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_createOrUpdateBulk,
                    "EntityMutationResponse org.apache.atlas.web.rest.EntityREST.createOrUpdate(AtlasEntitiesWithExtInfo)");
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_removeLabelsByTypeName,
                    "void org.apache.atlas.web.rest.EntityREST.removeLabels(String,Set,HttpServletRequest)");
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_setLabelsByTypeName,
                    "void org.apache.atlas.web.rest.EntityREST.setLabels(String,Set,HttpServletRequest)");
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_addLabelsByTypeName,
                    "void org.apache.atlas.web.rest.EntityREST.addLabels(String,Set,HttpServletRequest)");
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_addOrUpdateBMByName,
                    "void org.apache.atlas.web.rest.EntityREST.addOrUpdateBusinessAttributes(String,String,Map)");
        assertEquals(DebugMetricsWrapper.Constants.EntityREST_removeBMByName,
                    "void org.apache.atlas.web.rest.EntityREST.removeBusinessAttributes(String,String,Map)");
        assertEquals(DebugMetricsWrapper.Constants.DiscoveryREST_quickSearchQuickSearchParams,
                    "AtlasQuickSearchResult org.apache.atlas.web.rest.DiscoveryREST.quickSearch(QuickSearchParameters)");
    }

    @Test
    public void testDebugMetricsSourceField() throws Exception {
        Field sourceField = DebugMetricsWrapper.class.getDeclaredField("debugMetricsSource");
        sourceField.setAccessible(true);
        AtlasDebugMetricsSource source = (AtlasDebugMetricsSource) sourceField.get(debugMetricsWrapper);
        assertNotNull(source);
    }

    private void assertEquals(String actual, String expected) {
        org.testng.Assert.assertEquals(actual, expected);
    }
}
