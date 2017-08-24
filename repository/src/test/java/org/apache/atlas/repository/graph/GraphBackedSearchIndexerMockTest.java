/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graph;

import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class GraphBackedSearchIndexerMockTest implements IAtlasGraphProvider {

    @Mock
    private Configuration configuration;

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasGraphManagement management;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSearchIndicesAreInitializedOnConstructionWhenHAIsDisabled() throws IndexException, RepositoryException {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);        
        when(graph.getManagementSystem()).thenReturn(management);

        GraphBackedSearchIndexer graphBackedSearchIndexer = new GraphBackedSearchIndexer(this, configuration, typeRegistry);
    }

    @Test
    public void testSearchIndicesAreNotInitializedOnConstructionWhenHAIsEnabled() throws IndexException, RepositoryException {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(graph.getManagementSystem()).thenReturn(management);
        when(management.containsPropertyKey(Constants.VERTEX_TYPE_PROPERTY_KEY)).thenReturn(true);

        new GraphBackedSearchIndexer(this, configuration, typeRegistry);
        verifyZeroInteractions(management);
    }

    @Test
    public void testIndicesAreReinitializedWhenServerBecomesActive() throws AtlasException {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(graph.getManagementSystem()).thenReturn(management);

        GraphBackedSearchIndexer graphBackedSearchIndexer = new GraphBackedSearchIndexer(this, configuration, typeRegistry);
        graphBackedSearchIndexer.instanceIsActive();
    }
    

    @Override
      public AtlasGraph get() {
          return graph;
      }
}
