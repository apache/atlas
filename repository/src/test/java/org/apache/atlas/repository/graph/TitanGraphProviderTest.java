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

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test
public class TitanGraphProviderTest {

   private Configuration configuration;
   private TitanGraph graph;

   @BeforeTest
   public void setUp() throws AtlasException {
       //First get Instance
       graph = TitanGraphProvider.getGraphInstance();
       configuration = ApplicationProperties.getSubsetConfiguration(ApplicationProperties.get(), TitanGraphProvider.GRAPH_PREFIX);
   }

    @AfterClass
    public void tearDown() throws Exception {
        try {
            graph.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            TitanCleanup.clear(graph);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

   @Test
   public void testValidate() throws AtlasException {
       try {
           TitanGraphProvider.validateIndexBackend(configuration);
       } catch(Exception e){
           Assert.fail("Unexpected exception ", e);
       }

       //Change backend
       configuration.setProperty(TitanGraphProvider.INDEX_BACKEND_CONF, TitanGraphProvider.INDEX_BACKEND_LUCENE);
       try {
           TitanGraphProvider.validateIndexBackend(configuration);
           Assert.fail("Expected exception");
       } catch(Exception e){
           Assert.assertEquals(e.getMessage(), "Configured Index Backend lucene differs from earlier configured Index Backend elasticsearch. Aborting!");
       }
   }
}
