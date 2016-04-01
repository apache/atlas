/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.fs.model;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;
import scala.Enumeration;
import scala.collection.Iterator;

import javax.inject.Inject;

@Test
@Guice(modules = RepositoryMetadataModule.class)
public class HDFSModelTest {

    public static final Logger LOG = LoggerFactory.getLogger(HDFSModelTest.class);
    private static final String ATLAS_URL = "http://localhost:21000/";

    @Inject
    private MetadataService metadataService;

    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @BeforeClass
    public void setUp() throws Exception {
    }

    @AfterClass
    public void tearDown() throws Exception {
        TypeSystem.getInstance().reset();
        try {
            //TODO - Fix failure during shutdown while using BDB
            graphProvider.get().shutdown();
        } catch(Exception e) {
            e.printStackTrace();
        }
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateDataModel() throws Exception {
        FSDataModel.main(new String[]{});
        TypesDef fsTypesDef = FSDataModel.typesDef();

        String fsTypesAsJSON = TypesSerialization.toJson(fsTypesDef);
        LOG.info("fsTypesAsJSON = {}", fsTypesAsJSON);

        metadataService.createType(fsTypesAsJSON);

        // verify types are registered
        final Iterator<Enumeration.Value> valueIterator = FSDataTypes.values().iterator();
        while (valueIterator.hasNext() ) {
            final Enumeration.Value typeEnum = valueIterator.next();
            String typeDefStr = metadataService.getTypeDefinition(typeEnum.toString());
            Assert.assertNotNull(typeDefStr);

            TypesDef typesDef = TypesSerialization.fromJson(typeDefStr);
            Assert.assertNotNull(typesDef);
        }
    }

}