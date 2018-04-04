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

package org.apache.atlas.repository.migration;

import com.google.inject.Inject;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.atlas.graph.GraphSandboxUtil.useLocalSolr;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadModelFromJson;
import static org.testng.Assert.assertEquals;

@Guice(modules = TestModules.TestOnlyModule.class)
public class HiveParititionIT extends  MigrationBaseAsserts {
    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStoreInitializer storeInitializer;

    @Inject
    private GraphBackedSearchIndexer indexer;

    @Inject
    public HiveParititionIT(AtlasGraph graph) {
        super(graph);
    }

    @AfterClass
    public void clear() throws Exception {
        AtlasGraphProvider.cleanup();

        if (useLocalSolr()) {
            LocalSolrRunner.stop();
        }
    }

    @Test
    public void fileImporterTest() throws IOException, AtlasBaseException {
        loadModelFromJson("0000-Area0/0010-base_model.json", typeDefStore, typeRegistry);
        loadModelFromJson("1000-Hadoop/1030-hive_model.json", typeDefStore, typeRegistry);

        String directoryName = TestResourceFileUtils.getDirectory("parts_db");
        DataMigrationService.FileImporter fi = new DataMigrationService.FileImporter(typeDefStore, typeRegistry,
                storeInitializer, directoryName, indexer);

        fi.run();


        assertPartitionKeyProperty(getVertex("hive_table", "t1"), 1);
        assertPartitionKeyProperty(getVertex("hive_table", "tv1"), 1);
        assertHiveVertices(1, 2, 7);

        assertTypeCountNameGuid("hive_db", 1, "parts_db", "ae30d78b-51b4-42ab-9436-8d60c8f68b95");
        assertTypeCountNameGuid("hive_process", 1, "", "");
        assertEdges("hive_db", "parts_db", AtlasEdgeDirection.IN,1, 1, "");
        assertEdges("hive_table", "t1", AtlasEdgeDirection.OUT, 1, 1, "hive_db_tables");
        assertEdges("hive_table", "tv1", AtlasEdgeDirection.OUT, 1, 1, "hive_db_tables");

        assertMigrationStatus(136);
    }

    private void assertPartitionKeyProperty(AtlasVertex vertex, int expectedCount) {
        List<String> keys = GraphHelper.getListProperty(vertex, "hive_table.partitionKeys");
        assertEquals(keys.size(), expectedCount);
    }
}
