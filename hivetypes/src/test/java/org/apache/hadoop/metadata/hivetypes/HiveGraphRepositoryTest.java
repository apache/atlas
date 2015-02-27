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

package org.apache.hadoop.metadata.hivetypes;


import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.repository.graph.GraphBackedMetadataRepository;
import org.apache.hadoop.metadata.repository.graph.GraphHelper;
import org.apache.hadoop.metadata.repository.graph.GraphService;
import org.apache.hadoop.metadata.repository.graph.TitanGraphProvider;
import org.apache.hadoop.metadata.repository.graph.TitanGraphService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

@Test (enabled = false)
public class HiveGraphRepositoryTest {

    protected HiveTypeSystem hts;
    private GraphBackedMetadataRepository repository;
    private GraphService gs;
    public static final String HIVE_L4J_PROPS = "target/hive-log4j.properties";
    public static final String HIVE_EXEC_L4J_PROPS = "target/hive-exec-log4j.properties";

    private static final Logger LOG =
            LoggerFactory.getLogger(HiveGraphRepositoryTest.class);

    @BeforeClass
    public void setup() throws ConfigurationException, MetadataException {

        gs = new TitanGraphService(new TitanGraphProvider());
        repository = new GraphBackedMetadataRepository(gs);
        hts = HiveTypeSystem.getInstance();
    }

    @AfterClass
    public void tearDown() {
        Graph graph = gs.getBlueprintsGraph();
        System.out.println("*******************Graph Dump****************************");
        System.out.println("Vertices of " + graph);
        for (Vertex vertex : graph.getVertices()) {
            System.out.println(GraphHelper.vertexString(vertex));
        }

        System.out.println("Edges of " + graph);
        for (Edge edge : graph.getEdges()) {
            System.out.println(GraphHelper.edgeString(edge));
        }
        System.out.println("*******************Graph Dump****************************");
    }

    @Test (enabled = false)
    public void testHiveImport() throws Exception {
        HiveConf conf = new HiveConf();
        HiveMetaStoreClient hiveMetaStoreClient;
        hiveMetaStoreClient = new HiveMetaStoreClient(conf);
        HiveImporter hImporter = new HiveImporter(repository, hts, hiveMetaStoreClient);
        hImporter.importHiveMetadata();
        LOG.info("Defined DB instances");
        File f = new File("./target/logs/hiveobjs.txt");
        f.getParentFile().mkdirs();
        FileWriter fw = new FileWriter(f);
        BufferedWriter bw = new BufferedWriter(fw);
        List<String> idList =
                repository.getEntityList(HiveTypeSystem.DefinedTypes.HIVE_DB.name());
        for (String id : idList) {
            ITypedReferenceableInstance instance = repository.getEntityDefinition(id);
            bw.write(instance.toString());
        }
        LOG.info("Defined Table instances");
        idList =
                repository.getEntityList(HiveTypeSystem.DefinedTypes.HIVE_TABLE.name());

        for (String id : idList) {
            ITypedReferenceableInstance instance = repository.getEntityDefinition(id);
            bw.write(instance.toString());
        }
        LOG.info("Defined Partition instances");
        idList =
                repository.getEntityList(HiveTypeSystem.DefinedTypes.HIVE_PARTITION.name());

        for (String id : idList) {
            ITypedReferenceableInstance instance = repository.getEntityDefinition(id);
            bw.write(instance.toString());
        }
        LOG.info("Defined Column instances");
        idList =
                repository.getEntityList(HiveTypeSystem.DefinedTypes.HIVE_COLUMN.name());

        for (String id : idList) {
            ITypedReferenceableInstance instance = repository.getEntityDefinition(id);
            bw.write(instance.toString());
        }
        LOG.info("Defined Index instances");
        idList =
                repository.getEntityList(HiveTypeSystem.DefinedTypes.HIVE_INDEX.name());

        for (String id : idList) {
            ITypedReferenceableInstance instance = repository.getEntityDefinition(id);
            bw.write(instance.toString());
        }
        LOG.info("Defined Process instances");
        idList =
                repository.getEntityList(HiveTypeSystem.DefinedTypes.HIVE_PROCESS.name());

        for (String id : idList) {
            ITypedReferenceableInstance instance = repository.getEntityDefinition(id);
            bw.write(instance.toString());
        }
        bw.flush();
        bw.close();
    }

}
