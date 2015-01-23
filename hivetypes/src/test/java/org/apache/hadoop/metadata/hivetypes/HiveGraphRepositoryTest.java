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


import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.repository.graph.GraphBackedMetadataRepository;
import org.apache.hadoop.metadata.repository.graph.GraphService;
import org.apache.hadoop.metadata.repository.graph.TitanGraphProvider;
import org.apache.hadoop.metadata.repository.graph.TitanGraphService;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class HiveGraphRepositoryTest {

    protected HiveTypeSystem hts;
    GraphBackedMetadataRepository repository;

    private static final Logger LOG =
            LoggerFactory.getLogger(HiveGraphRepositoryTest.class);

    @Before
    public void setup() throws ConfigurationException, MetadataException {

        TypeSystem ts = TypeSystem.getInstance();
        GraphService gs = new TitanGraphService(new TitanGraphProvider());
        repository = new GraphBackedMetadataRepository(gs);
        hts = HiveTypeSystem.getInstance();
    }

    @Test
    public void testHiveImport() throws MetaException, MetadataException, IOException {

        HiveImporter hImporter = new HiveImporter(repository, hts, new HiveMetaStoreClient(new HiveConf()));
        hImporter.importHiveMetadata();
        LOG.info("Defined DB instances");
        FileWriter fw = new FileWriter("hiveobjs.txt");
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
        bw.flush();
        bw.close();
    }
}
