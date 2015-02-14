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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.storage.Id;
import org.apache.hadoop.metadata.storage.memory.MemRepository;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@Test (enabled = false)
public class HiveTypeSystemTest {

    protected MemRepository mr;
    protected HiveTypeSystem hts;

    private static final Logger LOG =
            LoggerFactory.getLogger(HiveTypeSystemTest.class);

    @BeforeClass
    public void setup() throws MetadataException {

        TypeSystem ts = TypeSystem.getInstance();
        ts.reset();
        mr = new MemRepository(ts);
        hts = HiveTypeSystem.getInstance();
    }

    @Test (enabled = false)
    public void testHiveImport() throws MetaException, MetadataException, IOException {

        HiveImporter hImporter = new HiveImporter(mr, hts, new HiveMetaStoreClient(new HiveConf()));
        hImporter.importHiveMetadata();
        LOG.info("Defined DB instances");
        File f = new File("./target/logs/hiveobjs.txt");
        f.getParentFile().mkdirs();
        FileWriter fw = new FileWriter(f);        BufferedWriter bw = new BufferedWriter(fw);
        for (Id id : hImporter.getDBInstances()) {
            ITypedReferenceableInstance instance = mr.get(id);
            bw.write(instance.toString());
        }
        LOG.info("Defined Table instances");
        for (Id id : hImporter.getTableInstances()) {
            ITypedReferenceableInstance instance = mr.get(id);
            bw.write(instance.toString());
        }
        LOG.info("Defined Partition instances");
        for (Id id : hImporter.getPartitionInstances()) {
            ITypedReferenceableInstance instance = mr.get(id);
            bw.write(instance.toString());
        }
        LOG.info("Defined Column instances");
        for (Id id : hImporter.getColumnInstances()) {
            ITypedReferenceableInstance instance = mr.get(id);
            bw.write(instance.toString());
        }
        LOG.info("Defined Index instances");
        for (Id id : hImporter.getIndexInstances()) {
            ITypedReferenceableInstance instance = mr.get(id);
            bw.write(instance.toString());
        }
        LOG.info("Defined Process instances");
        for (Id id : hImporter.getProcessInstances()) {
            ITypedReferenceableInstance instance = mr.get(id);
            bw.write(instance.toString());
        }
        bw.flush();
        bw.close();
    }
}