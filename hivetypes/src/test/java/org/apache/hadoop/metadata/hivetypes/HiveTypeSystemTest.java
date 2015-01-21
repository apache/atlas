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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.storage.Id;
import org.apache.hadoop.metadata.storage.memory.MemRepository;
import org.apache.hadoop.metadata.types.TypeSystem;
import org.junit.Before;
import org.junit.Test;


public class HiveTypeSystemTest {

    protected MemRepository mr;
    protected HiveTypeSystem hts;
    public static final Log LOG = LogFactory.getLog(HiveTypeSystemTest.class);

    @Before
    public void setup() throws MetadataException {

        TypeSystem ts = TypeSystem.getInstance();
        ts.reset();
        mr = new MemRepository(ts);
        hts = HiveTypeSystem.getInstance();
    }

    @Test
    public void testHiveImport() throws MetaException, MetadataException {

        HiveImporter himport = new HiveImporter(mr, hts, new HiveMetaStoreClient(new HiveConf()));
        himport.importHiveMetadata();
        LOG.info("Defined instances");
        for (Id id : himport.getInstances()) {
            ITypedReferenceableInstance instance = mr.get(id);
            LOG.info(instance.toString());
        }
    }

}