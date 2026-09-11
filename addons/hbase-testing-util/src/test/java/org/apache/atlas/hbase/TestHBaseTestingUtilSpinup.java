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
package org.apache.atlas.hbase;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

import static org.testng.AssertJUnit.assertFalse;

/**
 * Make sure we can spin up a HBTU without a hbase-site.xml
 */
public class TestHBaseTestingUtilSpinup {
    private final HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility();

    public TestHBaseTestingUtilSpinup() throws Exception {
        String runId = UUID.randomUUID().toString();
        File   baseDir = Files.createTempDirectory("atlas-hbase-test-" + runId).toFile();

        // Keep each test run isolated from stale local state and avoid fixed-port races.
        hBaseTestingUtility.getConfiguration().set("hadoop.tmp.dir", new File(baseDir, "hadoop-tmp").getAbsolutePath());
        hBaseTestingUtility.getConfiguration().set("hbase.rootdir", new File(baseDir, "hbase-root").toURI().toString());
        hBaseTestingUtility.getConfiguration().set("hbase.zookeeper.property.dataDir", new File(baseDir, "zk-data").getAbsolutePath());
        hBaseTestingUtility.getConfiguration().set("zookeeper.znode.parent", "/hbase-unsecure-" + runId);
        hBaseTestingUtility.getConfiguration().set("test.hbase.zookeeper.property.clientPort", "0");
        hBaseTestingUtility.getConfiguration().set("hbase.master.port", "0");
        hBaseTestingUtility.getConfiguration().set("hbase.master.info.port", "0");
        hBaseTestingUtility.getConfiguration().set("hbase.regionserver.port", "0");
        hBaseTestingUtility.getConfiguration().set("hbase.regionserver.info.port", "0");
        hBaseTestingUtility.getConfiguration().set("hbase.master.hostname", "localhost");
        hBaseTestingUtility.getConfiguration().set("hbase.regionserver.hostname", "localhost");
        hBaseTestingUtility.getConfiguration().set("hbase.regionserver.hostname.seen.by.master", "localhost");
        hBaseTestingUtility.getConfiguration().set("hbase.table.sanity.checks", "false");
    }

    @Test
    public void testGetMetaTableRows() throws Exception {
        try (MiniHBaseCluster miniCluster = hBaseTestingUtility.startMiniCluster()) {
            if (!hBaseTestingUtility.getHBaseCluster().waitForActiveAndReadyMaster(30000)) {
                throw new RuntimeException("Active master not ready");
            }

            List<byte[]> results = hBaseTestingUtility.getMetaTableRows();

            assertFalse("results should have some entries and is empty.", results.isEmpty());
        } finally {
            hBaseTestingUtility.shutdownMiniCluster();
        }
    }
}
