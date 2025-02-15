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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import static org.testng.AssertJUnit.assertFalse;

/**
 * Make sure we can spin up a HBTU without a hbase-site.xml
 */
public class TestHBaseTestingUtilSpinup {
    private final HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility();

    public TestHBaseTestingUtilSpinup() throws Exception {
        hBaseTestingUtility.getConfiguration().set("test.hbase.zookeeper.property.clientPort", String.valueOf(getFreePort()));
        hBaseTestingUtility.getConfiguration().set("hbase.master.port", String.valueOf(getFreePort()));
        hBaseTestingUtility.getConfiguration().set("hbase.master.info.port", String.valueOf(getFreePort()));
        hBaseTestingUtility.getConfiguration().set("hbase.regionserver.port", String.valueOf(getFreePort()));
        hBaseTestingUtility.getConfiguration().set("hbase.regionserver.info.port", String.valueOf(getFreePort()));
        hBaseTestingUtility.getConfiguration().set("zookeeper.znode.parent", "/hbase-unsecure");
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

    private static int getFreePort() throws IOException {
        ServerSocket serverSocket = new ServerSocket(0);
        int          port         = serverSocket.getLocalPort();

        serverSocket.close();

        return port;
    }
}
