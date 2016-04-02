/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.audit;

import org.apache.atlas.ApplicationProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class HBaseTestUtils {
    private static HBaseTestingUtility hbaseTestUtility;
    private static LocalHBaseCluster hbaseCluster;

    public static void startCluster() throws Exception {
        Configuration hbaseConf =
                HBaseBasedAuditRepository.getHBaseConfiguration(ApplicationProperties.get());
        hbaseTestUtility = new HBaseTestingUtility(hbaseConf);
        int zkPort = hbaseConf.getInt("hbase.zookeeper.property.clientPort", 19026);
        hbaseTestUtility.startMiniZKCluster(1, zkPort);

        hbaseCluster = new LocalHBaseCluster(hbaseTestUtility.getConfiguration());
        hbaseCluster.startup();
    }

    public static void stopCluster() throws Exception {
        hbaseTestUtility.getConnection().close();
        hbaseCluster.shutdown();
        hbaseTestUtility.shutdownMiniZKCluster();
    }

    public static Connection getConnection() throws IOException {
        return hbaseTestUtility.getConnection();
    }
}
