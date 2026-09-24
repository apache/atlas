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
package org.apache.atlas.repository.audit;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class HBaseBasedAuditRepositoryTest {
    @Test
    public void getHBaseConfiguration_copiesAtlasAuditProperties() {
        Map<String, Object> entries = new HashMap<>();
        entries.put("atlas.audit.hbase.zookeeper.quorum", "zk1.example.com");
        entries.put("atlas.audit.hbase.client.port", "2181");
        entries.put("atlas.graph.storage.backend", "hbase2");

        org.apache.hadoop.conf.Configuration hbaseConf =
                HBaseBasedAuditRepository.getHBaseConfiguration(new MapConfiguration(entries));

        assertEquals(hbaseConf.get("hbase.zookeeper.quorum"), "zk1.example.com");
        assertEquals(hbaseConf.get("hbase.client.port"), "2181");
        assertEquals(hbaseConf.get("graph.storage.backend"), null);
    }

    @Test
    public void startInternal_initializesTableCompressionAndConnection() throws Exception {
        Map<String, Object> entries = new HashMap<>();
        entries.put(HBaseBasedAuditRepository.CONFIG_TABLE_NAME, "UNIT_TEST_AUDIT_TABLE");
        entries.put(HBaseBasedAuditRepository.CONFIG_COMPRESSION_ALGORITHM, "SNAPPY");

        MapConfiguration atlasConf = new MapConfiguration(entries);
        org.apache.hadoop.conf.Configuration hbaseConf = new org.apache.hadoop.conf.Configuration(false);

        Connection connection = mock(Connection.class);
        TestableHBaseRepository repository = new TestableHBaseRepository(connection);

        repository.startInternal(atlasConf, hbaseConf);

        assertTrue(repository.createConnectionCalled);
        assertSame(repository.receivedHBaseConf, hbaseConf);
        assertEquals(((TableName) readField(repository, "tableName")).getNameAsString(), "UNIT_TEST_AUDIT_TABLE");
        assertEquals(readField(repository, "compressionType"), "SNAPPY");
        assertSame(readField(repository, "connection"), connection);
    }

    private static Object readField(Object target, String fieldName) throws Exception {
        Field field = HBaseBasedAuditRepository.class.getDeclaredField(fieldName);
        field.setAccessible(true);

        return field.get(target);
    }

    private static final class TestableHBaseRepository extends HBaseBasedAuditRepository {
        private final Connection connection;

        private boolean                         createConnectionCalled;
        private org.apache.hadoop.conf.Configuration receivedHBaseConf;

        private TestableHBaseRepository(Connection connection) {
            this.connection = connection;
        }

        @Override
        protected Connection createConnection(org.apache.hadoop.conf.Configuration hbaseConf) {
            this.createConnectionCalled = true;
            this.receivedHBaseConf = hbaseConf;

            return connection;
        }
    }
}
