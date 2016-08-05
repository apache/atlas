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
import org.apache.atlas.EntityAuditEvent;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class HBaseBasedAuditRepositoryTest extends AuditRepositoryTestBase {
    private TableName tableName;

    @BeforeClass
    public void setup() throws Exception {
        eventRepository = new HBaseBasedAuditRepository();
        HBaseTestUtils.startCluster();
        ((HBaseBasedAuditRepository) eventRepository).start();

        Configuration properties = ApplicationProperties.get();
        String tableNameStr = properties.getString(HBaseBasedAuditRepository.CONFIG_TABLE_NAME,
                HBaseBasedAuditRepository.DEFAULT_TABLE_NAME);
        tableName = TableName.valueOf(tableNameStr);
    }

    @AfterClass
    public void teardown() throws Exception {
        ((HBaseBasedAuditRepository) eventRepository).stop();
        HBaseTestUtils.stopCluster();
    }

    @Test
    public void testTableCreated() throws Exception {
        Connection connection = HBaseTestUtils.getConnection();
        Admin admin = connection.getAdmin();
        assertTrue(admin.tableExists(tableName));
    }

    @Override
    protected void assertEventEquals(EntityAuditEvent actual, EntityAuditEvent expected) {
        super.assertEventEquals(actual, expected);
        assertNull(actual.getEntityDefinition());
    }
}