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

import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class HBaseBasedAuditRepositoryHATest {

    @Mock
    private Configuration configuration;

    @Mock
    private org.apache.hadoop.conf.Configuration hbaseConf;

    @Mock
    private Connection connection;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testTableShouldNotBeCreatedOnStartIfHAIsEnabled() throws IOException, AtlasException {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getString(HBaseBasedAuditRepository.CONFIG_TABLE_NAME,
                HBaseBasedAuditRepository.DEFAULT_TABLE_NAME)).
                thenReturn(HBaseBasedAuditRepository.DEFAULT_TABLE_NAME);
        HBaseBasedAuditRepository auditRepository = new HBaseBasedAuditRepository() {
            @Override
            protected Connection createConnection(org.apache.hadoop.conf.Configuration hbaseConf) {
                return connection;
            }
        };
        auditRepository.startInternal(configuration, hbaseConf);

        verifyZeroInteractions(connection);
    }

    @Test
    public void testShouldCreateTableWhenReactingToActive() throws AtlasException, IOException {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getString(HBaseBasedAuditRepository.CONFIG_TABLE_NAME,
                HBaseBasedAuditRepository.DEFAULT_TABLE_NAME)).
                thenReturn(HBaseBasedAuditRepository.DEFAULT_TABLE_NAME);
        TableName tableName = TableName.valueOf(HBaseBasedAuditRepository.DEFAULT_TABLE_NAME);
        Admin admin = mock(Admin.class);
        when(connection.getAdmin()).thenReturn(admin);
        when(admin.tableExists(tableName)).thenReturn(true);
        HBaseBasedAuditRepository auditRepository = new HBaseBasedAuditRepository() {
            @Override
            protected Connection createConnection(org.apache.hadoop.conf.Configuration hbaseConf) {
                return connection;
            }
        };
        auditRepository.startInternal(configuration, hbaseConf);
        auditRepository.instanceIsActive();

        verify(connection).getAdmin();
        verify(admin).tableExists(tableName);
    }
}
