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
 * See the License for the specific language regarding permissions and
 * limitations under the License.
 */
package org.janusgraph.diskstorage.rdbms.dao;

import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.testng.annotations.Test;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DaoManagerTest {

    @Test
    public void existingJanusSchemaDisablesEclipseLinkSchemaGeneration() throws Exception {
        DataSource dataSource = dataSourceWithTable("janus_column");
        Map<String, Object> config = new HashMap<>();

        config.put(PersistenceUnitProperties.SCHEMA_GENERATION_DATABASE_ACTION, "create");
        config.put(PersistenceUnitProperties.SCHEMA_GENERATION_CREATE_DATABASE_SCHEMAS, "true");
        config.put(PersistenceUnitProperties.SCHEMA_GENERATION_CREATE_SOURCE, "script");
        config.put(PersistenceUnitProperties.SCHEMA_GENERATION_CREATE_SCRIPT_SOURCE, "META-INF/postgres/create_schema.sql");

        DaoManager.suppressSchemaGenerationIfSchemaExists(dataSource, config);

        assertEquals(config.get(PersistenceUnitProperties.SCHEMA_GENERATION_DATABASE_ACTION), "none");
        assertFalse(config.containsKey(PersistenceUnitProperties.SCHEMA_GENERATION_CREATE_DATABASE_SCHEMAS));
        assertFalse(config.containsKey(PersistenceUnitProperties.SCHEMA_GENERATION_CREATE_SOURCE));
        assertFalse(config.containsKey(PersistenceUnitProperties.SCHEMA_GENERATION_CREATE_SCRIPT_SOURCE));
    }

    @Test
    public void missingJanusSchemaLeavesSchemaGenerationConfigured() throws Exception {
        DataSource dataSource = dataSourceWithTable(null);
        Map<String, Object> config = new HashMap<>();

        config.put(PersistenceUnitProperties.SCHEMA_GENERATION_DATABASE_ACTION, "create");

        DaoManager.suppressSchemaGenerationIfSchemaExists(dataSource, config);

        assertEquals(config.get(PersistenceUnitProperties.SCHEMA_GENERATION_DATABASE_ACTION), "create");
    }

    @Test
    public void schemaCheckFailureLeavesSchemaGenerationConfigured() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Map<String, Object> config = new HashMap<>();

        when(dataSource.getConnection()).thenThrow(new SQLException("unavailable"));
        config.put(PersistenceUnitProperties.SCHEMA_GENERATION_DATABASE_ACTION, "create");

        DaoManager.suppressSchemaGenerationIfSchemaExists(dataSource, config);

        assertEquals(config.get(PersistenceUnitProperties.SCHEMA_GENERATION_DATABASE_ACTION), "create");
        assertFalse(DaoManager.janusSchemaExists(dataSource));
    }

    @Test
    public void janusSchemaExistsWhenTableIsPresent() throws Exception {
        assertTrue(DaoManager.janusSchemaExists(dataSourceWithTable("janus_column")));
        assertFalse(DaoManager.janusSchemaExists(dataSourceWithTable(null)));
    }

    private static DataSource dataSourceWithTable(String tableName) throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        ResultSet tables = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(any(), any(), eq("janus_column"), any())).thenReturn(tables);
        when(metaData.getTables(any(), any(), eq("JANUS_COLUMN"), any())).thenReturn(tables);
        when(tables.next()).thenReturn(tableName != null);

        return dataSource;
    }
}
