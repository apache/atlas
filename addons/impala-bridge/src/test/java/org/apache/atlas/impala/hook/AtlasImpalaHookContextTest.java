package org.apache.atlas.impala.hook;

/** Licensed to the Apache Software Foundation (ASF) under one
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
 **/

import org.apache.atlas.impala.model.ImpalaOperationType;
import org.apache.atlas.impala.model.ImpalaQuery;
import org.apache.atlas.impala.model.LineageVertex;
import org.apache.atlas.impala.model.LineageVertexMetadata;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

public class AtlasImpalaHookContextTest {


    @Mock
    private ImpalaLineageHook impalaLineageHook;

    @Mock
    private ImpalaOperationType impalaOperationType;

    @Mock
    private ImpalaQuery impalaQuery;


    @BeforeMethod
    public void initializeMocks() {
        MockitoAnnotations.initMocks(this);
    }


    @Test
    public void testGetQualifiedNameForTableWithFullTableName() throws Exception {

        String database = "testDatabase";
        String table = "testTable";
        String metadataNamespace = "testNamespace";
        String expectedTableQualifiedName =  (database + AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME + table + AtlasImpalaHookContext.QNAME_SEP_METADATA_NAMESPACE).toLowerCase() + metadataNamespace;
        String fullTableName = database+AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME+table;

        when(impalaLineageHook.getMetadataNamespace()).thenReturn(metadataNamespace);

        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);

        String receivedTableQualifiedName = impalaHook.getQualifiedNameForTable(fullTableName);
        assertEquals(expectedTableQualifiedName,receivedTableQualifiedName);
    }

    @Test
    public void testGetQualifiedNameForTableWithNullTableName() throws Exception {

        String fullTableName = null;

        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);
        assertThrows(IllegalArgumentException.class, () -> impalaHook.getQualifiedNameForTable(fullTableName));
    }

    @Test
    public void testGetQualifiedNameForTableWithPartialTableName() throws Exception {

        String tableName = "testTableName";

        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);
        assertThrows(IllegalArgumentException.class, () -> impalaHook.getQualifiedNameForTable(tableName));
    }

    @Test
    public void testGetQualifiedNameForColumnUsingLineageVertexAndLineageVertexMetadata() throws Exception {

        String database = "testDatabase";
        String table = "testTable";
        String column = "testColumn";
        String metadataNamespace = "testNamespace";
        String fullTableName = database+AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME+table;
        String fullColumnName = database+AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME+table+AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME+column;
        LineageVertex vertex = new LineageVertex();
        LineageVertexMetadata metadata = new LineageVertexMetadata();

        String expectedColumnQualifiedName =  (database + AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME  + table + AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME +
                column + AtlasImpalaHookContext.QNAME_SEP_METADATA_NAMESPACE).toLowerCase() + metadataNamespace;

        metadata.setTableName(fullTableName);
        vertex.setMetadata(metadata);
        vertex.setVertexId(fullColumnName);

        when(impalaLineageHook.getMetadataNamespace())
                .thenReturn(metadataNamespace);

        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);

        String receivedColumnQualifiedName = impalaHook.getQualifiedNameForColumn(vertex);
        assertEquals(expectedColumnQualifiedName,receivedColumnQualifiedName);
    }



    @Test
    public void testGetQualifiedNameForColumnUsingLineageVertexAndLineageVertexMetadataAsNull() throws Exception {

        String database = "testDatabase";
        String table = "testTable";
        String column = "testColumn";
        String metadataNamespace = "testNamespace";
        String fullColumnName = database+AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME+table+AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME+column;
        LineageVertex vertex = new LineageVertex();

        String expectedColumnQualifiedName =  (database + AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME  + table + AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME +
                column + AtlasImpalaHookContext.QNAME_SEP_METADATA_NAMESPACE).toLowerCase() + metadataNamespace;

        vertex.setMetadata(null);
        vertex.setVertexId(fullColumnName);

        when(impalaLineageHook.getMetadataNamespace())
                .thenReturn(metadataNamespace);

        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);

        String receivedColumnQualifiedName = impalaHook.getQualifiedNameForColumn(vertex);
        assertEquals(expectedColumnQualifiedName,receivedColumnQualifiedName);
    }

    @Test
    public void testGetQualifiedNameForColumnUsingLineageVertexAndLineageVertexMetadataTableAsNull() throws Exception {

        String database = "testDatabase";
        String table = "testTable";
        String column = "testColumn";
        LineageVertex vertex = new LineageVertex();
        LineageVertexMetadata metadata = new LineageVertexMetadata();

        vertex.setMetadata(metadata);

        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);

        assertThrows(IllegalArgumentException.class, () -> impalaHook.getQualifiedNameForColumn(vertex));
    }

    @Test
    public void testGetQualifiedNameForColumn() throws Exception {

        String database = "testDatabase";
        String table = "testTable";
        String column = "testColumn";
        String metadataNamespace = "testNamespace";
        String expectedColumnQualifiedName =     (database + AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME  + table + AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME +
                column + AtlasImpalaHookContext.QNAME_SEP_METADATA_NAMESPACE).toLowerCase() + metadataNamespace;
        String fullColumnName = database+AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME+table+AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME+column;

        when(impalaLineageHook.getMetadataNamespace())
                .thenReturn(metadataNamespace);


        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);

        String receivedColumnQualifiedName = impalaHook.getQualifiedNameForColumn(fullColumnName);
        assertEquals(expectedColumnQualifiedName,receivedColumnQualifiedName);
    }


    @Test
    public void testGetTableNameFromColumn() throws Exception {

        String table = "testTable";
        String column = "testColumn";
        String expectedTableName =  table;
        String fullTableName = table+AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME+column;


        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);

        String receivedTableName = impalaHook.getTableNameFromColumn(fullTableName);
        assertEquals(expectedTableName,receivedTableName);
    }

    @Test
    public void testGetDatabaseNameFromTable() throws Exception {

        String table = "testTable";
        String database = "testDatabase";
        String expectedDatabaseName =  database;
        String fullTableName = database+AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME+table;


        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);

        String receivedDatabaseName = impalaHook.getDatabaseNameFromTable(fullTableName);
        assertEquals(expectedDatabaseName,receivedDatabaseName);
    }


    @Test
    public void testGetColumnNameOnlyWithFullCOlumnName() throws Exception {

        String table = "testTable";
        String column = "testColumn";
        String expectedColumnName =  column;
        String fullColumnName = table+AtlasImpalaHookContext.QNAME_SEP_ENTITY_NAME+column;

        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);

        String receivedColumnName = impalaHook.getColumnNameOnly(fullColumnName);
        assertEquals(expectedColumnName,receivedColumnName);
    }


    @Test
    public void testGetColumnNameOnlyWithNullValue() throws Exception {
        String fullColumnName = null;
        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);

        assertThrows(IllegalArgumentException.class, () -> impalaHook.getColumnNameOnly(fullColumnName));
    }

    @Test
    public void testGetColumnNameOnlyWithPartialColumnName() throws Exception {

        String table = "testTable";
        String column = "testColumn";
        String expectedColumnName =  column;
        String columnName = column;

        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);

        String receivedColumnName = impalaHook.getColumnNameOnly(columnName);
        assertEquals(expectedColumnName,receivedColumnName);
    }

    @Test
    public void testGetQualifiedNameForDb() throws Exception {

        String database = "testDatabase";
        String metadataNamespace = "testNamespace";
        String expectedDatabaseName =  (database + AtlasImpalaHookContext.QNAME_SEP_METADATA_NAMESPACE).toLowerCase() + metadataNamespace;
        AtlasImpalaHookContext impalaHook = new AtlasImpalaHookContext(impalaLineageHook, impalaOperationType, impalaQuery);

        when(impalaLineageHook.getMetadataNamespace()).thenReturn(metadataNamespace);

        String receivedDatabaseName = impalaHook.getQualifiedNameForDb(database);
        assertEquals(expectedDatabaseName,receivedDatabaseName);
    }


}
