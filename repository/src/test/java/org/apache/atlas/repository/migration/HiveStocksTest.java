/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.migration;

import com.google.inject.Inject;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;

@Guice(modules = TestModules.TestOnlyModule.class)
public class HiveStocksTest extends MigrationBaseAsserts {

    @Inject
    public HiveStocksTest(AtlasGraph graph) {
        super(graph);
    }

    @Test
    public void migrateStocks() throws AtlasBaseException, IOException {
        runFileImporter("stocks_db");

        assertHiveVertices(1, 1, 7);
        assertTypeCountNameGuid("hive_db", 1, "stocks", "4e13b36b-9c54-4616-9001-1058221165d0");
        assertTypeCountNameGuid("hive_table", 1, "stocks_daily", "5cfc2540-9947-40e0-8905-367e07481774");
        assertTypeAttribute("hive_table", 7, "stocks_daily", "5cfc2540-9947-40e0-8905-367e07481774", "hive_table.columns");
        assertTypeCountNameGuid("hive_column", 1, "high", "d72ce4fb-6f17-4e68-aa85-967366c9e891");
        assertTypeCountNameGuid("hive_column", 1, "open", "788ba8fe-b7d8-41ba-84ef-c929732924ec");
        assertTypeCountNameGuid("hive_column", 1, "dt", "643a0a71-0d97-477d-a43b-7ca433f85160");
        assertTypeCountNameGuid("hive_column", 1, "low", "38caeaf7-49e6-4d6d-8727-231406a46821");
        assertTypeCountNameGuid("hive_column", 1, "close", "3bae9b76-f812-4745-b4d2-2a72d2773d07");
        assertTypeCountNameGuid("hive_column", 1, "volume", "bee376a4-3d8d-4943-b7e8-9bce042c2657");
        assertTypeCountNameGuid("hive_column", 1, "adj_close", "fcba2002-cb38-4c2e-b853-68d421d66703");
        assertTypeCountNameGuid("hive_process", 0, "", "");
        assertTypeCountNameGuid("hive_storagedesc", 1, "", "294290d8-4498-4677-973c-c266d594b039");
        assertTypeCountNameGuid("Tag1", 1, "", "");

        assertEdges(getVertex("hive_db", "stocks").getEdges(AtlasEdgeDirection.IN).iterator(),1, 1, "");
        assertEdges(getVertex("hive_table", "stocks_daily").getEdges(AtlasEdgeDirection.OUT).iterator(), 1, 1, "hive_db_tables");
        assertEdges(getVertex("hive_column", "high").getEdges(AtlasEdgeDirection.OUT).iterator(), 1,1, "hive_table_columns");

        assertMigrationStatus(187);
    }
}
