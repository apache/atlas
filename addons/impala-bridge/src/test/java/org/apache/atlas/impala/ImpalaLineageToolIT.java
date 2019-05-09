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
package org.apache.atlas.impala;

import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.impala.hook.AtlasImpalaHookContext;
import org.apache.atlas.impala.hook.ImpalaLineageHook;
import org.apache.atlas.impala.model.ImpalaQuery;
import org.testng.annotations.Test;

public class ImpalaLineageToolIT extends ImpalaLineageITBase {
    private static String dir = System.getProperty("user.dir") + "/src/test/resources/";
    private static String IMPALA = dir + "impala3.json";
    private static String IMPALA_WAL = dir + "WALimpala.wal";

    /**
     * This tests
     * 1) ImpalaLineageTool can parse one lineage file that contains "create view" command lineage
     * 2) Lineage is sent to Atlas
     * 3) Atlas can get this lineage from Atlas
     */
    @Test
    public void testCreateViewFromFile() {
        List<ImpalaQuery> lineageList = new ArrayList<>();
        ImpalaLineageHook impalaLineageHook = new ImpalaLineageHook();

        try {
            // create database and tables to simulate Impala behavior that Impala updates metadata
            // to HMS and HMSHook sends the metadata to Atlas, which has to happen before
            // Atlas can handle lineage notification
            String dbName = "db_1";
            createDatabase(dbName);

            String sourceTableName = "table_1";
            createTable(dbName, sourceTableName,"(id string, count int)", false);

            String targetTableName = "view_1";
            createTable(dbName, targetTableName,"(count int, id string)", false);

            // process lineage record, and send corresponding notification to Atlas
            String[] args = new String[]{"-d", "./", "-p", "impala"};
            ImpalaLineageTool toolInstance = new ImpalaLineageTool(args);
            toolInstance.importHImpalaEntities(impalaLineageHook, IMPALA, IMPALA_WAL);

            // verify the process is saved in Atlas
            // the value is from info in IMPALA_3
            String createTime = new Long((long)(1554750072)*1000).toString();
            String processQFName =
                "db_1.view_1" + AtlasImpalaHookContext.QNAME_SEP_CLUSTER_NAME +
                    CLUSTER_NAME + AtlasImpalaHookContext.QNAME_SEP_PROCESS + createTime;

            processQFName = processQFName.toLowerCase();

            assertProcessIsRegistered(processQFName,
                "create view db_1.view_1 as select count, id from db_1.table_1");

        } catch (Exception e) {
            System.out.print("Appending file error");
        }
    }
}
