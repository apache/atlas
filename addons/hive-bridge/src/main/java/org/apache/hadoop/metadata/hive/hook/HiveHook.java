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

package org.apache.hadoop.metadata.hive.hook;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.metadata.MetadataServiceClient;
import org.apache.hadoop.metadata.hive.bridge.HiveMetaStoreBridge;
import org.apache.hadoop.metadata.hive.model.HiveDataTypes;
import org.apache.hadoop.metadata.typesystem.Referenceable;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * DgiHook sends lineage information to the DgiSever.
 */
public class HiveHook implements ExecuteWithHookContext, HiveSemanticAnalyzerHook {

    private static final Log LOG = LogFactory.getLog(HiveHook.class.getName());
    // wait time determines how long we wait before we exit the jvm on
    // shutdown. Pending requests after that will not be sent.
    private static final int WAIT_TIME = 3;
    private static ExecutorService executor;

    private static final String MIN_THREADS = "hive.hook.dgi.minThreads";
    private static final String MAX_THREADS = "hive.hook.dgi.maxThreads";
    private static final String KEEP_ALIVE_TIME = "hive.hook.dgi.keepAliveTime";

    private static final int minThreadsDefault = 5;
    private static final int maxThreadsDefault = 5;
    private static final long keepAliveTimeDefault = 10;

    static {
        // anything shared should be initialized here and destroyed in the
        // shutdown hook The hook contract is weird in that it creates a
        // boatload of hooks.

        // initialize the async facility to process hook calls. We don't
        // want to do this inline since it adds plenty of overhead for the
        // query.
        HiveConf hiveConf = new HiveConf();
        int minThreads = hiveConf.getInt(MIN_THREADS, minThreadsDefault);
        int maxThreads = hiveConf.getInt(MAX_THREADS, maxThreadsDefault);
        long keepAliveTime = hiveConf.getLong(KEEP_ALIVE_TIME, keepAliveTimeDefault);

        executor = new ThreadPoolExecutor(minThreads, maxThreads,
                keepAliveTime, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("DGI Logger %d")
                        .build());

        try {
            Runtime.getRuntime().addShutdownHook(
                    new Thread() {
                        @Override
                        public void run() {
                            try {
                                executor.shutdown();
                                executor.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
                                executor = null;
                            } catch (InterruptedException ie) {
                                LOG.info("Interrupt received in shutdown.");
                            }
                            // shutdown client
                        }
                    }
            );
        } catch (IllegalStateException is) {
            LOG.info("Attempting to send msg while shutdown in progress.");
        }

        LOG.info("Created DGI Hook");
    }

    @Override
    public void run(final HookContext hookContext) throws Exception {
        if (executor == null) {
            LOG.info("No executor running. Bail.");
            return;
        }

        // clone to avoid concurrent access
        final HiveConf conf = new HiveConf(hookContext.getConf());
        boolean debug = conf.get("debug", "false").equals("true");

        if (debug) {
            fireAndForget(hookContext, conf);
        } else {
            executor.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                fireAndForget(hookContext, conf);
                            } catch (Throwable e) {
                                LOG.info("DGI hook failed", e);
                            }
                        }
                    }
            );
        }
    }

    private void fireAndForget(HookContext hookContext, HiveConf conf) throws Exception {
        LOG.info("Entered DGI hook for query hook " + hookContext.getHookType());
        if (hookContext.getHookType() != HookContext.HookType.POST_EXEC_HOOK) {
            LOG.debug("No-op for query hook " + hookContext.getHookType());
        }

        HiveMetaStoreBridge dgiBridge = new HiveMetaStoreBridge(conf);

        HiveOperation operation = HiveOperation.valueOf(hookContext.getOperationName());

        switch (operation) {
            case CREATEDATABASE:
                Set<WriteEntity> outputs = hookContext.getOutputs();
                for (WriteEntity entity : outputs) {
                    if (entity.getType() == Entity.Type.DATABASE) {
                        dgiBridge.registerDatabase(entity.getDatabase().getName());
                    }
                }
                break;

            case CREATETABLE:
                outputs = hookContext.getOutputs();
                for (WriteEntity entity : outputs) {
                    if (entity.getType() == Entity.Type.TABLE) {

                        Table table = entity.getTable();
                        //TODO table.getDbName().toLowerCase() is required as hive stores in lowercase, but table.getDbName() is not lowercase
                        Referenceable dbReferenceable = getDatabaseReference(dgiBridge, table.getDbName().toLowerCase());
                        dgiBridge.registerTable(dbReferenceable, table.getDbName(), table.getTableName());
                    }
                }
                break;

            case CREATETABLE_AS_SELECT:
                registerCTAS(dgiBridge, hookContext, conf);
                break;

            default:
        }
    }

    private void registerCTAS(HiveMetaStoreBridge dgiBridge, HookContext hookContext, HiveConf conf) throws Exception {
        LOG.debug("Registering CTAS");

        Set<ReadEntity> inputs = hookContext.getInputs();
        Set<WriteEntity> outputs = hookContext.getOutputs();

        //Even explain CTAS has operation name as CREATETABLE_AS_SELECT
        if (inputs.isEmpty() && outputs.isEmpty()) {
            LOG.info("Explain statement. Skipping...");
        }

        String user = hookContext.getUserName();
        HiveOperation operation = HiveOperation.valueOf(hookContext.getOperationName());
        String queryId = null;
        String queryStr = null;
        long queryStartTime = 0;

        QueryPlan plan = hookContext.getQueryPlan();
        if (plan != null) {
            queryId = plan.getQueryId();
            queryStr = plan.getQueryString();
            queryStartTime = plan.getQueryStartTime();
        }

        Referenceable processReferenceable = new Referenceable(HiveDataTypes.HIVE_PROCESS.getName());
        processReferenceable.set("processName", operation.getOperationName());
        processReferenceable.set("startTime", queryStartTime);
        processReferenceable.set("userName", user);
        List<Referenceable> source = new ArrayList<>();
        for (ReadEntity readEntity : inputs) {
            if (readEntity.getTyp() == Entity.Type.TABLE) {
                Table table = readEntity.getTable();
                String dbName = table.getDbName().toLowerCase();
                source.add(getTableReference(dgiBridge, dbName, table.getTableName()));
            }
        }
        processReferenceable.set("sourceTableNames", source);
        List<Referenceable> target = new ArrayList<>();
        for (WriteEntity writeEntity : outputs) {
            if (writeEntity.getTyp() == Entity.Type.TABLE) {
                Table table = writeEntity.getTable();
                String dbName = table.getDbName().toLowerCase();
                target.add(getTableReference(dgiBridge, dbName, table.getTableName()));
            }
        }
        processReferenceable.set("targetTableNames", target);
        processReferenceable.set("queryText", queryStr);
        processReferenceable.set("queryId", queryId);
        processReferenceable.set("queryPlan", getQueryPlan(hookContext, conf));
        processReferenceable.set("endTime", System.currentTimeMillis());

        //TODO set
        processReferenceable.set("queryGraph", "queryGraph");
        dgiBridge.createInstance(processReferenceable);
    }

    /**
     * Gets reference for the database. Creates new instance if it doesn't exist
     *
     * @param dgiBridge
     * @param dbName    database name
     * @return Reference for database
     * @throws Exception
     */
    private Referenceable getDatabaseReference(HiveMetaStoreBridge dgiBridge, String dbName) throws Exception {
        String typeName = HiveDataTypes.HIVE_DB.getName();
        MetadataServiceClient dgiClient = dgiBridge.getMetadataServiceClient();

        JSONObject result = dgiClient.rawSearch(typeName, "name", dbName);
        JSONArray results = (JSONArray) result.get("results");

        if (results.length() == 0) {
            //Create new instance
            return dgiBridge.registerDatabase(dbName);

        } else {
            String guid = (String) ((JSONObject) results.get(0)).get("guid");
            return new Referenceable(guid, typeName, null);
        }
    }

    /**
     * Gets reference for the table. Creates new instance if it doesn't exist
     *
     * @param dgiBridge
     * @param dbName
     * @param tableName table name
     * @return table reference
     * @throws Exception
     */
    private Referenceable getTableReference(HiveMetaStoreBridge dgiBridge, String dbName, String tableName) throws Exception {
        String typeName = HiveDataTypes.HIVE_TABLE.getName();
        MetadataServiceClient dgiClient = dgiBridge.getMetadataServiceClient();

        JSONObject result = dgiClient.rawSearch(typeName, "tableName", tableName);
        JSONArray results = (JSONArray) result.get("results");

        if (results.length() == 0) {
            Referenceable dbRererence = getDatabaseReference(dgiBridge, dbName);
            return dgiBridge.registerTable(dbRererence, dbName, tableName).first;

        } else {
            //There should be just one instance with the given name
            String guid = (String) ((JSONObject) results.get(0)).get("guid");
            return new Referenceable(guid, typeName, null);
        }
    }


    private String getQueryPlan(HookContext hookContext, HiveConf conf) throws Exception {
        //We need to somehow get the sem associated with the plan and use it here.
        MySemanticAnaylzer sem = new MySemanticAnaylzer(conf);
        QueryPlan queryPlan = hookContext.getQueryPlan();
        sem.setInputs(queryPlan.getInputs());
        ExplainWork ew = new ExplainWork(null, null, queryPlan.getRootTasks(), queryPlan.getFetchTask(), null, sem,
                false, true, false, false, false);

        ExplainTask explain = new ExplainTask();
        explain.initialize(conf, queryPlan, null);

        org.json.JSONObject explainPlan = explain.getJSONPlan(null, ew);
        return explainPlan.toString();
    }

    private void analyzeHiveParseTree(ASTNode ast) {
        String astStr = ast.dump();
        Tree tab = ast.getChild(0);
        String fullTableName;
        boolean isExternal = false;
        boolean isTemporary = false;
        String inputFormat = null;
        String outputFormat = null;
        String serde = null;
        String storageHandler = null;
        String likeTableName = null;
        String comment = null;
        Tree ctasNode = null;
        Tree rowFormatNode = null;
        String location = null;
        Map<String, String> serdeProps = new HashMap<>();

        try {

            BufferedWriter fw = new BufferedWriter(
                    new FileWriter(new File("/tmp/dgi/", "ASTDump"), true));

            fw.write("Full AST Dump" + astStr);


            switch (ast.getToken().getType()) {
                case HiveParser.TOK_CREATETABLE:


                    if (tab.getType() != HiveParser.TOK_TABNAME ||
                            (tab.getChildCount() != 1 && tab.getChildCount() != 2)) {
                        LOG.error("Ignoring malformed Create table statement");
                    }
                    if (tab.getChildCount() == 2) {
                        String dbName = BaseSemanticAnalyzer
                                .unescapeIdentifier(tab.getChild(0).getText());
                        String tableName = BaseSemanticAnalyzer
                                .unescapeIdentifier(tab.getChild(1).getText());

                        fullTableName = dbName + "." + tableName;
                    } else {
                        fullTableName = BaseSemanticAnalyzer
                                .unescapeIdentifier(tab.getChild(0).getText());
                    }
                    LOG.info("Creating table " + fullTableName);
                    int numCh = ast.getChildCount();

                    for (int num = 1; num < numCh; num++) {
                        ASTNode child = (ASTNode) ast.getChild(num);
                        // Handle storage format
                        switch (child.getToken().getType()) {
                            case HiveParser.TOK_TABLEFILEFORMAT:
                                if (child.getChildCount() < 2) {
                                    throw new SemanticException(
                                            "Incomplete specification of File Format. " +
                                                    "You must provide InputFormat, OutputFormat.");
                                }
                                inputFormat = BaseSemanticAnalyzer
                                        .unescapeSQLString(child.getChild(0).getText());
                                outputFormat = BaseSemanticAnalyzer
                                        .unescapeSQLString(child.getChild(1).getText());
                                if (child.getChildCount() == 3) {
                                    serde = BaseSemanticAnalyzer
                                            .unescapeSQLString(child.getChild(2).getText());
                                }
                                break;
                            case HiveParser.TOK_STORAGEHANDLER:
                                storageHandler = BaseSemanticAnalyzer
                                        .unescapeSQLString(child.getChild(0).getText());
                                if (child.getChildCount() == 2) {
                                    BaseSemanticAnalyzer.readProps(
                                            (ASTNode) (child.getChild(1).getChild(0)),
                                            serdeProps);
                                }
                                break;
                            case HiveParser.TOK_FILEFORMAT_GENERIC:
                                ASTNode grandChild = (ASTNode) child.getChild(0);
                                String name = (grandChild == null ? "" : grandChild.getText())
                                        .trim().toUpperCase();
                                if (name.isEmpty()) {
                                    LOG.error("File format in STORED AS clause is empty");
                                    break;
                                }
                                break;
                        }

                        switch (child.getToken().getType()) {

                            case HiveParser.KW_EXTERNAL:
                                isExternal = true;
                                break;
                            case HiveParser.KW_TEMPORARY:
                                isTemporary = true;
                                break;
                            case HiveParser.TOK_LIKETABLE:
                                if (child.getChildCount() > 0) {
                                    likeTableName = BaseSemanticAnalyzer
                                            .getUnescapedName((ASTNode) child.getChild(0));
                                }
                                break;
                            case HiveParser.TOK_QUERY:
                                ctasNode = child;
                                break;
                            case HiveParser.TOK_TABLECOMMENT:
                                comment = BaseSemanticAnalyzer
                                        .unescapeSQLString(child.getChild(0).getText());
                                break;
                            case HiveParser.TOK_TABLEPARTCOLS:
                            case HiveParser.TOK_TABCOLLIST:
                            case HiveParser.TOK_ALTERTABLE_BUCKETS:
                                break;
                            case HiveParser.TOK_TABLEROWFORMAT:
                                rowFormatNode = child;
                                break;
                            case HiveParser.TOK_TABLELOCATION:
                                location = BaseSemanticAnalyzer
                                        .unescapeSQLString(child.getChild(0).getText());
                                break;
                            case HiveParser.TOK_TABLEPROPERTIES:
                                break;
                            case HiveParser.TOK_TABLESERIALIZER:
                                child = (ASTNode) child.getChild(0);
                                serde = BaseSemanticAnalyzer
                                        .unescapeSQLString(child.getChild(0).getText());
                                break;
                            case HiveParser.TOK_TABLESKEWED:
                                break;
                            default:
                                throw new AssertionError("Unknown token: " + child.getToken());
                        }
                    }
                    StringBuilder sb = new StringBuilder(1024);
                    sb.append("Full table name: ").append(fullTableName).append('\n');
                    sb.append("\tisTemporary: ").append(isTemporary).append('\n');
                    sb.append("\tIsExternal: ").append(isExternal).append('\n');
                    if (inputFormat != null) {
                        sb.append("\tinputFormat: ").append(inputFormat).append('\n');
                    }
                    if (outputFormat != null) {
                        sb.append("\toutputFormat: ").append(outputFormat).append('\n');
                    }
                    if (serde != null) {
                        sb.append("\tserde: ").append(serde).append('\n');
                    }
                    if (storageHandler != null) {
                        sb.append("\tstorageHandler: ").append(storageHandler).append('\n');
                    }
                    if (likeTableName != null) {
                        sb.append("\tlikeTableName: ").append(likeTableName);
                    }
                    if (comment != null) {
                        sb.append("\tcomment: ").append(comment);
                    }
                    if (location != null) {
                        sb.append("\tlocation: ").append(location);
                    }
                    if (ctasNode != null) {
                        sb.append("\tctasNode: ").append(((ASTNode) ctasNode).dump());
                    }
                    if (rowFormatNode != null) {
                        sb.append("\trowFormatNode: ").append(((ASTNode) rowFormatNode).dump());
                    }
                    fw.write(sb.toString());
            }
            fw.flush();
            fw.close();
        } catch (Exception e) {
            LOG.error("Unable to log logical plan to file", e);
        }
    }

    private void parseQuery(String sqlText) throws Exception {
        ParseDriver parseDriver = new ParseDriver();
        ASTNode node = parseDriver.parse(sqlText);
        analyzeHiveParseTree(node);
    }

    /**
     * This is  an attempt to use the parser.  Sematnic issues are not handled here.
     * <p/>
     * Trying to recompile the query runs into some issues in the preExec
     * hook but we need to make sure all the semantic issues are handled.  May be we should save the AST in the
     * Semantic analyzer and have it available in the preExec hook so that we walk with it freely.
     *
     * @param context
     * @param ast
     * @return
     * @throws SemanticException
     */
    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
            throws SemanticException {
        analyzeHiveParseTree(ast);
        return ast;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context,
                            List<Task<? extends Serializable>> rootTasks) throws SemanticException {

    }

    private class MySemanticAnaylzer extends BaseSemanticAnalyzer {
        public MySemanticAnaylzer(HiveConf conf) throws SemanticException {
            super(conf);
        }

        public void analyzeInternal(ASTNode ast) throws SemanticException {
            throw new RuntimeException("Not implemented");
        }

        public void setInputs(HashSet<ReadEntity> inputs) {
            this.inputs = inputs;
        }
    }
}
