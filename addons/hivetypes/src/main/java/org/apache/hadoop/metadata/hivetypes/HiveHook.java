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

package org.apache.hadoop.metadata.hivetypes;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * DgiHook sends lineage information to the DgiSever
 */
public class HiveHook implements ExecuteWithHookContext, HiveSemanticAnalyzerHook {

    private static final Log LOG = LogFactory.getLog(HiveHook.class.getName());
    private static final String dgcDumpDir = "/tmp/dgcfiles";
    // wait time determines how long we wait before we exit the jvm on
    // shutdown. Pending requests after that will not be sent.
    private static final int WAIT_TIME = 3;
    private static final String dbHost = "10.11.4.125";
    private static final String url = "jdbc:postgres://" + dbHost + "/dgctest";
    private static final String user = "postgres";
    private static final String password = "postgres";
    private static final String insertQuery =
            "insert into query_info(query_id, query_text, query_plan, start_time, user_name, " +
                    "query_graph) "
                    + "values (?, ?, ?, ?, ?, ?";
    private static final String updateQuery =
            "update  query_info set end_time = ? where query_id = ?";
    private static ExecutorService executor;

    static {
        // anything shared should be initialized here and destroyed in the
        // shutdown hook The hook contract is weird in that it creates a
        // boatload of hooks.

        // initialize the async facility to process hook calls. We don't
        // want to do this inline since it adds plenty of overhead for the
        // query.
        executor = Executors.newSingleThreadExecutor(
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
        LOG.info("Created DgiHook");
    }

    Connection connection = null;
    PreparedStatement insertStatement = null;
    PreparedStatement updateStatement = null;
    private HiveTypeSystem hiveTypeSystem;

    public HiveHook() {
        try {
            File dgcDumpFile = new File(dgcDumpDir);
            dgcDumpFile.mkdirs();
            connection = DriverManager.getConnection(url, user, password);
            insertStatement = connection.prepareStatement(insertQuery);
            updateStatement = connection.prepareStatement(updateQuery);
        } catch (Exception e) {
            LOG.error("Exception initializing HiveHook " + e);
        }
    }

    @Override
    public void run(final HookContext hookContext) throws Exception {
        if (executor == null) {
            LOG.info("No executor running. Bail.");
            return;
        }

        final long currentTime = System.currentTimeMillis();

        // clone to avoid concurrent access
        final HiveConf conf = new HiveConf(hookContext.getConf());

        executor.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {

                            QueryPlan plan = hookContext.getQueryPlan();
                            if (plan == null) {
                                LOG.info("no plan in callback.");
                                return;
                            }

                            String queryId = plan.getQueryId();
                            long queryStartTime = plan.getQueryStartTime();
                            String user = hookContext.getUgi().getUserName();
                            String operationType = hookContext.getOperationName();
                            Set<WriteEntity> outputs = plan.getOutputs();
                            Set<ReadEntity> inputs = plan.getInputs();

                            switch (hookContext.getHookType()) {

                                case PRE_EXEC_HOOK: // command about to execute
                                    ExplainTask explain = new ExplainTask();
                                    explain.initialize(conf, plan, null);
                                    String query = plan.getQueryStr();
                                    List<Task<?>> rootTasks = plan.getRootTasks();

                                    //We need to somehow get the sem associated with the plan and
                                    // use it here.
                                    //MySemanticAnaylzer sem = new MySemanticAnaylzer(conf);
                                    //sem.setInputs(plan.getInputs());
                                    //ExplainWork ew = new ExplainWork(null, null, rootTasks,
                                    // plan.getFetchTask(), null, sem,
                                    //        false, true, false, false, false);
                                    //JSONObject explainPlan =
                                    //        explain.getJSONLogicalPlan(null, ew);
                                    String graph = "";
                                    if (plan.getQuery().getStageGraph() != null) {
                                        graph = plan.getQuery().getStageGraph().toString();
                                    }
                                    JSONObject explainPlan =
                                            explain.getJSONPlan(null, null, rootTasks,
                                                    plan.getFetchTask(), true, false, false);
                                    fireAndForget(conf,
                                            createPreHookEvent(queryId, query,
                                                    explainPlan, queryStartTime,
                                                    user, inputs, outputs, graph));
                                    break;
                                case POST_EXEC_HOOK: // command succeeded successfully
                                    fireAndForget(conf,
                                            createPostHookEvent(queryId, currentTime, user,
                                                    true, inputs, outputs));
                                    break;
                                case ON_FAILURE_HOOK: // command failed
                                    fireAndForget(conf,
                                            createPostHookEvent(queryId, currentTime, user,
                                                    false, inputs, outputs));
                                    break;
                                default:
                                    //ignore
                                    LOG.info("unknown hook type");
                                    break;
                            }
                        } catch (Exception e) {
                            LOG.info("Failed to submit plan: " + StringUtils.stringifyException(e));
                        }
                    }
                }
        );
    }

    private void appendEntities(JSONObject obj, String key,
                                Set<? extends Entity> entities)
    throws JSONException {

        for (Entity e : entities) {
            if (e != null) {
                JSONObject entityObj = new JSONObject();
                entityObj.put("type", e.getType().toString());
                entityObj.put("name", e.toString());

                obj.append(key, entityObj);
            }
        }
    }

    private JSONObject createPreHookEvent(String queryId, String query,
                                          JSONObject explainPlan, long startTime, String user,
                                          Set<ReadEntity> inputs, Set<WriteEntity> outputs,
                                          String graph)
    throws JSONException {

        JSONObject queryObj = new JSONObject();

        queryObj.put("queryText", query);
        queryObj.put("queryPlan", explainPlan);
        queryObj.put("queryId", queryId);
        queryObj.put("startTime", startTime);
        queryObj.put("user", user);
        queryObj.put("graph", graph);

        appendEntities(queryObj, "inputs", inputs);
        appendEntities(queryObj, "output", outputs);

        LOG.info("Received pre-hook notification for :" + queryId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("DGI Info: " + queryObj.toString(2));
        }

        return queryObj;
    }

    private JSONObject createPostHookEvent(String queryId, long stopTime,
                                           String user, boolean success, Set<ReadEntity> inputs,
                                           Set<WriteEntity> outputs)
    throws JSONException {

        JSONObject completionObj = new JSONObject();

        completionObj.put("queryId", queryId);
        completionObj.put("stopTime", stopTime);
        completionObj.put("user", user);
        completionObj.put("result", success);

        appendEntities(completionObj, "inputs", inputs);
        appendEntities(completionObj, "output", outputs);

        LOG.info("Received post-hook notification for :" + queryId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("DGI Info: " + completionObj.toString(2));
        }

        return completionObj;
    }

    private synchronized void fireAndForget(Configuration conf, JSONObject obj) throws Exception {

        LOG.info("Submitting: " + obj.toString(2));

        String queryId = (String) obj.get("queryId");

        try {
            BufferedWriter fw = new BufferedWriter(
                    new FileWriter(new File(dgcDumpDir, queryId), true));
            fw.write(obj.toString(2));
            fw.flush();
            fw.close();
        } catch (Exception e) {
            LOG.error("Unable to log logical plan to file", e);
        }
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
                    new FileWriter(new File(dgcDumpDir, "ASTDump"), true));

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
     *
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
