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

package org.apache.hadoop.metadata.bridge.hivelineage.hook;

import java.io.Serializable;
import java.util.ArrayList;


public class HiveLineage implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public String queryId;
    public String hiveId;
    public String user;
    public String queryStartTime;
    public String queryEndTime;
    public String query;
    public String tableName;
    public String databaseName;
    public String action;
    public String tableLocation;
    public boolean success;
    public boolean failed;
    public String executionEngine;
    ArrayList<SourceTables> sourceTables;
    ArrayList<QueryColumns> queryColumns;
    ArrayList<WhereClause> whereClause;
    ArrayList<CreateColumns> createColumns;
    ArrayList<GroupBy> groupBy;
    ArrayList<GroupBy> orderBy;


    public String getQueryId() {
        return this.queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getExecutionEngine() {
        return this.executionEngine;
    }

    public void setExecutionEngine(String executionEngine) {
        this.executionEngine = executionEngine;
    }

    public String getHiveId() {
        return this.hiveId;
    }

    public void setHiveId(String hiveId) {
        this.hiveId = hiveId;
    }

    public boolean getSuccess() {
        return this.success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public boolean getFailed() {
        return this.failed;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
    }


    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public String getAction() {
        return this.action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableLocation() {
        return this.tableLocation;
    }

    public void setTableLocation(String tableLocation) {
        this.tableLocation = tableLocation;
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getQueryStartTime() {
        return this.queryStartTime;
    }

    public void setQueryStartTime(String queryStartTime) {
        this.queryStartTime = queryStartTime;
    }

    public String getQueryEndTime() {
        return this.queryEndTime;
    }

    public void setQueryEndTime(String queryEndTime) {
        this.queryEndTime = queryEndTime;
    }

    public String getQuery() {
        return this.query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public ArrayList<SourceTables> getSourceTables() {
        return this.sourceTables;
    }

    public void setSourceTables(ArrayList<SourceTables> sourceTables) {
        this.sourceTables = sourceTables;
    }

    public ArrayList<QueryColumns> getQueryColumns() {
        return this.queryColumns;
    }

    public void setQueryColumns(ArrayList<QueryColumns> queryColumns) {
        this.queryColumns = queryColumns;
    }


    public ArrayList<WhereClause> getWhereClause() {
        return this.whereClause;
    }

    public void setWhereClause(ArrayList<WhereClause> whereClause) {
        this.whereClause = whereClause;
    }


    public ArrayList<GroupBy> getGroupBy() {
        return this.groupBy;
    }

    public void setGroupBy(ArrayList<GroupBy> groupBy) {
        this.groupBy = groupBy;
    }

    public ArrayList<CreateColumns> getCreateColumns() {
        return this.createColumns;
    }

    public void setCreateColumns(ArrayList<CreateColumns> createColumns) {
        this.createColumns = createColumns;
    }

    public class SourceTables {
        public String tableName;
        public String tableAlias;
        public String databaseName;

        public String getTableName() {
            return this.tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getTableAlias() {
            return this.tableAlias;
        }

        public void setTableAlias(String tableAlias) {
            this.tableAlias = tableAlias;
        }


        public String getDatabaseName() {
            return this.databaseName;
        }

        public void setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
        }
    }

    public class QueryColumns {
        public String tbAliasOrName;
        public String columnName;
        public String columnAlias;
        public String columnFunction;
        public String columnDistinctFunction;

        public String getTbAliasOrName() {
            return this.tbAliasOrName;
        }

        public void setTbAliasOrName(String tbAliasOrName) {
            this.tbAliasOrName = tbAliasOrName;
        }

        public String getColumnName() {
            return this.columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnAlias() {
            return this.columnAlias;
        }

        public void setColumnAlias(String columnAlias) {
            this.columnAlias = columnAlias;
        }


        public String getColumnFunction() {
            return this.columnFunction;
        }

        public void setColumnFunction(String columnFunction) {
            this.columnFunction = columnFunction;
        }

        public String getColumnDistinctFunction() {
            return this.columnDistinctFunction;
        }

        public void setColumnDistinctFunction(String columnDistinctFunction) {
            this.columnDistinctFunction = columnDistinctFunction;
        }
    }

    public class GroupBy {
        public String tbAliasOrName;
        public String columnName;

        public String getTbAliasOrName() {
            return this.tbAliasOrName;
        }

        public void setTbAliasOrName(String tbAliasOrName) {
            this.tbAliasOrName = tbAliasOrName;
        }

        public String getColumnName() {
            return this.columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }
    }

    public class WhereClause {
        public String tbAliasOrName;
        public String columnCondition;
        public String columnName;
        public String columnOperator;
        public String columnValue;
        public ArrayList<ColumnValueIn> columnValueIn;


        public String getColumnCondition() {
            return this.columnCondition;
        }

        public void setColumnCondition(String columnCondition) {
            this.columnCondition = columnCondition;
        }

        public String getTbAliasOrName() {
            return this.tbAliasOrName;
        }

        public void setTbAliasOrName(String tbAliasOrName) {
            this.tbAliasOrName = tbAliasOrName;
        }

        public String getColumnName() {
            return this.columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnOperator() {
            return this.columnOperator;
        }

        public void setColumnOperator(String columnOperator) {
            this.columnOperator = columnOperator;
        }


        public String getColumnValue() {
            return this.columnValue;
        }

        public void setColumnValue(String columnValue) {
            this.columnValue = columnValue;
        }


        public ArrayList<ColumnValueIn> getColumnValueIn() {
            return this.columnValueIn;
        }

        public void setColumnValueIn(ArrayList<ColumnValueIn> columnValueIn) {
            this.columnValueIn = columnValueIn;
        }


    }

    public class CreateColumns {
        public String columnName;
        public String columnType;

        public String getColumnName() {
            return this.columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnType() {
            return this.columnType;
        }

        public void setColumnType(String columnType) {
            this.columnType = columnType;
        }
    }

    public class ColumnValueIn {
        public String columnValueIn;

        public String getColumnValueIn() {
            return this.columnValueIn;
        }

        public void setColumnValueIn(String columnValueIn) {
            this.columnValueIn = columnValueIn;
        }


    }

}
