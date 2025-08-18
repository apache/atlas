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
package org.apache.atlas.trino.client;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrinoClientHelper {
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public TrinoClientHelper(Configuration atlasConf) {
        this.jdbcUrl  = atlasConf.getString("atlas.trino.jdbc.address");
        this.username = atlasConf.getString("atlas.trino.jdbc.user");
        this.password = atlasConf.getString("atlas.trino.jdbc.password", "");
    }

    public Map<String, String> getAllTrinoCatalogs() {
        Map<String, String> catalogs = new HashMap<>();

        try (Connection connection = getTrinoConnection();
             Statement  stmt       = connection.createStatement()) {
            String     query = "SELECT catalog_name, connector_name FROM system.metadata.catalogs";
            ResultSet  rs    = stmt.executeQuery(query);

            while (rs.next()) {
                catalogs.put(rs.getString("catalog_name"), rs.getString("connector_name"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return catalogs;
    }

    public List<String> getTrinoSchemas(String catalog, String schemaToImport) throws SQLException {
        List<String>  schemas    = new ArrayList<>();

        try (Connection connection = getTrinoConnection();
             Statement  stmt       = connection.createStatement()) {
            StringBuilder query = new StringBuilder();

            query.append("SELECT schema_name FROM ").append(catalog).append(".information_schema.schemata");

            if (StringUtils.isNotEmpty(schemaToImport)) {
                query.append(" where schema_name = '").append(schemaToImport).append("'");
            }

            ResultSet rs = stmt.executeQuery(query.toString());

            while (rs.next()) {
                schemas.add(rs.getString("schema_name"));
            }
        }

        return schemas;
    }

    public Map<String, Map<String, Object>> getTrinoTables(String catalog, String schema, String tableToImport) throws SQLException {
        Map<String, Map<String, Object>> tables     = new HashMap<>();

        try (Connection connection = getTrinoConnection();
             Statement  stmt       = connection.createStatement()) {
            StringBuilder query = new StringBuilder();

            query.append("SELECT table_name, table_type FROM ").append(catalog).append(".information_schema.tables WHERE table_schema = '").append(schema).append("'");

            if (StringUtils.isNotEmpty(tableToImport)) {
                query.append(" and table_name = '").append(tableToImport).append("'");
            }

            ResultSet rs = stmt.executeQuery(query.toString());

            while (rs.next()) {
                Map<String, Object> tableMetadata = new HashMap<>();

                tableMetadata.put("table_name", rs.getString("table_name"));
                tableMetadata.put("table_type", rs.getString("table_type"));

                tables.put(rs.getString("table_name"), tableMetadata);
            }
        }

        return tables;
    }

    public Map<String, Map<String, Object>> getTrinoColumns(String catalog, String schema, String table) throws SQLException {
        Map<String, Map<String, Object>> columns = new HashMap<>();

        try (Connection connection = getTrinoConnection();
             Statement  stmt       = connection.createStatement()) {
            StringBuilder query = new StringBuilder();

            query.append("SELECT column_name, ordinal_position, column_default, is_nullable, data_type FROM ").append(catalog).append(".information_schema.columns WHERE table_schema = '").append(schema).append("' AND table_name = '").append(table).append("'");

            ResultSet rs = stmt.executeQuery(query.toString());

            while (rs.next()) {
                Map<String, Object> columnMetadata = new HashMap<>();

                columnMetadata.put("ordinal_position", rs.getInt("ordinal_position"));
                columnMetadata.put("column_default", rs.getString("column_default"));
                columnMetadata.put("column_name", rs.getString("column_name"));

                if (StringUtils.isNotEmpty(rs.getString("is_nullable"))) {
                    if (StringUtils.equalsIgnoreCase(rs.getString("is_nullable"), "YES")) {
                        columnMetadata.put("is_nullable", true);
                    } else {
                        columnMetadata.put("is_nullable", false);
                    }
                }

                columnMetadata.put("data_type", rs.getString("data_type"));

                columns.put(rs.getString("column_name"), columnMetadata);
            }
        }

        return columns;
    }

    private Connection getTrinoConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }
}
