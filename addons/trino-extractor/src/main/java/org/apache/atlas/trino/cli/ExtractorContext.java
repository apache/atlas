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
package org.apache.atlas.trino.cli;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.trino.client.AtlasClientHelper;
import org.apache.atlas.trino.client.TrinoClientHelper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;

public class ExtractorContext {
    static final String TRINO_NAMESPACE_CONF         = "atlas.trino.namespace";
    static final String DEFAULT_TRINO_NAMESPACE      = "cm";
    static final String OPTION_CATALOG_SHORT         = "c";
    static final String OPTION_CATALOG_LONG          = "catalog";
    static final String OPTION_SCHEMA_SHORT          = "s";
    static final String OPTION_SCHEMA_LONG           = "schema";
    static final String OPTION_TABLE_SHORT           = "t";
    static final String OPTION_TABLE_LONG            = "table";
    static final String OPTION_CRON_EXPRESSION_SHORT = "cx";
    static final String OPTION_CRON_EXPRESSION_LONG  = "cronExpression";
    static final String OPTION_HELP_SHORT            = "h";
    static final String OPTION_HELP_LONG             = "help";

    private final Configuration     atlasConf;
    private final String            namespace;
    private final String            catalog;
    private final String            schema;
    private final String            table;
    private final AtlasClientHelper atlasClientHelper;
    private final TrinoClientHelper trinoClientHelper;
    private final String            cronExpression;

    public ExtractorContext(CommandLine cmd) throws AtlasException, IOException {
        this.atlasConf         = getAtlasProperties();
        this.atlasClientHelper = createAtlasClientHelper();
        this.trinoClientHelper = createTrinoClientHelper();
        this.namespace         = atlasConf.getString(TRINO_NAMESPACE_CONF, DEFAULT_TRINO_NAMESPACE);
        this.catalog           = cmd.getOptionValue(OPTION_CATALOG_SHORT);
        this.schema            = cmd.getOptionValue(OPTION_SCHEMA_SHORT);
        this.table             = cmd.getOptionValue(OPTION_TABLE_SHORT);
        this.cronExpression    = cmd.getOptionValue(OPTION_CRON_EXPRESSION_SHORT);
    }

    public Configuration getAtlasConf() {
        return atlasConf;
    }

    public AtlasClientHelper getAtlasConnector() {
        return atlasClientHelper;
    }

    public TrinoClientHelper getTrinoConnector() {
        return trinoClientHelper;
    }

    public String getTable() {
        return table;
    }

    public String getSchema() {
        return schema;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    private Configuration getAtlasProperties() throws AtlasException {
        return ApplicationProperties.get();
    }

    private TrinoClientHelper createTrinoClientHelper() {
        return new TrinoClientHelper(atlasConf);
    }

    private AtlasClientHelper createAtlasClientHelper() throws IOException {
        return new AtlasClientHelper(atlasConf);
    }
}
