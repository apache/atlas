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
package org.apache.atlas.trino.model;

import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.trino.connector.AtlasEntityConnector;
import org.apache.atlas.trino.connector.ConnectorFactory;

public class Catalog {
    private final String                 name;
    private final String                 type;
    private final String                 hookInstanceName;
    private final String                 instanceName;
    private final AtlasEntityConnector   connector;
    private       AtlasEntityWithExtInfo trinoInstanceEntity;
    private       String                 schemaToImport;
    private       String                 tableToImport;

    public Catalog(String name, String type, boolean hookEnabled, String hookInstanceName, String instanceName) {
        this.name             = name;
        this.type             = type;
        this.hookInstanceName = hookInstanceName;
        this.instanceName     = instanceName;
        this.connector        = hookEnabled ? ConnectorFactory.getConnector(type) : null;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public AtlasEntityConnector getConnector() {
        return connector;
    }

    public String getHookInstanceName() {
        return hookInstanceName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public AtlasEntityWithExtInfo getTrinoInstanceEntity() {
        return trinoInstanceEntity;
    }

    public void setTrinoInstanceEntity(AtlasEntityWithExtInfo trinoInstanceEntity) {
        this.trinoInstanceEntity = trinoInstanceEntity;
    }

    public String getTableToImport() {
        return tableToImport;
    }

    public void setTableToImport(String tableToImport) {
        this.tableToImport = tableToImport;
    }

    public String getSchemaToImport() {
        return schemaToImport;
    }

    public void setSchemaToImport(String schemaToImport) {
        this.schemaToImport = schemaToImport;
    }
}
