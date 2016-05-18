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

package org.apache.atlas.discovery;

import org.apache.atlas.AtlasException;

/**
 * Lineage service interface.
 */
public interface LineageService {

    /**
     * Return the lineage outputs graph for the given datasetName.
     *
     * @param datasetName datasetName
     * @return Outputs Graph as JSON
     */
    String getOutputsGraph(String datasetName) throws AtlasException;

    /**
     * Return the lineage inputs graph for the given datasetName.
     *
     * @param datasetName datasetName
     * @return Inputs Graph as JSON
     */
    String getInputsGraph(String datasetName) throws AtlasException;

    /**
     * Return the lineage inputs graph for the given entity id.
     *
     * @param guid entity id
     * @return Inputs Graph as JSON
     */
    String getInputsGraphForEntity(String guid) throws AtlasException;

    /**
     * Return the lineage inputs graph for the given entity id.
     *
     * @param guid entity id
     * @return Inputs Graph as JSON
     */
    String getOutputsGraphForEntity(String guid) throws AtlasException;

    /**
     * Return the schema for the given datasetName.
     *
     * @param datasetName datasetName
     * @return Schema as JSON
     */
    String getSchema(String datasetName) throws AtlasException;

    /**
     * Return the schema for the given entity id.
     *
     * @param guid tableName
     * @return Schema as JSON
     */
    String getSchemaForEntity(String guid) throws AtlasException;
}
