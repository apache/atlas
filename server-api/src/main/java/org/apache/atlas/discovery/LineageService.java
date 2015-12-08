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
     * Return the lineage outputs for the given tableName.
     *
     * @param tableName tableName
     * @return Outputs as JSON
     */
    String getOutputs(String tableName) throws AtlasException;

    /**
     * Return the lineage outputs graph for the given tableName.
     *
     * @param tableName tableName
     * @return Outputs Graph as JSON
     */
    String getOutputsGraph(String tableName) throws AtlasException;

    /**
     * Return the lineage inputs for the given tableName.
     *
     * @param tableName tableName
     * @return Inputs as JSON
     */
    String getInputs(String tableName) throws AtlasException;

    /**
     * Return the lineage inputs graph for the given tableName.
     *
     * @param tableName tableName
     * @return Inputs Graph as JSON
     */
    String getInputsGraph(String tableName) throws AtlasException;

    /**
     * Return the schema for the given tableName.
     *
     * @param tableName tableName
     * @return Schema as JSON
     */
    String getSchema(String tableName) throws AtlasException;
}
