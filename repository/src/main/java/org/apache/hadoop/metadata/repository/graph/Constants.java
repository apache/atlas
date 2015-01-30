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

package org.apache.hadoop.metadata.repository.graph;

final class Constants {

    private Constants() {
    }

    static final String GUID_PROPERTY_KEY = "guid";
    static final String GUID_INDEX = "guid_index";

    static final String ENTITY_TYPE_PROPERTY_KEY = "type";
    static final String ENTITY_TYPE_INDEX = "type_index";

    static final String VERSION_PROPERTY_KEY = "version";
    static final String TIMESTAMP_PROPERTY_KEY = "timestamp";

    /**
     * search backing index name.
     */
    static final String BACKING_INDEX = "search";
    static final String INDEX_NAME = "metadata";

    /**
     * search backing index name for vertex keys.
     */
    static final String VERTEX_INDEX = "vertex_index";

    /**
     * search backing index name for edge labels.
     */
    static final String EDGE_INDEX = "edge_index";
}
