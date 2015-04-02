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

package org.apache.hadoop.metadata.repository;

public final class Constants {

    /**
     * Globally Unique identifier property key.
     */
    public static final String GUID_PROPERTY_KEY = "guid";
    public static final String GUID_INDEX = "guid_index";

    /**
     * Entity type name property key.
     */
    public static final String ENTITY_TYPE_PROPERTY_KEY = "typeName";
    public static final String ENTITY_TYPE_INDEX = "type_index";

    public static final String ENTITY_TEXT_PROPERTY_KEY = "entityText";

    /**
     * Properties for type store graph
     */
    public static final String TYPE_CATEGORY_PROPERTY_KEY = "type.category";
    public static final String VERTEX_TYPE_PROPERTY_KEY = "type";
    public static final String TYPENAME_PROPERTY_KEY = "type.name";

    /**
     * Trait names property key and index name.
     */
    public static final String TRAIT_NAMES_PROPERTY_KEY = "traitNames";
    public static final String TRAIT_NAMES_INDEX = "trait_names_index";

    public static final String VERSION_PROPERTY_KEY = "version";
    public static final String TIMESTAMP_PROPERTY_KEY = "timestamp";

    /**
     * search backing index name.
     */
    public static final String BACKING_INDEX = "search";
    public static final String INDEX_NAME = "metadata";

    /**
     * search backing index name for vertex keys.
     */
    public static final String VERTEX_INDEX = "vertex_index";

    /**
     * search backing index name for edge labels.
     */
    public static final String EDGE_INDEX = "edge_index";

    private Constants() {
    }
}
