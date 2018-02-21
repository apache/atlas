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

package org.apache.atlas.repository;

/**
 * Repository Constants.
 *
 */
public final class Constants {

    /**
     * Globally Unique identifier property key.
     */

    public static final String INTERNAL_PROPERTY_KEY_PREFIX = "__";
    public static final String RELATIONSHIP_PROPERTY_KEY_PREFIX = "_r";
    public static final String GUID_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "guid";
    public static final String RELATIONSHIP_GUID_PROPERTY_KEY = RELATIONSHIP_PROPERTY_KEY_PREFIX + GUID_PROPERTY_KEY;

    /**
     * Entity type name property key.
     */
    public static final String ENTITY_TYPE_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "typeName";
    public static final String TYPE_NAME_INTERNAL       = INTERNAL_PROPERTY_KEY_PREFIX + "internal";

    /**
     * Entity type's super types property key.
     */
    public static final String SUPER_TYPES_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "superTypeNames";

    /**
     * Full-text for the entity for enabling full-text search.
     */
    //weird issue in TitanDB if __ added to this property key. Not adding it for now
    public static final String ENTITY_TEXT_PROPERTY_KEY = "entityText";

    /**
     * Properties for type store graph.
     */
    public static final String TYPE_CATEGORY_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "type.category";
    public static final String VERTEX_TYPE_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "type";
    public static final String TYPENAME_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "type.name";
    public static final String TYPEDESCRIPTION_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "type.description";
    public static final String TYPEVERSION_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "type.version";
    public static final String TYPEOPTIONS_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "type.options";

    // relationship def constants
    public static final String RELATIONSHIPTYPE_END1_KEY = "endDef1";
    public static final String RELATIONSHIPTYPE_END2_KEY = "endDef2";
    public static final String RELATIONSHIPTYPE_CATEGORY_KEY = "relationshipCategory";
    public static final String RELATIONSHIPTYPE_TAG_PROPAGATION_KEY = "tagPropagation";
    /**
     * Trait names property key and index name.
     */
    public static final String TRAIT_NAMES_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "traitNames";
    public static final String PROPAGATED_TRAIT_NAMES_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "propagatedTraitNames";

    public static final String VERSION_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "version";
    public static final String STATE_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "state";
    public static final String CREATED_BY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "createdBy";
    public static final String MODIFIED_BY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "modifiedBy";

    public static final String TIMESTAMP_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "timestamp";

    public static final String MODIFICATION_TIMESTAMP_PROPERTY_KEY =
        INTERNAL_PROPERTY_KEY_PREFIX + "modificationTimestamp";

    /**
     * search backing index name.
     */
    public static final String BACKING_INDEX = "search";

    /**
     * search backing index name for vertex keys.
     */
    public static final String VERTEX_INDEX = "vertex_index";

    /**
     * search backing index name for edge labels.
     */
    public static final String EDGE_INDEX = "edge_index";

    public static final String FULLTEXT_INDEX = "fulltext_index";

    public static final String QUALIFIED_NAME = "Referenceable.qualifiedName";
    public static final String TYPE_NAME_PROPERTY_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "typeName";
    public static final String INDEX_SEARCH_MAX_RESULT_SET_SIZE = "atlas.graph.index.search.max-result-set-size";
    public static final String INDEX_SEARCH_TYPES_MAX_QUERY_STR_LENGTH = "atlas.graph.index.search.types.max-query-str-length";
    public static final String INDEX_SEARCH_TAGS_MAX_QUERY_STR_LENGTH  = "atlas.graph.index.search.tags.max-query-str-length";
    public static final String INDEX_SEARCH_VERTEX_PREFIX_PROPERTY  = "atlas.graph.index.search.vertex.prefix";
    public static final String INDEX_SEARCH_VERTEX_PREFIX_DEFAULT = "$v$";

    public static final String MAX_FULLTEXT_QUERY_STR_LENGTH  = "atlas.graph.fulltext-max-query-str-length";
    public static final String MAX_DSL_QUERY_STR_LENGTH  = "atlas.graph.dsl-max-query-str-length";

    public static final String ATTRIBUTE_NAME_GUID     = "guid";
    public static final String ATTRIBUTE_NAME_TYPENAME = "typeName";
    public static final String ATTRIBUTE_NAME_SUPERTYPENAMES = "superTypeNames";
    public static final String ATTRIBUTE_NAME_STATE    = "state";
    public static final String ATTRIBUTE_NAME_VERSION  = "version";
    public static final String TEMP_STRUCT_NAME_PREFIX = "__tempQueryResultStruct";

    public static final String CLASSIFICATION_ENTITY_GUID                   = INTERNAL_PROPERTY_KEY_PREFIX + "entityGuid";
    public static final String CLASSIFICATION_PROPAGATE_KEY                 = INTERNAL_PROPERTY_KEY_PREFIX + "propagate";
    public static final String CLASSIFICATION_VALIDITY_PERIOD_STARTTIME_KEY = INTERNAL_PROPERTY_KEY_PREFIX + "vp_startTime";
    public static final String CLASSIFICATION_VALIDITY_PERIOD_ENDTIME_KEY   = INTERNAL_PROPERTY_KEY_PREFIX + "vp_endTime";
    public static final String CLASSIFICATION_VALIDITY_PERIOD_TIMEZONE_KEY  = INTERNAL_PROPERTY_KEY_PREFIX + "vp_timeZone";

    private Constants() {
    }

}
