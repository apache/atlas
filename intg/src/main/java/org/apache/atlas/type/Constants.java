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
package org.apache.atlas.type;

import java.util.HashMap;

import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.encodePropertyKey;

/**
 * Intg Constants.
 */
public final class Constants {

    public static final String INTERNAL_PROPERTY_KEY_PREFIX        = "__";

    /**
     * Shared System Attributes
     */
    public static final String TYPE_NAME_PROPERTY_KEY               = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "typeName");
    public static final String STATE_PROPERTY_KEY                   = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "state");
    public static final String CREATED_BY_KEY                       = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "createdBy");
    public static final String MODIFIED_BY_KEY                      = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "modifiedBy");
    public static final String TIMESTAMP_PROPERTY_KEY               = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "timestamp");
    public static final String MODIFICATION_TIMESTAMP_PROPERTY_KEY  = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "modificationTimestamp");

    /**
     * Entity-Only System Attributes
     */
    public static final String GUID_PROPERTY_KEY                    = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "guid");
    public static final String HISTORICAL_GUID_PROPERTY_KEY         = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "historicalGuids");
    public static final String LABELS_PROPERTY_KEY                  = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "labels");
    public static final String CUSTOM_ATTRIBUTES_PROPERTY_KEY       = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "customAttributes");
    public static final String CLASSIFICATION_TEXT_KEY              = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "classificationsText");
    public static final String CLASSIFICATION_NAMES_KEY             = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "classificationNames");
    public static final String PROPAGATED_CLASSIFICATION_NAMES_KEY  = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "propagatedClassificationNames");
    public static final String IS_INCOMPLETE_PROPERTY_KEY           = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "isIncomplete");
    public static final String PENDING_TASKS_PROPERTY_KEY           = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "pendingTasks");
    public static final String MEANINGS_PROPERTY_KEY                = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "meanings");
    public static final String GLOSSARY_PROPERTY_KEY                = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "glossary");
    public static final String CATEGORIES_PROPERTY_KEY              = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "categories");
    public static final String CATEGORIES_PARENT_PROPERTY_KEY       = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "parentCategory");

    public static final String MEANINGS_TEXT_PROPERTY_KEY           = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "meaningsText");
    public static final String MEANING_NAMES_PROPERTY_KEY           = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "meaningNames");
    public static final String HAS_LINEAGE                          = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "hasLineage");
    public static final String HAS_LINEAGE_VALID                    = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "hasLineageValid");
    public static final String LEXICOGRAPHICAL_SORT_ORDER           = "lexicographicalSortOrder";

    //Classification-Only System Attributes
    public static final String CLASSIFICATION_ENTITY_STATUS_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "entityStatus");


    /*
     * elasticsearch mappings
     */
    public static final HashMap<String, Object> ES_DATE_FIELD = new HashMap<>();
    static {
        ES_DATE_FIELD.put("type", "date");
        ES_DATE_FIELD.put("format", "epoch_millis");
    }
    public static final HashMap<String, HashMap<String, Object>> DATE_MULTIFIELD = new HashMap<>();
    static {
        DATE_MULTIFIELD.put("date", ES_DATE_FIELD);
    }

    public static final HashMap<String, Object> ES_TEXT_FIELD = new HashMap<>();
    static {
        ES_TEXT_FIELD.put("type", "text");
        ES_TEXT_FIELD.put("analyzer", "atlan_text_analyzer");
    }
    public static final HashMap<String, HashMap<String, Object>> TEXT_MULTIFIELD = new HashMap<>();
    static {
        TEXT_MULTIFIELD.put("text", ES_TEXT_FIELD);
    }

    public static final HashMap<String, Object> ES_ATLAN_KEYWORD_ANALYZER_CONFIG = new HashMap<>();
    static {
        ES_ATLAN_KEYWORD_ANALYZER_CONFIG.put("normalizer", "atlan_normalizer");
    }

    public static final String INDEX_NAME_VERTEX_INDEX = "vertex_index";
    public static final String INDEX_NAME_FULLTEXT_INDEX = "fulltext_index";
    public static final String INDEX_NAME_EDGE_INDEX = "edge_index";

    private Constants() {}
}