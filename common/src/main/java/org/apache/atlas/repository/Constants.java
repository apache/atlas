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

package org.apache.atlas.repository;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;

import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.encodePropertyKey;

/**
 * Repository Constants.
 *
 */
public final class Constants {

    /**
     * Globally Unique identifier property key.
     */

    public static final String INTERNAL_PROPERTY_KEY_PREFIX     = "__";
    public static final String RELATIONSHIP_PROPERTY_KEY_PREFIX = "_r";
    public static final String GUID_PROPERTY_KEY                = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "guid");
    public static final String RELATIONSHIP_GUID_PROPERTY_KEY   = encodePropertyKey(RELATIONSHIP_PROPERTY_KEY_PREFIX + GUID_PROPERTY_KEY);

    /**
     * Entity type name property key.
     */
    public static final String ENTITY_TYPE_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "typeName");
    public static final String TYPE_NAME_INTERNAL       = INTERNAL_PROPERTY_KEY_PREFIX + "internal";

    /**
     * Entity type's super types property key.
     */
    public static final String SUPER_TYPES_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "superTypeNames");

    /**
     * Full-text for the entity for enabling full-text search.
     */
    public static final String ENTITY_TEXT_PROPERTY_KEY = encodePropertyKey("entityText");

    /**
     * Properties for type store graph.
     */
    public static final String TYPE_CATEGORY_PROPERTY_KEY   = getEncodedTypePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "type.category");
    public static final String VERTEX_TYPE_PROPERTY_KEY     = getEncodedTypePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "type");
    public static final String TYPENAME_PROPERTY_KEY        = getEncodedTypePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "type.name");
    public static final String TYPEDESCRIPTION_PROPERTY_KEY = getEncodedTypePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "type.description");
    public static final String TYPEVERSION_PROPERTY_KEY     = getEncodedTypePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "type.version");
    public static final String TYPEOPTIONS_PROPERTY_KEY     = getEncodedTypePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "type.options");
    public static final String TYPESERVICETYPE_PROPERTY_KEY = getEncodedTypePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "type.servicetype");

    // relationship def constants
    public static final String RELATIONSHIPTYPE_END1_KEY                               = "endDef1";
    public static final String RELATIONSHIPTYPE_END2_KEY                               = "endDef2";
    public static final String RELATIONSHIPTYPE_CATEGORY_KEY                           = "relationshipCategory";
    public static final String RELATIONSHIPTYPE_LABEL_KEY                              = "relationshipLabel";
    public static final String RELATIONSHIPTYPE_TAG_PROPAGATION_KEY                    = encodePropertyKey("tagPropagation");
    public static final String RELATIONSHIPTYPE_BLOCKED_PROPAGATED_CLASSIFICATIONS_KEY = encodePropertyKey("blockedPropagatedClassifications");

    /**
     * Trait names property key and index name.
     */
    public static final String TRAIT_NAMES_PROPERTY_KEY            = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "traitNames");
    public static final String PROPAGATED_TRAIT_NAMES_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "propagatedTraitNames");

    public static final String VERSION_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "version");
    public static final String STATE_PROPERTY_KEY   = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "state");
    public static final String CREATED_BY_KEY       = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "createdBy");
    public static final String MODIFIED_BY_KEY      = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "modifiedBy");

    /**
     * The homeId field is used when saving into Atlas a copy of an object that is being imported from another
     * repository. The homeId will be set to a String that identifies the other repository. The specific format
     * of repository identifiers is domain dependent. Where it is set by Open Metadata Repository Services it will
     * be a MetadataCollectionId.
     * An object that is mastered by the Atlas repository, will have a null homeId field. This represents a locally
     * mastered object that can be manipulated by Atlas and its applications as normal.
     * An object with a non-null homeId is a copy of an object mastered by a different repository and the object
     * should only be updated via the notifications and calls from Open Metadata Repository Services.
     */
    public static final String HOME_ID_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "homeId");

    /**
     * The isProxy field is used when saving into Atlas a proxy of an entity - i.e. it is not a whole entity, but
     * a partial representation of an entity that is referred to by a relationship end.
     * The isProxy field will be set to true if the entity is a proxy. The field is used during retrieval of an
     * entity (proxy) from Atlas to indicate that the entity does not contain full entity detail.
     */
    public static final String IS_PROXY_KEY           = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "isProxy");

    /**
     * The provenanceType field is used to record the provenance of an instance of an entity or relationship - this
     * indicates how the instance was created. This corresponds to the InstanceProvenanceType enum defined in ODPi.
     * To avoid creating a hard dependency on the ODPi class, the value is stored as an int corresponding to the
     * ordinal in the ODPi enum.
     */
    public static final String PROVENANCE_TYPE_KEY    = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "provenanceType");

    public static final String TIMESTAMP_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "timestamp");

    public static final String MODIFICATION_TIMESTAMP_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "modificationTimestamp");

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

    public static final String QUALIFIED_NAME                          = "Referenceable.qualifiedName";
    public static final String TYPE_NAME_PROPERTY_KEY                  = INTERNAL_PROPERTY_KEY_PREFIX + "typeName";
    public static final String INDEX_SEARCH_MAX_RESULT_SET_SIZE        = "atlas.graph.index.search.max-result-set-size";
    public static final String INDEX_SEARCH_TYPES_MAX_QUERY_STR_LENGTH = "atlas.graph.index.search.types.max-query-str-length";
    public static final String INDEX_SEARCH_TAGS_MAX_QUERY_STR_LENGTH  = "atlas.graph.index.search.tags.max-query-str-length";
    public static final String INDEX_SEARCH_VERTEX_PREFIX_PROPERTY     = "atlas.graph.index.search.vertex.prefix";
    public static final String INDEX_SEARCH_VERTEX_PREFIX_DEFAULT      = "$v$";

    public static final String MAX_FULLTEXT_QUERY_STR_LENGTH = "atlas.graph.fulltext-max-query-str-length";
    public static final String MAX_DSL_QUERY_STR_LENGTH      = "atlas.graph.dsl-max-query-str-length";

    public static final String ATTRIBUTE_NAME_GUID           = "guid";
    public static final String ATTRIBUTE_NAME_TYPENAME       = "typeName";
    public static final String ATTRIBUTE_NAME_SUPERTYPENAMES = "superTypeNames";
    public static final String ATTRIBUTE_NAME_STATE          = "state";
    public static final String ATTRIBUTE_NAME_VERSION        = "version";
    public static final String TEMP_STRUCT_NAME_PREFIX       = "__tempQueryResultStruct";

    public static final String CLASSIFICATION_ENTITY_GUID                     = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "entityGuid");
    public static final String CLASSIFICATION_ENTITY_STATUS                   = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "entityStatus");
    public static final String CLASSIFICATION_VALIDITY_PERIODS_KEY            = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "validityPeriods");
    public static final String CLASSIFICATION_VERTEX_PROPAGATE_KEY            = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "propagate");
    public static final String CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY  = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "removePropagations");
    public static final String CLASSIFICATION_VERTEX_NAME_KEY                 = encodePropertyKey(TYPE_NAME_PROPERTY_KEY);
    public static final String CLASSIFICATION_EDGE_NAME_PROPERTY_KEY          = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "name");
    public static final String CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "isPropagated");
    public static final String CLASSIFICATION_EDGE_STATE_PROPERTY_KEY         = STATE_PROPERTY_KEY;
    public static final String CLASSIFICATION_LABEL                           = "classifiedAs";
    public static final String TERM_ASSIGNMENT_LABEL                          = "r:AtlasGlossarySemanticAssignment";
    public static final String ATTRIBUTE_INDEX_PROPERTY_KEY                   = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "index");
    public static final String ATTRIBUTE_KEY_PROPERTY_KEY                     = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "key");

    public static final String VERTEX_ID_IN_IMPORT_KEY = "__vIdInImport";
    public static final String EDGE_ID_IN_IMPORT_KEY   = "__eIdInImport";

    /*
     * replication attributes
     */

    public static final String ATTR_NAME_REFERENCEABLE = "Referenceable.";
    public static final String ATTR_NAME_REPLICATED_TO = "replicatedTo";
    public static final String ATTR_NAME_REPLICATED_FROM = "replicatedFrom";

    private Constants() {
    }

    private static String getEncodedTypePropertyKey(String defaultKey) {
        try {
            Configuration configuration = ApplicationProperties.get();

            if (configuration.containsKey("atlas.graph.index.search.backend") &&
                configuration.getString("atlas.graph.index.search.backend").equals("elasticsearch")) {

                return defaultKey.replaceAll("\\.", "_");
            }

            return encodePropertyKey(defaultKey);
        } catch (AtlasException e) {
            return encodePropertyKey(defaultKey);
        }
    }
}
