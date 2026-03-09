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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.encodePropertyKey;
import static org.apache.atlas.type.AtlasStructType.UNIQUE_ATTRIBUTE_SHADE_PROPERTY_PREFIX;

/**
 * Repository Constants.
 *
 */
public final class Constants {
    private static final Logger LOG = LoggerFactory.getLogger(Constants.class);

    /**
     * Globally Unique identifier property key.
     */

    public static final String INTERNAL_PROPERTY_KEY_PREFIX     = "__";
    public static final String RELATIONSHIP_PROPERTY_KEY_PREFIX = "_r";
    public static final String GUID_PROPERTY_KEY                = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "guid");
    public static final String RELATIONSHIP_GUID_PROPERTY_KEY   = encodePropertyKey(RELATIONSHIP_PROPERTY_KEY_PREFIX + GUID_PROPERTY_KEY);
    public static final String HISTORICAL_GUID_PROPERTY_KEY     = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "historicalGuids");
    public static final String QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "qualifiedNameHierarchy");
    public static final String FREETEXT_REQUEST_HANDLER         = "/freetext";
    public static final String TERMS_REQUEST_HANDLER            = "/terms";
    public static final String ES_API_ALIASES                   = "/_aliases";

    /**
     * Entity type name property key.
     */
    public static final String ENTITY_TYPE_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "typeName");
    public static final String TYPE_NAME_INTERNAL       = INTERNAL_PROPERTY_KEY_PREFIX + "internal";
    public static final String ASSET_ENTITY_TYPE = "Asset";
    public static final String OWNER_ATTRIBUTE   = "owner";

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
    public static final String  TYPENAME_PROPERTY_KEY        = getEncodedTypePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "type.name");
    public static final String TYPE_DISPLAYNAME_PROPERTY_KEY= getEncodedTypePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "type.displayName");
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
    public static final String TRAIT_NAMES_PROPERTY_KEY             = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "traitNames");
    public static final String PROPAGATED_TRAIT_NAMES_PROPERTY_KEY  = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "propagatedTraitNames");

    public static final String VERSION_PROPERTY_KEY                 = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "version");
    public static final String STATE_PROPERTY_KEY                   = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "state");
    public static final String CREATED_BY_KEY                       = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "createdBy");
    public static final String MODIFIED_BY_KEY                      = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "modifiedBy");
    public static final String CLASSIFICATION_TEXT_KEY              = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "classificationsText");
    public static final String CLASSIFICATION_NAMES_KEY             = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "classificationNames");
    public static final String PROPAGATED_CLASSIFICATION_NAMES_KEY  = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "propagatedClassificationNames");
    public static final String CUSTOM_ATTRIBUTES_PROPERTY_KEY       = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "customAttributes");
    public static final String LABELS_PROPERTY_KEY                  = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "labels");
    public static final String EDGE_PENDING_TASKS_PROPERTY_KEY      = encodePropertyKey(RELATIONSHIP_PROPERTY_KEY_PREFIX + "__pendingTasks");

    /**
     * Patch vertices property keys.
     */
    public static final String PATCH_ID_PROPERTY_KEY          = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "patch.id");
    public static final String PATCH_DESCRIPTION_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "patch.description");
    public static final String PATCH_TYPE_PROPERTY_KEY        = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "patch.type");
    public static final String PATCH_ACTION_PROPERTY_KEY      = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "patch.action");
    public static final String PATCH_STATE_PROPERTY_KEY       = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "patch.state");

    /**
     * Glossary property keys.
     */
    public static final String ATLAS_GLOSSARY_ENTITY_TYPE          = "AtlasGlossary";
    public static final String ATLAS_GLOSSARY_TERM_ENTITY_TYPE     = "AtlasGlossaryTerm";
    public static final String ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE = "AtlasGlossaryCategory";
    public static final String CATEGORY_PARENT_EDGE_LABEL          = "r:AtlasGlossaryCategoryHierarchyLink";
    public static final String CATEGORY_TERMS_EDGE_LABEL           = "r:AtlasGlossaryTermCategorization";
    public static final String GLOSSARY_TERMS_EDGE_LABEL           = "r:AtlasGlossaryTermAnchor";
    public static final String GLOSSARY_CATEGORY_EDGE_LABEL        = "r:AtlasGlossaryCategoryAnchor";

    /**
     * MESH property keys.
     */
    public static final String DATA_DOMAIN_ENTITY_TYPE     = "DataDomain";
    public static final String DATA_PRODUCT_ENTITY_TYPE    = "DataProduct";

    public static final String AI_APPLICATION       = "AIApplication";
    public static final String AI_MODEL             = "AIModel";
    
    public static final String STAKEHOLDER_ENTITY_TYPE       = "Stakeholder";
    public static final String STAKEHOLDER_TITLE_ENTITY_TYPE = "StakeholderTitle";

    public static final String REL_DOMAIN_TO_DOMAINS  = "parent_domain_sub_domains";
    public static final String REL_DOMAIN_TO_PRODUCTS = "data_domain_data_products";

    public static final String REL_DOMAIN_TO_STAKEHOLDERS            = "data_domain_stakeholders";
    public static final String REL_STAKEHOLDER_TITLE_TO_STAKEHOLDERS = "stakeholder_title_stakeholders";

    public static final String REL_DATA_PRODUCT_TO_OUTPUT_PORTS = "data_products_output_ports";
    public static final String REL_DATA_PRODUCT_TO_INPUT_PORTS  = "data_products_input_ports";

    public static final String INPUT_PORT_PRODUCT_EDGE_LABEL = "__Asset.inputPortDataProducts";
    public static final String OUTPUT_PORT_PRODUCT_EDGE_LABEL = "__Asset.outputPortDataProducts";

    public static final String OUTPUT_PORTS = "outputPorts";
    public static final String INPUT_PORTS = "inputPorts";
    public static final String ADDED_OUTPUT_PORTS = "addedOutputPorts";
    public static final String REMOVED_OUTPUT_PORTS = "removedOutputPorts";

    public static final String UD_RELATIONSHIP_EDGE_LABEL = "__Referenceable.userDefRelationshipTo";
    public static final String UD_RELATIONSHIP_END_NAME_FROM = "userDefRelationshipFrom";
    public static final String UD_RELATIONSHIP_END_NAME_TO = "userDefRelationshipTo";

    /**
     * SQL property keys.
     */

    public static final String SQL_ENTITY_TYPE              = "SQL";
    public static final String CONNECTION_ENTITY_TYPE       = "Connection";
    public static final String QUERY_ENTITY_TYPE            = "Query";
    public static final String QUERY_FOLDER_ENTITY_TYPE     = "Folder";
    public static final String QUERY_COLLECTION_ENTITY_TYPE = "Collection";

    /*
     * Purpose / Persona
     */
    public static final String ACCESS_CONTROL_ENTITY_TYPE = "AccessControl";
    public static final String PERSONA_ENTITY_TYPE        = "Persona";
    public static final String PURPOSE_ENTITY_TYPE        = "Purpose";
    public static final String POLICY_ENTITY_TYPE         = "AuthPolicy";
    public static final String SERVICE_ENTITY_TYPE        = "AuthService";
    public static final String REL_POLICY_TO_ACCESS_CONTROL  = "access_control_policies";

    /**
     * Resource
     */
    public static final String LINK_ENTITY_TYPE = "Link";
    public static final String README_ENTITY_TYPE = "Readme";

    public static final String ASSET_RELATION_ATTR = "asset";

    public static final String ASSET_README_EDGE_LABEL = "__Asset.readme";
    public static final String ASSET_LINK_EDGE_LABEL = "__Asset.links";

    public static final String DATA_SET_SUPER_TYPE       = "Catalog";
    public static final String PROCESS_SUPER_TYPE        = "Process";
    public static final String ERROR      = "error";
    public static final String STATUS     = "Status";

    /**
     * Contract
     */
    public static final String CONTRACT_ENTITY_TYPE = "DataContract";
    public static final String ATTR_CONTRACT_VERSION = "dataContractVersion";


    /**
     * Lineage relations.
     */
    public static final String PROCESS_OUTPUTS = "__Process.outputs";
    public static final String PROCESS_INPUTS = "__Process.inputs";
    public static final String CSA_ADOPTION_EXPORT = "csa-adoption-export";

    public static String[] PROCESS_EDGE_LABELS = {PROCESS_OUTPUTS, PROCESS_INPUTS};

    public static final String PROCESS_ENTITY_TYPE = "Process";

    public static final String CONNECTION_PROCESS_ENTITY_TYPE = "ConnectionProcess";
    public static final String PARENT_CONNECTION_PROCESS_QUALIFIED_NAME = "parentConnectionProcessQualifiedName";

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

    public static final String ENTITY_DELETED_TIMESTAMP_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "entityDeletedTimestamp");

    public static final String MODIFICATION_TIMESTAMP_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "modificationTimestamp");

    public static final String IS_INCOMPLETE_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "isIncomplete");

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

    /**
     * elasticsearch index prefix.
     */
    public static final String INDEX_PREFIX = "janusgraph_";

    public static final String VERTEX_INDEX_NAME = INDEX_PREFIX + VERTEX_INDEX;
    public static final String EDGE_INDEX_NAME = INDEX_PREFIX + EDGE_INDEX;

    public static final String NAME                                    = "name";
    public static final String QUALIFIED_NAME                          = "qualifiedName";
    public static final String CONNECTION_QUALIFIED_NAME               = "connectionQualifiedName";
    public static final String UNIQUE_QUALIFIED_NAME                   = UNIQUE_ATTRIBUTE_SHADE_PROPERTY_PREFIX + QUALIFIED_NAME;
    public static final String TYPE_NAME_PROPERTY_KEY                  = INTERNAL_PROPERTY_KEY_PREFIX + "typeName";

    public static final String LABEL_PROPERTY_KEY                      = "label";
    public static final String INDEX_SEARCH_MAX_RESULT_SET_SIZE        = "atlas.graph.index.search.max-result-set-size";
    public static final String INDEX_SEARCH_TYPES_MAX_QUERY_STR_LENGTH = "atlas.graph.index.search.types.max-query-str-length";
    public static final String INDEX_SEARCH_TAGS_MAX_QUERY_STR_LENGTH  = "atlas.graph.index.search.tags.max-query-str-length";
    public static final String INDEX_SEARCH_VERTEX_PREFIX_PROPERTY     = "atlas.graph.index.search.vertex.prefix";
    public static final String INDEX_SEARCH_VERTEX_PREFIX_DEFAULT      = "$v$";
    public static final String DOMAIN_GUIDS                            = "domainGUIDs";
    public static final String PRODUCT_GUIDS                           = "productGUIDs";

    public static final String ATTR_TENANT_ID = "tenantId";
    public static final String DEFAULT_TENANT_ID = "default";

    public static final String MAX_FULLTEXT_QUERY_STR_LENGTH = "atlas.graph.fulltext-max-query-str-length";
    public static final String MAX_DSL_QUERY_STR_LENGTH      = "atlas.graph.dsl-max-query-str-length";

    public static final String ATTRIBUTE_NAME_GUID           = "guid";
    public static final String ATTRIBUTE_NAME_TYPENAME       = "typeName";
    public static final String ATTRIBUTE_NAME_SUPERTYPENAMES = "superTypeNames";
    public static final String ATTRIBUTE_NAME_STATE          = "state";
    public static final String ATTRIBUTE_NAME_VERSION        = "version";
    public static final String ATTRIBUTE_LINK                = "link";
    public static final String TEMP_STRUCT_NAME_PREFIX       = "__tempQueryResultStruct";

    public static final String CLASSIFICATION_ENTITY_GUID                     = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "entityGuid");
    public static final String CLASSIFICATION_ENTITY_STATUS                   = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "entityStatus");
    public static final String CLASSIFICATION_VALIDITY_PERIODS_KEY            = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "validityPeriods");
    public static final String CLASSIFICATION_VERTEX_PROPAGATE_KEY            = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "propagate");
    public static final String CLASSIFICATION_VERTEX_REMOVE_PROPAGATIONS_KEY  = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "removePropagations");
    public static final String CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_LINEAGE= encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "restrictPropagationThroughLineage");

    public static final String CLASSIFICATION_VERTEX_RESTRICT_PROPAGATE_THROUGH_HIERARCHY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "restrictPropagationThroughHierarchy");
    public static final String CLASSIFICATION_VERTEX_NAME_KEY                 = encodePropertyKey(TYPE_NAME_PROPERTY_KEY);
    public static final String CLASSIFICATION_EDGE_NAME_PROPERTY_KEY          = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "name");
    public static final String CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "isPropagated");
    public static final String CLASSIFICATION_EDGE_STATE_PROPERTY_KEY         = STATE_PROPERTY_KEY;
    public static final String CLASSIFICATION_LABEL                           = "classifiedAs";
    public static final String CLASSIFICATION_NAME_DELIMITER                  = "|";
    public static final String LABEL_NAME_DELIMITER                           = CLASSIFICATION_NAME_DELIMITER;
    public static final String TERM_ASSIGNMENT_LABEL                          = "r:AtlasGlossarySemanticAssignment";
    public static final String ATTRIBUTE_INDEX_PROPERTY_KEY                   = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "index");
    public static final String ATTRIBUTE_KEY_PROPERTY_KEY                     = encodePropertyKey(INTERNAL_PROPERTY_KEY_PREFIX + "key");
    public static final String ATTRIBUTE_VALUE_DELIMITER                      = ",";

    public static final String VERTEX_ID_IN_IMPORT_KEY = "__vIdInImport";
    public static final String EDGE_ID_IN_IMPORT_KEY   = "__eIdInImport";

    /*
     * Edge labels for data product relations which are hard deleted
     */

    public static final Set<String> EDGE_LABELS_FOR_HARD_DELETION = new HashSet<>(Arrays.asList( OUTPUT_PORT_PRODUCT_EDGE_LABEL, INPUT_PORT_PRODUCT_EDGE_LABEL, TERM_ASSIGNMENT_LABEL ));
    /*
     * elasticsearch attributes
     */

    public static final String  ELASTICSEARCH_CLUSTER_STATUS_YELLOW   = "YELLOW";
    public static final String  ELASTICSEARCH_CLUSTER_STATUS_GREEN   = "GREEN";
    public static final String  ELASTICSEARCH_REST_STATUS_OK   = "OK";

    /*
     * replication attributes
     */

    public static final String  ATTR_NAME_REFERENCEABLE   = "Referenceable.";
    public static final String  ATTR_NAME_REPLICATED_TO   = "replicatedTo";
    public static final String  ATTR_NAME_REPLICATED_FROM = "replicatedFrom";
    public static final Integer INCOMPLETE_ENTITY_VALUE   = Integer.valueOf(1);

    /*
     * typedef patch constants
     */
    public static final String  TYPEDEF_PATCH_ADD_MANDATORY_ATTRIBUTE   = "ADD_MANDATORY_ATTRIBUTE";

    /*
     * Task related constants
     */
    public static final String TASK_PREFIX            = INTERNAL_PROPERTY_KEY_PREFIX + "task_";
    public static final String TASK_TYPE_PROPERTY_KEY = encodePropertyKey(TASK_PREFIX + "v_type");
    public static final String TASK_TYPE_NAME         = INTERNAL_PROPERTY_KEY_PREFIX + "AtlasTaskDef";
    public static final String TASK_GUID              = encodePropertyKey(TASK_PREFIX + "guid");
    public static final String TASK_TYPE              = encodePropertyKey(TASK_PREFIX + "type");
    public static final String TASK_CREATED_TIME      = encodePropertyKey(TASK_PREFIX + "timestamp");
    public static final String TASK_UPDATED_TIME      = encodePropertyKey(TASK_PREFIX + "modificationTimestamp");
    public static final String TASK_CREATED_BY        = encodePropertyKey(TASK_PREFIX + "createdBy");
    public static final String TASK_STATUS            = encodePropertyKey(TASK_PREFIX + "status");
    public static final String TASK_ATTEMPT_COUNT     = encodePropertyKey(TASK_PREFIX + "attemptCount");
    public static final String TASK_PARAMETERS        = encodePropertyKey(TASK_PREFIX + "parameters");
    public static final String TASK_ERROR_MESSAGE     = encodePropertyKey(TASK_PREFIX + "errorMessage");
    public static final String TASK_WARNING_MESSAGE     = encodePropertyKey(TASK_PREFIX + "warning");
    public static final String TASK_START_TIME        = encodePropertyKey(TASK_PREFIX + "startTime");
    public static final String TASK_END_TIME          = encodePropertyKey(TASK_PREFIX + "endTime");
    public static final String TASK_TIME_TAKEN_IN_SECONDS   = encodePropertyKey(TASK_PREFIX + "timeTakenInSeconds");
    public static final String TASK_CLASSIFICATION_ID       = encodePropertyKey(TASK_PREFIX + "classificationId");
    public static final String TASK_ENTITY_GUID             = encodePropertyKey(TASK_PREFIX + "entityGuid");
    public static final String TASK_PARENT_ENTITY_GUID             = encodePropertyKey(TASK_PREFIX + "parentEntityGuid");
    public static final String TASK_CLASSIFICATION_TYPENAME = encodePropertyKey(TASK_PREFIX + "classificationTypeName");
    public static final String ACTIVE_STATE_VALUE           = "ACTIVE";

    public static final String ATLAN_HEADER_PREFIX_PATTERN  = "x-atlan-";
    public static final String TASK_HEADER_ATLAN_AGENT      = "x-atlan-agent";
    public static final String TASK_HEADER_ATLAN_AGENT_ID   = "x-atlan-agent-id";
    public static final String TASK_HEADER_ATLAN_PKG_NAME   = "x-atlan-package-name";
    public static final String TASK_HEADER_ATLAN_AGENT_WORKFLOW_ID = "x-atlan-agent-workflow-id";
    public static final String TASK_HEADER_ATLAN_VIA_UI            = "x-atlan-via-ui";
    public static final String TASK_HEADER_ATLAN_REQUEST_ID        = "x-atlan-request-id";
    public static final String TASK_HEADER_ATLAN_GOOGLE_SHEETS_ID  = "x-atlan-google-sheets-id";
    public static final String TASK_HEADER_ATLAN_MS_EXCEL_ID       = "x-atlan-microsoft-excel-id";
    public static final Set<String> TASK_HEADER_SET = new HashSet<String>() {{
        add(TASK_HEADER_ATLAN_AGENT);
        add(TASK_HEADER_ATLAN_AGENT_ID);
        add(TASK_HEADER_ATLAN_VIA_UI);
        add(TASK_HEADER_ATLAN_PKG_NAME);
        add(TASK_HEADER_ATLAN_AGENT_WORKFLOW_ID);
        add(TASK_HEADER_ATLAN_REQUEST_ID);
        add(TASK_HEADER_ATLAN_GOOGLE_SHEETS_ID);
        add(TASK_HEADER_ATLAN_MS_EXCEL_ID);
    }};
    
    /**
     * Index Recovery vertex property keys.
     */
    public static final String INDEX_RECOVERY_PREFIX                  = INTERNAL_PROPERTY_KEY_PREFIX + "idxRecovery_";
    public static final String PROPERTY_KEY_INDEX_RECOVERY_NAME       = encodePropertyKey(INDEX_RECOVERY_PREFIX + "name");
    public static final String PROPERTY_KEY_INDEX_RECOVERY_START_TIME = encodePropertyKey(INDEX_RECOVERY_PREFIX + "startTime");
    public static final String PROPERTY_KEY_INDEX_RECOVERY_PREV_TIME  = encodePropertyKey(INDEX_RECOVERY_PREFIX + "prevTime");

    public static final String SQOOP_SOURCE       = "sqoop";
    public static final String FALCON_SOURCE      = "falcon";
    public static final String HBASE_SOURCE       = "hbase";
    public static final String HS2_SOURCE         = "hive_server2";
    public static final String HMS_SOURCE         = "hive_metastore";
    public static final String IMPALA_SOURCE      = "impala";
    public static final String STORM_SOURCE       = "storm";
    public static final String FILE_SPOOL_SOURCE  = "file_spool";
    public static final String DOMAIN_GUIDS_ATTR = "domainGUIDs";
    public static final String ASSET_POLICY_GUIDS  = "assetPolicyGUIDs";
    public static final String PRODUCT_GUIDS_ATTR  = "productGUIDs";

    public static final String NON_COMPLIANT_ASSET_POLICY_GUIDS  = "nonCompliantAssetPolicyGUIDs";
    public static final String ASSET_POLICIES_COUNT  = "assetPoliciesCount";

    /*
     * All supported file-format extensions for Bulk Imports through file upload
     */
    public enum SupportedFileExtensions { XLSX, XLS, CSV }

    /*
     * All statsd metric names
     */
    public static final String ENTITIES_ADDED_METRIC = "entities.added";
    public static final String ENTITIES_UPDATED_METRIC = "entities.updated";
    public static final String ENTITIES_DELETED_METRIC = "entities.deleted";
    public static final String ENTITIES_PURGED_METRIC = "entities.purged";
    public static final String CLASSIFICATIONS_ADDED_METRIC = "classifications.added";
    public static final String CLASSIFICATIONS_UPDATED_METRIC = "classifications.updated";
    public static final String CLASSIFICATIONS_DELETED_METRIC = "classifications.deleted";
    public static final String TERMS_ADDED_METRIC = "terms.added";
    public static final String TERMS_DELETED_METRIC = "terms.deleted";
    public static final String RELATIONSHIPS_ADDED_METRIC = "relationships.added";
    public static final String RELATIONSHIPS_UPDATED_METRIC = "relationships.updated";
    public static final String RELATIONSHIPS_DELETED_METRIC = "relationships.deleted";
    public static final String RELATIONSHIPS_PURGED_METRIC = "relationships.purged";
    public static final String LABELS_ADDED_METRIC = "labels.added";
    public static final String LABELS_DELETED_METRIC = "labels.deleted";
    public static final String BA_UPDATED_METRIC = "ba.deleted";

    public static final String BASIC_SEARCH_COUNT_METRIC = "search.basic.count";
    public static final String BASIC_SEARCH_EXECUTION_TIME_METRIC = "search.basic.execution.time";
    public static final String CLASSIFICATION_PROPAGATION_TIME_METRIC = "classification.propagation.time";
    public static final String CLASSIFICATION_PROPAGATION_JOB_COUNT_METRIC = "classification.propagation.job.count";

    public static final int ELASTICSEARCH_PAGINATION_SIZE = 50;

    public static final String CATALOG_PROCESS_INPUT_RELATIONSHIP_LABEL = "__Process.inputs";
    public static final String CATALOG_PROCESS_OUTPUT_RELATIONSHIP_LABEL = "__Process.outputs";
    public static final String CATALOG_AIRFLOW_INPUT_RELATIONSHIP_LABEL = "__AirflowTask.inputs";
    public static final String CATALOG_AIRFLOW_OUTPUT_RELATIONSHIP_LABEL = "__AirflowTask.outputs";
    public static final String CATALOG_CONNECTION_PROCESS_INPUT_RELATIONSHIP_LABEL = "__ConnectionProcess.inputs";
    public static final String CATALOG_CONNECTION_PROCESS_OUTPUT_RELATIONSHIP_LABEL = "__ConnectionProcess.outputs";
    public static final String CATALOG_SPARK_JOB_INPUT_RELATIONSHIP_LABEL = "__SparkJob.inputs";
    public static final String CATALOG_SPARK_JOB_OUTPUT_RELATIONSHIP_LABEL = "__SparkJob.outputs";
    public static final String CLASSIFICATION_PROPAGATION_MODE_DEFAULT  ="DEFAULT";
    public static final String CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE  ="RESTRICT_LINEAGE";

    public static final String CLASSIFICATION_PROPAGATION_MODE_RESTRICT_HIERARCHY  ="RESTRICT_HIERARCHY";


    public static final HashMap<String, ArrayList<String>> CLASSIFICATION_PROPAGATION_MODE_LABELS_MAP = new HashMap<String, ArrayList<String>>(){{
        put(CLASSIFICATION_PROPAGATION_MODE_RESTRICT_LINEAGE, new ArrayList<>(
                Arrays.asList(CATALOG_PROCESS_INPUT_RELATIONSHIP_LABEL,
                        CATALOG_PROCESS_OUTPUT_RELATIONSHIP_LABEL,
                CATALOG_AIRFLOW_INPUT_RELATIONSHIP_LABEL,
                CATALOG_AIRFLOW_OUTPUT_RELATIONSHIP_LABEL,
                CATALOG_CONNECTION_PROCESS_INPUT_RELATIONSHIP_LABEL,
                CATALOG_CONNECTION_PROCESS_OUTPUT_RELATIONSHIP_LABEL,
                CATALOG_SPARK_JOB_INPUT_RELATIONSHIP_LABEL,
                CATALOG_SPARK_JOB_OUTPUT_RELATIONSHIP_LABEL
        )));
        put(CLASSIFICATION_PROPAGATION_MODE_DEFAULT, null);
        put(CLASSIFICATION_PROPAGATION_MODE_RESTRICT_HIERARCHY, new ArrayList<>(
                Arrays.asList(CATALOG_PROCESS_INPUT_RELATIONSHIP_LABEL,
                        CATALOG_PROCESS_OUTPUT_RELATIONSHIP_LABEL
                )));
    }};

    public static final String ATTR_ADMIN_USERS = "adminUsers";
    public static final String ATTR_ADMIN_GROUPS = "adminGroups";
    public static final String ATTR_ADMIN_ROLES = "adminRoles";
    public static final String ATTR_VIEWER_USERS = "viewerUsers";
    public static final String ATTR_VIEWER_GROUPS = "viewerGroups";
    public static final String ATTR_OWNER_USERS = "ownerUsers";
    public static final String ATTR_OWNER_GROUPS = "ownerGroups";
    public static final String ATTR_ANNOUNCEMENT_MESSAGE = "announcementMessage";

    public static final String ATTR_STARRED_BY = "starredBy";
    public static final String ATTR_STARRED_COUNT = "starredCount";
    public static final String ATTR_STARRED_DETAILS_LIST = "starredDetailsList";
    public static final String ATTR_ASSET_STARRED_BY = "assetStarredBy";
    public static final String ATTR_ASSET_STARRED_AT = "assetStarredAt";
    public static final String ATTR_CERTIFICATE_STATUS = "certificateStatus";
    public static final String ATTR_CONTRACT = "dataContractSpec";
    public static final String ATTR_CONTRACT_JSON = "dataContractJson";
    public static final String STRUCT_STARRED_DETAILS = "StarredDetails";

    public static final String KEYCLOAK_ROLE_ADMIN   = "$admin";
    public static final String KEYCLOAK_ROLE_MEMBER  = "$member";
    public static final String KEYCLOAK_ROLE_GUEST   = "$guest";
    public static final String KEYCLOAK_ROLE_DEFAULT = "default-roles-default";
    public static final String KEYCLOAK_ROLE_API_TOKEN = "$api-token-default-access";

    public static final String REQUEST_HEADER_USER_AGENT = "User-Agent";
    public static final String REQUEST_HEADER_HOST       = "Host";

    public static final String ACTION_READ = "entity-read";

    public static final Set<String> SKIP_UPDATE_AUTH_CHECK_TYPES = new HashSet<String>() {{
        add(README_ENTITY_TYPE);
        add(LINK_ENTITY_TYPE);
        add(STAKEHOLDER_ENTITY_TYPE);
        add(STAKEHOLDER_TITLE_ENTITY_TYPE);
    }};

    public static final Set<String> SKIP_DELETE_AUTH_CHECK_TYPES = new HashSet<String>() {{
        add(README_ENTITY_TYPE);
        add(LINK_ENTITY_TYPE);
        add(POLICY_ENTITY_TYPE);
        add(STAKEHOLDER_TITLE_ENTITY_TYPE);
    }};

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


    public static String getStaticFileAsString(String fileName) throws IOException {
        String atlasHomeDir  = System.getProperty("atlas.home");
        atlasHomeDir = StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir;
        
        Path path = Paths.get(atlasHomeDir, "static", fileName);
        String resourceAsString = null;
        try {
            resourceAsString = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.error("Failed to get static file as string: {}", fileName);
            throw e;
        }

        return resourceAsString;
    }
}
