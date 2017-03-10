/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.text.MessageFormat;
import java.util.Arrays;

public enum AtlasErrorCode {
    NO_SEARCH_RESULTS(204, "ATLAS-204-00-001", "Given search filter {0} did not yield any results"),

    UNKNOWN_TYPE(400, "ATLAS-400-00-001", "Unknown type {0} for {1}.{2}"),
    CIRCULAR_REFERENCE(400, "ATLAS-400-00-002", "{0}: invalid supertypes - circular reference back to self {1}"),
    INCOMPATIBLE_SUPERTYPE(400, "ATLAS-400-00-003", "{0}: incompatible supertype {1}"),
    UNKNOWN_CONSTRAINT(400, "ATLAS-400-00-004", "{0}.{1}: unknown constraint {1}"),
    UNSUPPORTED_CONSTRAINT(400, "ATLAS-400-00-005", "{0}.{1} : {2} constraint not supported"),
    CONSTRAINT_NOT_SATISFIED(400, "ATLAS-400-00-006", "{0}.{1} : {2} incompatible attribute type {3}"),
    CONSTRAINT_MISSING_PARAMS(400, "ATLAS-400-00-007", "{0}.{1} : invalid constraint. missing parameter {2} in {3}. params={4}"),
    CONSTRAINT_NOT_EXIST(400, "ATLAS-400-00-008", "{0}.{1} : invalid constraint. {2} {3}.{4} does not exist"),
    CONSTRAINT_NOT_MATCHED(400, "ATLAS-400-00-009", "{0}.{1} : invalid constraint. Data type of {2} {3}.{4} should be {5}, but found {6}"),
    UNKNOWN_TYPENAME(400, "ATLAS-400-00-00A", "{0}: Unknown typename"),
    CONSTRAINT_NOT_SUPPORTED_ON_MAP_TYPE(400, "ATLAS-400-00-00B", "{0}.{1} : constraints not supported on map type {2}"),
    CANNOT_ADD_MANDATORY_ATTRIBUTE(400, "ATLAS-400-00-00C", "{0}.{1} : can not add mandatory attribute"),
    ATTRIBUTE_DELETION_NOT_SUPPORTED(400, "ATLAS-400-00-00D", "{0}.{1} : attribute delete not supported"),
    SUPERTYPE_REMOVAL_NOT_SUPPORTED(400, "ATLAS-400-00-00E", "superType remove not supported"),
    UNEXPECTED_TYPE(400, "ATLAS-400-00-00F", "expected type {0}; found {1}"),
    TYPE_MATCH_FAILED(400, "ATLAS-400-00-010", "Given type {0} doesn't match {1}"),
    INVALID_TYPE_DEFINITION(400, "ATLAS-400-00-011", "Invalid type definition {0}"),
    INVALID_ATTRIBUTE_TYPE_FOR_CARDINALITY(400, "ATLAS-400-00-012", "Cardinality of attribute {0}.{1} requires a list or set type"),
    ATTRIBUTE_UNIQUE_INVALID(400, "ATLAS-400-00-013", "Type {0} with unique attribute {1} does not exist"),
    TYPE_NAME_INVALID(400, "ATLAS-400-00-014", "Type {0} with name {1} does not exist"),
    TYPE_CATEGORY_INVALID(400, "ATLAS-400-00-015", "Type Category {0} is invalid"),
    PATCH_NOT_APPLICABLE_FOR_TYPE(400, "ATLAS-400-00-016", "{0} - invalid patch for type {1}"),
    PATCH_FOR_UNKNOWN_TYPE(400, "ATLAS-400-00-017", "{0} - patch references unknown type {1}"),
    PATCH_INVALID_DATA(400, "ATLAS-400-00-018", "{0} - patch data is invalid for type {1}"),
    TYPE_NAME_INVALID_FORMAT(400, "ATLAS-400-00-019", "{0}: invalid name for {1}.  Names must consist of a letter followed by a sequence of letter, number, or '_' characters"),
    INVALID_PARAMETERS(400, "ATLAS-400-00-01A", "invalid parameters: {0}"),
    CLASSIFICATION_ALREADY_ASSOCIATED(400, "ATLAS-400-00-01B", "instance {0} already is associated with classification {1}"),
    CONSTRAINT_INVERSE_REF_ATTRIBUTE_INVALID_TYPE(400, "ATLAS-400-00-01C", "{0}.{1}: invalid {2} constraint. Attribute {3} is not an entity type"),
    CONSTRAINT_INVERSE_REF_INVERSE_ATTRIBUTE_NON_EXISTING(400, "ATLAS-400-00-01D", "{0}.{1}: invalid {2} constraint. Inverse attribute {3}.{4} does not exist"),
    CONSTRAINT_INVERSE_REF_INVERSE_ATTRIBUTE_INVALID_TYPE(400, "ATLAS-400-00-01E", "{0}.{1}: invalid {2} constraint. Inverse attribute {3}.{4} is not an entity type"),
    CONSTRAINT_OWNED_REF_ATTRIBUTE_INVALID_TYPE(400, "ATLAS-400-00-01F", "{0}.{1}: invalid {2} constraint. Attribute {3} is not an entity type"),
    CANNOT_MAP_ATTRIBUTE(400, "ATLAS-400-00-020", "cannot map attribute: {0} of type: {1} from vertex"),
    INVALID_OBJECT_ID(400, "ATLAS-400-00-021", "ObjectId is not valid {0}"),
    UNRESOLVED_REFERENCES_FOUND(400, "ATLAS-400-00-022", "Unresolved references: byId={0}; byUniqueAttributes={1}"),
    UNKNOWN_ATTRIBUTE(400, "ATLAS-400-00-023", "Attribute {0} not found for type {1}"),
    SYSTEM_TYPE(400, "ATLAS-400-00-024", "{0} is a System-type"),
    INVALID_STRUCT_VALUE(400, "ATLAS-400-00-025", "not a valid struct value {0}"),
    INSTANCE_LINEAGE_INVALID_PARAMS(400, "ATLAS-400-00-026", "Invalid lineage query parameters passed {0}: {1}"),
    ATTRIBUTE_UPDATE_NOT_SUPPORTED(400, "ATLAS-400-00-027", "{0}.{1} : attribute update not supported"),
	INVALID_VALUE(400, "ATLAS-400-00-028", "invalid value: {0}"),
    BAD_REQUEST(400, "ATLAS-400-00-020", "{0}"),

     // All Not found enums go here
    TYPE_NAME_NOT_FOUND(404, "ATLAS-404-00-001", "Given typename {0} was invalid"),
    TYPE_GUID_NOT_FOUND(404, "ATLAS-404-00-002", "Given type guid {0} was invalid"),
    EMPTY_RESULTS(404, "ATLAS-404-00-004", "No result found for {0}"),
    INSTANCE_GUID_NOT_FOUND(404, "ATLAS-404-00-005", "Given instance guid {0} is invalid/not found"),
    INSTANCE_LINEAGE_QUERY_FAILED(404, "ATLAS-404-00-006", "Instance lineage query failed {0}"),
    INSTANCE_CRUD_INVALID_PARAMS(404, "ATLAS-404-00-007", "Invalid instance creation/updation parameters passed : {0}"),
    CLASSIFICATION_NOT_FOUND(404, "ATLAS-404-00-008", "Given classification {0} was invalid"),
    INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND(404, "ATLAS-404-00-009", "Instance {0} with unique attribute {1} does not exist"),
    REFERENCED_ENTITY_NOT_FOUND(404, "ATLAS-404-00-00A", "Referenced entity {0} is not found"),
    INSTANCE_NOT_FOUND(404, "ATLAS-404-00-00B", "Given instance is invalid/not found: {0}"),

     // All data conflict errors go here
    TYPE_ALREADY_EXISTS(409, "ATLAS-409-00-001", "Given type {0} already exists"),
    TYPE_HAS_REFERENCES(409, "ATLAS-409-00-002", "Given type {0} has references"),
    INSTANCE_ALREADY_EXISTS(409, "ATLAS-409-00-003", "failed to update entity: {0}"),

     // All internal errors go here
    INTERNAL_ERROR(500, "ATLAS-500-00-001", "Internal server error {0}"),
    INDEX_CREATION_FAILED(500, "ATLAS-500-00-002", "Index creation failed for {0}"),
    INDEX_ROLLBACK_FAILED(500, "ATLAS-500-00-003", "Index rollback failed for {0}"),
    DISCOVERY_QUERY_FAILED(500, "ATLAS-500-00-004", "Discovery query failed {0}"),
    FAILED_TO_OBTAIN_TYPE_UPDATE_LOCK(500, "ATLAS-500-00-005", "Failed to get the lock; another type update might be in progress. Please try again"),
    FAILED_TO_OBTAIN_IMPORT_EXPORT_LOCK(500, "ATLAS-500-00-006", "Another import or export is in progress. Please try again"),
    NOTIFICATION_FAILED(500, "ATLAS-500-00-007", "Failed to notify for change {0}"),
    GREMLIN_GROOVY_SCRIPT_ENGINE_FAILED(500, "ATLAS-500-00-008", "scriptEngine cannot be initialized for: {0}"),
    JSON_ERROR_OBJECT_MAPPER_NULL_RETURNED(500, "ATLAS-500-00-009", "ObjectMapper.readValue returned NULL for class: {0}"),
    GREMLIN_SCRIPT_EXECUTION_FAILED(500, "ATLAS-500-00-00A", "Script execution failed for: {0}");

    private String errorCode;
    private String errorMessage;
    private Response.Status httpCode;

    private static final Logger LOG = LoggerFactory.getLogger(AtlasErrorCode.class);

    AtlasErrorCode(int httpCode, String errorCode, String errorMessage) {
        this.httpCode = Response.Status.fromStatusCode(httpCode);
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;

    }

    public String getFormattedErrorMessage(String... params) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== AtlasErrorCode.getMessage(%s)", Arrays.toString(params)));
        }

        MessageFormat mf = new MessageFormat(errorMessage);
        String result = mf.format(params);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("==> AtlasErrorCode.getMessage(%s): %s", Arrays.toString(params), result));
        }
        return result;
    }

    public Response.Status getHttpCode() {
        return httpCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
