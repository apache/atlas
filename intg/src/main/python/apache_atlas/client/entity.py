#!/usr/bin/env/python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from apache_atlas.model.instance import *
from apache_atlas.utils          import *


class EntityClient:
    ENTITY_API               = BASE_URI + "v2/entity/"
    PREFIX_ATTR              = "attr:"
    PREFIX_ATTR_             = "attr_"
    ADMIN_API                = BASE_URI + "admin/"
    ENTITY_PURGE_API         = ADMIN_API + "purge/"
    ENTITY_BULK_API          = ENTITY_API + "bulk/"
    BULK_SET_CLASSIFICATIONS = "bulk/setClassifications"
    BULK_HEADERS             = "bulk/headers"

    # Entity APIs
    GET_ENTITY_BY_GUID                    = API(ENTITY_API + "guid", HttpMethod.GET, HTTPStatus.OK)
    GET_ENTITY_BY_UNIQUE_ATTRIBUTE        = API(ENTITY_API + "uniqueAttribute/type", HttpMethod.GET, HTTPStatus.OK)
    GET_ENTITIES_BY_GUIDS                 = API(ENTITY_BULK_API, HttpMethod.GET, HTTPStatus.OK)
    GET_ENTITIES_BY_UNIQUE_ATTRIBUTE      = API(ENTITY_BULK_API + "uniqueAttribute/type", HttpMethod.GET, HTTPStatus.OK)
    GET_ENTITY_HEADER_BY_GUID             = API(ENTITY_API + "guid/{entity_guid}/header", HttpMethod.GET, HTTPStatus.OK)
    GET_ENTITY_HEADER_BY_UNIQUE_ATTRIBUTE = API(ENTITY_API + "uniqueAttribute/type/{type_name}/header", HttpMethod.GET, HTTPStatus.OK)

    GET_AUDIT_EVENTS              = API(ENTITY_API + "{guid}/audit", HttpMethod.GET, HTTPStatus.OK)
    CREATE_ENTITY                 = API(ENTITY_API, HttpMethod.POST, HTTPStatus.OK)
    CREATE_ENTITIES               = API(ENTITY_BULK_API, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_ENTITY                 = API(ENTITY_API, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_ENTITY_BY_ATTRIBUTE    = API(ENTITY_API + "uniqueAttribute/type/", HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_ENTITIES               = API(ENTITY_BULK_API, HttpMethod.POST, HTTPStatus.OK)
    PARTIAL_UPDATE_ENTITY_BY_GUID = API(ENTITY_API + "guid/{entity_guid}", HttpMethod.PUT, HTTPStatus.OK)
    DELETE_ENTITY_BY_GUID         = API(ENTITY_API + "guid", HttpMethod.DELETE, HTTPStatus.OK)
    DELETE_ENTITY_BY_ATTRIBUTE    = API(ENTITY_API + "uniqueAttribute/type/", HttpMethod.DELETE, HTTPStatus.OK)
    DELETE_ENTITIES_BY_GUIDS      = API(ENTITY_BULK_API, HttpMethod.DELETE, HTTPStatus.OK)
    PURGE_ENTITIES_BY_GUIDS       = API(ENTITY_PURGE_API, HttpMethod.PUT, HTTPStatus.OK)

    # Classification APIs
    GET_CLASSIFICATIONS                         = API(ENTITY_API + "guid/{guid}/classifications", HttpMethod.GET, HTTPStatus.OK)
    GET_FROM_CLASSIFICATION                     = API(ENTITY_API + "guid/{entity_guid}/classification/{classification}", HttpMethod.GET, HTTPStatus.OK)
    ADD_CLASSIFICATIONS                         = API(ENTITY_API + "guid/{guid}/classifications", HttpMethod.POST, HTTPStatus.NO_CONTENT)
    ADD_CLASSIFICATION                          = API(ENTITY_BULK_API + "/classification", HttpMethod.POST, HTTPStatus.NO_CONTENT)
    ADD_CLASSIFICATION_BY_TYPE_AND_ATTRIBUTE    = API(ENTITY_API + "uniqueAttribute/type/{type_name}/classifications", HttpMethod.POST, HTTPStatus.NO_CONTENT)
    UPDATE_CLASSIFICATIONS                      = API(ENTITY_API + "guid/{guid}/classifications", HttpMethod.PUT, HTTPStatus.NO_CONTENT)
    UPDATE_CLASSIFICATION_BY_TYPE_AND_ATTRIBUTE = API(ENTITY_API + "uniqueAttribute/type/{type_name}/classifications", HttpMethod.PUT, HTTPStatus.NO_CONTENT)
    UPDATE_BULK_SET_CLASSIFICATIONS             = API(ENTITY_API + BULK_SET_CLASSIFICATIONS, HttpMethod.POST, HTTPStatus.OK)
    DELETE_CLASSIFICATION                       = API(ENTITY_API + "guid/{guid}/classification/{classification_name}", HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_CLASSIFICATION_BY_TYPE_AND_ATTRIBUTE = API(ENTITY_API + "uniqueAttribute/type/{type_name}/classification/{" "classification_name}", HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_BULK_HEADERS                            = API(ENTITY_API + BULK_HEADERS, HttpMethod.GET, HTTPStatus.OK)

    # Business Attributes APIs
    ADD_BUSINESS_ATTRIBUTE            = API(ENTITY_API + "guid/{entity_guid}/businessmetadata", HttpMethod.POST, HTTPStatus.NO_CONTENT)
    ADD_BUSINESS_ATTRIBUTE_BY_NAME    = API(ENTITY_API + "guid/{entity_guid}/businessmetadata/{bm_name}", HttpMethod.POST, HTTPStatus.NO_CONTENT)
    DELETE_BUSINESS_ATTRIBUTE         = API(ENTITY_API + "guid/{entity_guid}/businessmetadata", HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_BUSINESS_ATTRIBUTE_BY_NAME = API(ENTITY_API + "guid/{entity_guid}/businessmetadata/{bm_name}", HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    GET_BUSINESS_METADATA_TEMPLATE    = API(ENTITY_API + "businessmetadata/import/template", HttpMethod.GET, HTTPStatus.OK, APPLICATION_JSON, APPLICATION_OCTET_STREAM)
    IMPORT_BUSINESS_METADATA          = API(ENTITY_API + "businessmetadata/import", HttpMethod.POST, HTTPStatus.OK, MULTIPART_FORM_DATA, APPLICATION_JSON)

    # Labels APIs
    ADD_LABELS                        = API(ENTITY_API + "guid/{entity_guid}/labels", HttpMethod.PUT, HTTPStatus.NO_CONTENT)
    ADD_LABELS_BY_UNIQUE_ATTRIBUTE    = API(ENTITY_API + "uniqueAttribute/type/{type_name}/labels", HttpMethod.PUT, HTTPStatus.NO_CONTENT)
    SET_LABELS                        = API(ENTITY_API + "guid/%s/labels", HttpMethod.POST, HTTPStatus.NO_CONTENT)
    SET_LABELS_BY_UNIQUE_ATTRIBUTE    = API(ENTITY_API + "uniqueAttribute/type/{entity_guid}/labels", HttpMethod.POST, HTTPStatus.NO_CONTENT)
    DELETE_LABELS                     = API(ENTITY_API + "guid/{entity_guid}/labels", HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_LABELS_BY_UNIQUE_ATTRIBUTE = API(ENTITY_API + "uniqueAttribute/type/{type_name}/labels", HttpMethod.DELETE, HTTPStatus.NO_CONTENT)

    def __init__(self, client):
        self.client = client

    def get_entity_by_guid(self, guid, min_ext_info=False, ignore_relationships=False):
        query_params = {"minExtInfo": min_ext_info, "ignoreRelationships": ignore_relationships}

        return self.client.call_api(EntityClient.GET_ENTITY_BY_GUID.format_path_with_params(guid), AtlasEntityWithExtInfo, query_params)

    def get_entity_by_attribute(self, type_name, uniq_attributes, min_ext_info=False, ignore_relationships=False):
        query_params = attributes_to_params(uniq_attributes)
        query_params["minExtInfo"] = min_ext_info
        query_params["ignoreRelationships"] = ignore_relationships

        return self.client.call_api(EntityClient.GET_ENTITY_BY_UNIQUE_ATTRIBUTE.format_path_with_params(type_name),
                                    AtlasEntityWithExtInfo, query_params)

    def get_entities_by_guids(self, guids, min_ext_info=False, ignore_relationships=False):
        query_params = {"guid": guids, "minExtInfo": min_ext_info, "ignoreRelationships": ignore_relationships}

        return self.client.call_api(EntityClient.GET_ENTITIES_BY_GUIDS, AtlasEntitiesWithExtInfo, query_params)

    def get_entities_by_attribute(self, type_name, uniq_attributes_list, min_ext_info=False, ignore_relationships=False):
        query_params = list_attributes_to_params(uniq_attributes_list)
        query_params["minExtInfo"] = min_ext_info
        query_params["ignoreRelationships"] = ignore_relationships

        return self.client.call_api(EntityClient.GET_ENTITIES_BY_UNIQUE_ATTRIBUTE.format_path_with_params(type_name),
                                    AtlasEntitiesWithExtInfo, query_params)

    def get_entity_header_by_guid(self, entity_guid):
        return self.client.call_api(EntityClient.GET_ENTITY_HEADER_BY_GUID.format_path({'entity_guid': entity_guid}),
                                    AtlasEntityHeader)

    def get_entity_header_by_attribute(self, type_name, uniq_attributes):
        query_params = attributes_to_params(uniq_attributes)

        return self.client.call_api(EntityClient.GET_ENTITY_HEADER_BY_UNIQUE_ATTRIBUTE.format_path({'type_name': type_name}),
                                    AtlasEntityHeader, query_params)

    def get_audit_events(self, guid, start_key, audit_action, count):
        query_params = {"startKey": start_key, "count": count}

        if audit_action:
            query_params["auditAction"] = audit_action

        return self.client.call_api(EntityClient.GET_AUDIT_EVENTS.format_path({'guid': guid}), list, query_params)

    def create_entity(self, entity):
        return self.client.call_api(EntityClient.CREATE_ENTITY, EntityMutationResponse, None, entity)

    def create_entities(self, atlas_entities):
        return self.client.call_api(EntityClient.CREATE_ENTITIES, EntityMutationResponse, None, atlas_entities)

    def update_entity(self, entity):
        return self.client.call_api(EntityClient.UPDATE_ENTITY, EntityMutationResponse, None, entity)

    def update_entities(self, atlas_entities):
        return self.client.call_api(EntityClient.UPDATE_ENTITY, EntityMutationResponse, None, atlas_entities)

    def partial_update_entity_by_guid(self, entity_guid, attr_value, attr_name):
        query_params = {"name", attr_name}

        return self.client.call_api(EntityClient.PARTIAL_UPDATE_ENTITY_BY_GUID.format_path({'entity_guid': entity_guid}),
                                    EntityMutationResponse, attr_value, query_params)

    def delete_entity_by_guid(self, guid):
        return self.client.call_api(EntityClient.DELETE_ENTITY_BY_GUID.format_path_with_params(guid), EntityMutationResponse)

    def delete_entity_by_attribute(self, type_name, uniq_attributes):
        query_param = attributes_to_params(uniq_attributes)

        return self.client.call_api(EntityClient.DELETE_ENTITY_BY_ATTRIBUTE.format_path_with_params(type_name),
                                    EntityMutationResponse, query_param)

    def delete_entities_by_guids(self, guids):
        query_params = {"guid": guids}

        return self.client.call_api(EntityClient.DELETE_ENTITIES_BY_GUIDS, EntityMutationResponse, query_params)

    def purge_entities_by_guids(self, guids):
        return self.client.call_api(EntityClient.PURGE_ENTITIES_BY_GUIDS, EntityMutationResponse, None, guids)

    # Entity-classification APIs

    def get_classifications(self, guid):
        return self.client.call_api(EntityClient.GET_CLASSIFICATIONS.format_path({'guid': guid}), AtlasClassifications)

    def get_entity_classifications(self, entity_guid, classification_name):
        return self.client.call_api(EntityClient.GET_FROM_CLASSIFICATION.format_path(
            {'entity_guid': entity_guid, 'classification': classification_name}), AtlasClassifications)

    def add_classification(self, request):
        self.client.call_api(EntityClient.ADD_CLASSIFICATION, None, None, request)

    def add_classifications_by_guid(self, guid, classifications):
        self.client.call_api(EntityClient.ADD_CLASSIFICATIONS.format_path({'guid': guid}), None, None, classifications)

    def add_classifications_by_type(self, type_name, uniq_attributes, classifications):
        query_param = attributes_to_params(uniq_attributes)

        self.client.call_api(EntityClient.ADD_CLASSIFICATION_BY_TYPE_AND_ATTRIBUTE.format_path({'type_name': type_name}),
                             None, query_param, classifications)

    def update_classifications(self, guid, classifications):
        self.client.call_api(EntityClient.UPDATE_CLASSIFICATIONS.format_path({'guid': guid}), None, None, classifications)

    def update_classifications_by_attr(self, type_name, uniq_attributes, classifications):
        query_param = attributes_to_params(uniq_attributes)

        self.client.call_api(EntityClient.UPDATE_CLASSIFICATION_BY_TYPE_AND_ATTRIBUTE.format_path({'type_name': type_name}),
            None, query_param, classifications)

    def set_classifications(self, entity_headers):
        return self.client.call_api(EntityClient.UPDATE_BULK_SET_CLASSIFICATIONS, str, None, entity_headers)

    def delete_classification(self, guid, classification_name):
        query = {'guid': guid, 'classification_name': classification_name}

        return self.client.call_api(EntityClient.DELETE_CLASSIFICATION.format_path(query))

    def delete_classifications(self, guid, classifications):
        for atlas_classification in classifications:
            query = {'guid': guid, 'classification_name': atlas_classification.typeName}

            self.client.call_api(EntityClient.DELETE_CLASSIFICATION.format_path(query))

    def remove_classification(self, entity_guid, classification_name, associated_entity_guid):
        query = {'guid': entity_guid, 'classification_name': classification_name}

        self.client.call_api(EntityClient.DELETE_CLASSIFICATION.formart_path(query), None, None, associated_entity_guid)

    def remove_classification_by_name(self, type_name, uniq_attributes, classification_name):
        query_params = attributes_to_params(uniq_attributes)
        query        = {'type_name': type_name, 'classification_name': classification_name}

        self.client.call_api(EntityClient.DELETE_CLASSIFICATION_BY_TYPE_AND_ATTRIBUTE.format_path({query}), None, query_params)

    def get_entity_headers(self, tag_update_start_time):
        query_params = {"tagUpdateStartTime", tag_update_start_time}

        return self.client.call_api(EntityClient.GET_BULK_HEADERS, AtlasEntityHeaders, query_params)

    # Business attributes APIs
    def add_or_update_business_attributes(self, entity_guid, is_overwrite, business_attributes):
        query_params = {"isOverwrite", is_overwrite}

        self.client.call_api(EntityClient.ADD_BUSINESS_ATTRIBUTE.format_path({'entity_guid': entity_guid}), None,
                             query_params, business_attributes)

    def add_or_update_business_attributes_bm_name(self, entity_guid, bm_name, business_attributes):
        query = {'entity_guid': entity_guid, 'bm_name': bm_name}

        self.client.call_api(EntityClient.ADD_BUSINESS_ATTRIBUTE_BY_NAME.format_path(query), None, None, business_attributes)

    def remove_business_attributes(self, entity_guid, business_attributes):
        self.client.call_api(EntityClient.DELETE_BUSINESS_ATTRIBUTE.format_path({'entity_guid': entity_guid}), None,
                             None, business_attributes)

    def remove_business_attributes_bm_name(self, entity_guid, bm_name, business_attributes):
        query = {'entity_guid': entity_guid, 'bm_name': bm_name}

        self.client.call_api(EntityClient.DELETE_BUSINESS_ATTRIBUTE_BY_NAME.format_path(query), None, None, business_attributes)

    # Labels APIs
    def add_labels_by_guid(self, entity_guid, labels):
        self.client.call_api(EntityClient.ADD_LABELS.format_path({'entity_guid': entity_guid}), None, None, labels)

    def add_labels_by_name(self, type_name, uniq_attributes, labels):
        query_param = attributes_to_params(uniq_attributes)

        self.client.call_api(EntityClient.SET_LABELS_BY_UNIQUE_ATTRIBUTE.format_path({'type_name': type_name}), None,
                             query_param, labels)

    def remove_labels_by_guid(self, entity_guid, labels):
        self.client.call_api(EntityClient.DELETE_LABELS.format_path({'entity_guid': entity_guid}), None, None, labels)

    def remove_labels_by_name(self, type_name, uniq_attributes, labels):
        query_param = attributes_to_params(uniq_attributes)

        self.client.call_api(EntityClient.DELETE_LABELS_BY_UNIQUE_ATTRIBUTE.format({'type_name': type_name}), None,
                             query_param, labels)

    def set_labels_by_guid(self, entity_guid, labels):
        self.client.call_api(EntityClient.SET_LABELS.format({'entity_guid': entity_guid}), None, None, labels)

    def set_labels_by_name(self, type_name, uniq_attributes, labels):
        query_param = attributes_to_params(uniq_attributes)

        self.client.call_api(EntityClient.ADD_LABELS_BY_UNIQUE_ATTRIBUTE.format({'type_name': type_name}), None, query_param, labels)