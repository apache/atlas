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

from apache_atlas.model.discovery import *
from apache_atlas.utils           import API, HttpMethod, HTTPStatus


class DiscoveryClient:
    DISCOVERY_URI        = BASE_URI + "v2/search"
    DSL_SEARCH_URI       = DISCOVERY_URI + "/dsl"
    FULL_TEXT_SEARCH_URI = DISCOVERY_URI + "/fulltext"
    BASIC_SEARCH_URI     = DISCOVERY_URI + "/basic"
    FACETED_SEARCH_URI   = BASIC_SEARCH_URI
    SAVED_SEARCH_URI     = DISCOVERY_URI + "/saved"
    QUICK_SEARCH_URI     = DISCOVERY_URI + "/quick"

    DSL_SEARCH             = API(DSL_SEARCH_URI, HttpMethod.GET, HTTPStatus.OK)
    FULL_TEXT_SEARCH       = API(FULL_TEXT_SEARCH_URI, HttpMethod.GET, HTTPStatus.OK)
    BASIC_SEARCH           = API(BASIC_SEARCH_URI, HttpMethod.GET, HTTPStatus.OK)
    FACETED_SEARCH         = API(FACETED_SEARCH_URI, HttpMethod.POST, HTTPStatus.OK)
    ATTRIBUTE_SEARCH       = API(DISCOVERY_URI + "/attribute", HttpMethod.GET, HTTPStatus.OK)
    RELATIONSHIP_SEARCH    = API(DISCOVERY_URI + "/relationship", HttpMethod.GET, HTTPStatus.OK)
    QUICK_SEARCH_WITH_GET  = API(QUICK_SEARCH_URI, HttpMethod.GET, HTTPStatus.OK)
    QUICK_SEARCH_WITH_POST = API(QUICK_SEARCH_URI, HttpMethod.POST, HTTPStatus.OK)
    GET_SUGGESTIONS        = API(DISCOVERY_URI + "/suggestions", HttpMethod.GET, HTTPStatus.OK)

    # Saved Search
    GET_SAVED_SEARCHES           = API(SAVED_SEARCH_URI, HttpMethod.GET, HTTPStatus.OK)
    GET_SAVED_SEARCH             = API(SAVED_SEARCH_URI + "/{search_name}", HttpMethod.GET, HTTPStatus.OK)
    ADD_SAVED_SEARCH             = API(SAVED_SEARCH_URI, HttpMethod.POST, HTTPStatus.OK)
    UPDATE_SAVED_SEARCH          = API(SAVED_SEARCH_URI, HttpMethod.PUT, HTTPStatus.OK)
    DELETE_SAVED_SEARCH          = API(SAVED_SEARCH_URI + "/{guid}", HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    EXECUTE_SAVED_SEARCH_BY_NAME = API(SAVED_SEARCH_URI + "/execute/{search_name}", HttpMethod.GET, HTTPStatus.OK)
    EXECUTE_SAVED_SEARCH_BY_GUID = API(SAVED_SEARCH_URI + "/execute/guid/{search_guid}", HttpMethod.GET, HTTPStatus.OK)

    QUERY  = "query"
    LIMIT  = "limit"
    OFFSET = "offset"
    STATUS = "Status"

    def __init__(self, client):
        self.client = client

    def dsl_search(self, query):
        query_params = {DiscoveryClient.QUERY, query}

        return self.client.call_api(DiscoveryClient.DSL_SEARCH, AtlasSearchResult, query_params)

    def dsl_search_with_params(self, query, limit, offset):
        query_params = {DiscoveryClient.QUERY: query, DiscoveryClient.LIMIT: limit, DiscoveryClient.OFFSET: offset}

        return self.client.call_api(DiscoveryClient.DSL_SEARCH, AtlasSearchResult, query_params)

    def full_text_search(self, query):
        query_params = {DiscoveryClient.QUERY, query}

        return self.client.call_api(DiscoveryClient.FULL_TEXT_SEARCH, AtlasSearchResult, query_params)

    def full_text_search_with_params(self, query, limit, offset):
        query_params = {DiscoveryClient.QUERY: query, DiscoveryClient.LIMIT: limit, DiscoveryClient.OFFSET: offset}

        return self.client.call_api(DiscoveryClient.FULL_TEXT_SEARCH, AtlasSearchResult, query_params)

    def basic_search(self, type_name, classification, query, exclude_deleted_entities, limit, offset):
        query_params = {"typeName": type_name, "classification": classification, DiscoveryClient.QUERY: query,
                        "excludeDeletedEntities": exclude_deleted_entities, DiscoveryClient.LIMIT: limit,
                        DiscoveryClient.OFFSET: offset }

        return self.client.call_api(DiscoveryClient.BASIC_SEARCH, AtlasSearchResult, query_params)

    def faceted_search(self, search_parameters):
        return self.client.call_api(DiscoveryClient.FACETED_SEARCH, AtlasSearchResult, search_parameters)

    def attribute_search(self, type_name, attr_name, attr_value_prefix, limit, offset):
        query_params = {"attrName": attr_name, "attrValuePrefix": attr_value_prefix, "typeName": type_name,
                        DiscoveryClient.LIMIT: limit, DiscoveryClient.OFFSET: offset}

        return self.client.call_api(DiscoveryClient.ATTRIBUTE_SEARCH, AtlasSearchResult, query_params)

    def relationship_search(self, guid, relation, sort_by_attribute, sort_order, exclude_deleted_entities, limit, offset):
        query_params = {"guid": guid, "relation": relation, "sortBy": sort_by_attribute,
                        "excludeDeletedEntities": exclude_deleted_entities,
                        DiscoveryClient.LIMIT: limit, DiscoveryClient.OFFSET: offset}

        if sort_order:
            query_params["sortOrder"] = str(sort_order)

        return self.client.call_api(DiscoveryClient.RELATIONSHIP_SEARCH, AtlasSearchResult, query_params)

    def quick_search(self, query, type_name, exclude_deleted_entities, limit, offset):
        query_params = {"query": query, "typeName": type_name, "excludeDeletedEntities": exclude_deleted_entities,
                        DiscoveryClient.LIMIT: limit, DiscoveryClient.OFFSET: offset}

        return self.client.call_api(DiscoveryClient.QUICK_SEARCH_WITH_GET, AtlasQuickSearchResult, query_params)

    def quick_search_with_param(self, quick_search_parameters):
        return self.client.call_api(DiscoveryClient.QUICK_SEARCH_WITH_POST, AtlasQuickSearchResult, None,
                                    quick_search_parameters)

    # fieldName should be the parameter on which indexing is enabled such as "qualifiedName"
    def get_suggestions(self, prefix_string, field_name):
        query_params = {}

        if not prefix_string:
            query_params["prefixString"] = prefix_string

        if not field_name:
            query_params["fieldName"] = field_name

        return self.client.call_api(DiscoveryClient.GET_SUGGESTIONS, AtlasSuggestionsResult, query_params)

    def get_saved_searches(self, user_name):
        query_params = {"user", user_name}

        return self.client.call_api(DiscoveryClient.GET_SAVED_SEARCHES, list, query_params)

    def get_saved_search(self, user_name, search_name):
        query_params = {"user", user_name}

        return self.client.call_api(DiscoveryClient.GET_SAVED_SEARCH.format_path({'search_name': search_name}),
                                    AtlasUserSavedSearch, query_params)

    def add_saved_search(self, saved_search):
        return self.client.call_api(DiscoveryClient.ADD_SAVED_SEARCH, AtlasUserSavedSearch, None, saved_search)

    def update_saved_search(self, saved_search):
        return self.client.call_api(DiscoveryClient.UPDATE_SAVED_SEARCH, AtlasUserSavedSearch, None, saved_search)

    def delete_saved_search(self, guid):
        return self.client.call_api(DiscoveryClient.DELETE_SAVED_SEARCH.format_path({'guid': guid}))

    def execute_saved_search(self, user_name, search_name):
        query_params = {"user", user_name}

        return self.client.call_api(DiscoveryClient.EXECUTE_SAVED_SEARCH_BY_NAME.format_path({'search_name': search_name}),
                                    AtlasSearchResult, query_params)

    def execute_saved_search(self, search_guid):
        return self.client.call_api(DiscoveryClient.EXECUTE_SAVED_SEARCH_BY_GUID.format_path({'search_guid': search_guid}),
                                    AtlasSearchResult)