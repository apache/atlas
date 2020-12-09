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

from apache_atlas.model.glossary import *
from apache_atlas.utils          import *


class GlossaryClient:
    GLOSSARY_URI        = BASE_URI     + "v2/glossary"
    GLOSSARY_TERM       = GLOSSARY_URI + "/term"
    GLOSSARY_TERMS      = GLOSSARY_URI + "/terms"
    GLOSSARY_CATEGORY   = GLOSSARY_URI + "/category"
    GLOSSARY_CATEGORIES = GLOSSARY_URI + "/categories"

    GET_ALL_GLOSSARIES              = API(GLOSSARY_URI, HttpMethod.GET, HTTPStatus.OK)
    GET_GLOSSARY_BY_GUID            = API(GLOSSARY_URI + "/{glossary_guid}", HttpMethod.GET, HTTPStatus.OK)
    GET_DETAILED_GLOSSARY           = API(GLOSSARY_URI + "/{glossary_guid}/detailed", HttpMethod.GET, HTTPStatus.OK)

    GET_GLOSSARY_TERM               = API(GLOSSARY_TERM, HttpMethod.GET, HTTPStatus.OK)
    GET_GLOSSARY_TERMS              = API(GLOSSARY_URI + "/{glossary_guid}/terms", HttpMethod.GET, HTTPStatus.OK)
    GET_GLOSSARY_TERMS_HEADERS      = API(GLOSSARY_URI + "/{glossary_guid}/terms/headers", HttpMethod.GET, HTTPStatus.OK)

    GET_GLOSSARY_CATEGORY           = API(GLOSSARY_CATEGORY, HttpMethod.GET, HTTPStatus.OK)
    GET_GLOSSARY_CATEGORIES         = API(GLOSSARY_URI + "/{glossary_guid}/categories", HttpMethod.GET, HTTPStatus.OK)
    GET_GLOSSARY_CATEGORIES_HEADERS = API(GLOSSARY_URI + "/{glossary_guid}/categories/headers", HttpMethod.GET, HTTPStatus.OK)

    GET_CATEGORY_TERMS              = API(GLOSSARY_CATEGORY + "/{category_guid}/terms", HttpMethod.GET, HTTPStatus.OK)
    GET_RELATED_TERMS               = API(GLOSSARY_TERMS + "/{term_guid}/related", HttpMethod.GET, HTTPStatus.OK)
    GET_RELATED_CATEGORIES          = API(GLOSSARY_CATEGORY + "/{category_guid}/related", HttpMethod.GET, HTTPStatus.OK)
    CREATE_GLOSSARY                 = API(GLOSSARY_URI, HttpMethod.POST, HTTPStatus.OK)
    CREATE_GLOSSARY_TERM            = API(GLOSSARY_TERM, HttpMethod.POST, HTTPStatus.OK)
    CREATE_GLOSSARY_TERMS           = API(GLOSSARY_TERMS, HttpMethod.POST, HTTPStatus.OK)
    CREATE_GLOSSARY_CATEGORY        = API(GLOSSARY_CATEGORY, HttpMethod.POST, HTTPStatus.OK)
    CREATE_GLOSSARY_CATEGORIES      = API(GLOSSARY_CATEGORIES, HttpMethod.POST, HTTPStatus.OK)

    UPDATE_GLOSSARY_BY_GUID         = API(GLOSSARY_URI + "/{glossary_guid}", HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_PARTIAL_GLOSSARY         = API(GLOSSARY_URI + "/{glossary_guid}/partial", HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_GLOSSARY_TERM            = API(GLOSSARY_TERM + "/{term_guid}", HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_PARTIAL_TERM             = API(GLOSSARY_TERM + "/{term_guid}/partial", HttpMethod.PUT, HTTPStatus.OK)

    UPDATE_CATEGORY_BY_GUID         = API(GLOSSARY_CATEGORY + "/{category_guid}", HttpMethod.PUT, HTTPStatus.OK)
    UPDATE_PARTIAL_CATEGORY         = API(GLOSSARY_CATEGORY + "/{category_guid}/partial", HttpMethod.PUT, HTTPStatus.OK)

    DELETE_GLOSSARY_BY_GUID         = API(GLOSSARY_URI + "/{glossary_guid}", HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_TERM_BY_GUID             = API(GLOSSARY_TERM + "/{term_guid}", HttpMethod.DELETE, HTTPStatus.NO_CONTENT)
    DELETE_CATEGORY_BY_GUID         = API(GLOSSARY_CATEGORY + "/{category_guid}", HttpMethod.DELETE, HTTPStatus.NO_CONTENT)

    GET_ENTITIES_ASSIGNED_WITH_TERM = API(GLOSSARY_TERMS + "/{term_guid}/assignedEntities", HttpMethod.GET, HTTPStatus.OK)
    ASSIGN_TERM_TO_ENTITIES         = API(GLOSSARY_TERMS + "/{term_guid}/assignedEntities", HttpMethod.POST, HTTPStatus.NO_CONTENT)
    DISASSOCIATE_TERM_FROM_ENTITIES = API(GLOSSARY_TERMS + "/{term_guid}/assignedEntities", HttpMethod.PUT, HTTPStatus.NO_CONTENT)

    GET_IMPORT_GLOSSARY_TEMPLATE    = API(GLOSSARY_URI + "/import/template", HttpMethod.GET, HTTPStatus.OK, APPLICATION_JSON, APPLICATION_OCTET_STREAM)
    IMPORT_GLOSSARY                 = API(GLOSSARY_URI + "/import", HttpMethod.POST, HTTPStatus.OK, MULTIPART_FORM_DATA, APPLICATION_JSON)

    QUERY  = "query"
    LIMIT  = "limit"
    OFFSET = "offset"
    STATUS = "Status"

    def __init__(self, client):
        self.client = client

    def get_all_glossaries(self, sort_by_attribute, limit, offset):
        query_params = {"sort": sort_by_attribute, GlossaryClient.LIMIT: limit, GlossaryClient.OFFSET: offset}

        return self.client.call_api(GlossaryClient.GET_ALL_GLOSSARIES, list, query_params)

    def get_glossary_by_guid(self, glossary_guid):
        return self.client.call_api(GlossaryClient.GET_GLOSSARY_BY_GUID.format_path({'glossary_guid': glossary_guid}),
                                    AtlasGlossary)

    def get_glossary_ext_info(self, glossary_guid):
        return self.client.call_api(GlossaryClient.GET_DETAILED_GLOSSARY.format_path({'glossary_guid': glossary_guid}),
                                    AtlasGlossaryExtInfo)

    def get_glossary_term(self, term_guid):
        return self.client.call_api(GlossaryClient.GET_GLOSSARY_TERM.format_path_with_params(term_guid), AtlasGlossaryTerm)

    def get_glossary_terms(self, glossary_guid, sort_by_attribute, limit, offset):
        query_params = {"glossaryGuid": glossary_guid, GlossaryClient.LIMIT: limit, GlossaryClient.OFFSET: offset,
                        "sort": sort_by_attribute}

        return self.client.call_api(GlossaryClient.GET_GLOSSARY_TERMS.format_path({'glossary_guid': glossary_guid}),
                                    list, query_params)

    def get_glossary_term_headers(self, glossary_guid, sort_by_attribute, limit, offset):
        query_params = {"glossaryGuid": glossary_guid, GlossaryClient.LIMIT: limit, GlossaryClient.OFFSET: offset,
                        "sort": sort_by_attribute}

        return self.client.call_api(GlossaryClient.GET_GLOSSARY_TERMS_HEADERS.format_path({'glossary_guid': glossary_guid}),
                                    list, query_params)

    def get_glossary_category(self, category_guid):
        return self.client.call_api(GlossaryClient.GET_GLOSSARY_CATEGORY.format_path_with_params(category_guid),
                                    AtlasGlossaryCategory)

    def get_glossary_categories(self, glossary_guid, sort_by_attribute, limit, offset):
        query_params = {"glossaryGuid": glossary_guid, GlossaryClient.LIMIT: limit, GlossaryClient.OFFSET: offset,
                        "sort": sort_by_attribute}

        return self.client.call_api(GlossaryClient.GET_GLOSSARY_CATEGORIES.format_path({'glossary_guid': glossary_guid}),
                                    list, query_params)

    def get_glossary_category_headers(self, glossary_guid, sort_by_attribute, limit, offset):
        query_params = {"glossaryGuid": glossary_guid, GlossaryClient.LIMIT: limit, GlossaryClient.OFFSET: offset,
                        "sort": sort_by_attribute}

        return self.client.call_api(GlossaryClient.GET_GLOSSARY_CATEGORIES_HEADERS.format_path({'glossary_guid': glossary_guid}),
                                    list, query_params)

    def get_category_terms(self, category_guid, sort_by_attribute, limit, offset):
        query_params = {"categoryGuid": category_guid, GlossaryClient.LIMIT: limit, GlossaryClient.OFFSET: offset,
                        "sort": sort_by_attribute}

        return self.client.call_api(GlossaryClient.GET_CATEGORY_TERMS.format_path({'category_guid': category_guid}),
                                    list, query_params)

    def get_related_terms(self, term_guid, sort_by_attribute, limit, offset):
        query_params = {"termGuid": term_guid, GlossaryClient.LIMIT: limit, GlossaryClient.OFFSET: offset,
                        "sort": sort_by_attribute }

        return self.client.call_api(GlossaryClient.GET_RELATED_TERMS.format_path({'term_guid': term_guid}), dict, query_params)

    def get_related_categories(self, category_guid, sort_by_attribute, limit, offset):
        query_params = {GlossaryClient.LIMIT: limit, GlossaryClient.OFFSET: offset, "sort": sort_by_attribute}

        return self.client.call_api(GlossaryClient.GET_RELATED_CATEGORIES.format_path({'category_guid': category_guid}),
                                    dict, query_params)

    def create_glossary(self, glossary):
        return self.client.call_api(GlossaryClient.CREATE_GLOSSARY, AtlasGlossary, None, glossary)

    def create_glossary_term(self, glossary_term):
        return self.client.call_api(GlossaryClient.CREATE_GLOSSARY_TERM, AtlasGlossaryTerm, None, glossary_term)

    def create_glossary_terms(self, glossary_terms):
        return self.client.call_api(GlossaryClient.CREATE_GLOSSARY_TERMS, list, None, glossary_terms)

    def create_glossary_category(self, glossary_category):
        return self.client.call_api(GlossaryClient.CREATE_GLOSSARY_CATEGORY, AtlasGlossaryCategory, None, glossary_category)

    def create_glossary_categories(self, glossary_categories):
        return self.client.call_api(GlossaryClient.CREATE_GLOSSARY_CATEGORIES, list, glossary_categories)

    def update_glossary_by_guid(self, glossary_guid, updated_glossary):
        return self.client.call_api(GlossaryClient.UPDATE_GLOSSARY_BY_GUID.format_path({'glossary_guid': glossary_guid}),
                                    AtlasGlossary, None, updated_glossary)

    def partial_update_glossary_by_guid(self, glossary_guid, attributes):
        return self.client.call_api(GlossaryClient.UPDATE_PARTIAL_GLOSSARY.format_path({'glossary_guid': glossary_guid}),
                                    AtlasGlossary, attributes)

    def update_glossary_term_by_guid(self, term_guid, glossary_term):
        return self.client.call_api(GlossaryClient.UPDATE_GLOSSARY_TERM.format_path({'term_guid': term_guid}),
                                    AtlasGlossaryTerm, None, glossary_term)

    def partial_update_term_by_guid(self, term_guid, attributes):
        return self.client.call_api(GlossaryClient.UPDATE_PARTIAL_TERM.format_path({'term_guid': term_guid}),
                                    AtlasGlossaryTerm, attributes)

    def update_glossary_category_by_guid(self, category_guid, glossary_category):
        return self.client.call_api(GlossaryClient.UPDATE_CATEGORY_BY_GUID.format_path({'category_guid': category_guid}),
                                    AtlasGlossaryCategory, glossary_category)

    def partial_update_category_by_guid(self, category_guid, attributes):
        return self.client.call_api(GlossaryClient.UPDATE_PARTIAL_CATEGORY.format_path({'category_guid': category_guid}),
                                    AtlasGlossaryCategory, None, attributes)

    def delete_glossary_by_guid(self, glossary_guid):
        return self.client.call_api(GlossaryClient.DELETE_GLOSSARY_BY_GUID.format_path({'glossary_guid': glossary_guid}))

    def delete_glossary_term_by_guid(self, term_guid):
        return self.client.call_api(GlossaryClient.DELETE_TERM_BY_GUID.format_path({'term_guid': term_guid}))

    def delete_glossary_category_by_guid(self, category_guid):
        return self.client.call_api(GlossaryClient.DELETE_CATEGORY_BY_GUID.format_path({'category_guid': category_guid}))

    def get_entities_assigned_with_term(self, term_guid, sort_by_attribute, limit, offset):
        query_params = {"termGuid": term_guid, GlossaryClient.LIMIT: limit, GlossaryClient.OFFSET: offset, "sort": sort_by_attribute }

        return self.client.call_api(GlossaryClient.GET_ENTITIES_ASSIGNED_WITH_TERM.format_path({'term_guid': term_guid}), list, query_params)

    def assign_term_to_entities(self, term_guid, related_object_ids):
        return self.client.call_api(GlossaryClient.ASSIGN_TERM_TO_ENTITIES.format_path({'term_guid': term_guid}),
                                    None, None, related_object_ids)

    def disassociate_term_from_entities(self, term_guid, related_object_ids):
        return self.client.call_api(GlossaryClient.DISASSOCIATE_TERM_FROM_ENTITIES.format_path({'term_guid': term_guid}),
                                    None, None, related_object_ids)
