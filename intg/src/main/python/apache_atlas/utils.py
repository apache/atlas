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

import enum
from json import JSONEncoder
import time

BASE_URI                 = "api/atlas/"
APPLICATION_JSON         = 'application/json'
APPLICATION_OCTET_STREAM = 'application/octet-stream'
MULTIPART_FORM_DATA      = 'multipart/form-data'
PREFIX_ATTR              = "attr:"
PREFIX_ATTR_             = "attr_"

s_nextId = milliseconds = int(round(time.time() * 1000)) + 1


def list_attributes_to_params(attributes_list, query_params=None):
    if not query_params:
        query_params = {}

    for i, attr in enumerate(attributes_list):
        for key, value in attr.items():
            new_key               = PREFIX_ATTR_ + i + ":" + key
            query_params[new_key] = value

    return query_params


def attributes_to_params(attributes, query_params=None):
    if not query_params:
        query_params = {}

    if attributes:
        for key, value in attributes:
            new_key               = PREFIX_ATTR + key
            query_params[new_key] = value

    return query_params


class HttpMethod(enum.Enum):
    GET    = "GET"
    PUT    = "PUT"
    POST   = "POST"
    DELETE = "DELETE"


class API:
    def __init__(self, path, method, expected_status, consumes=APPLICATION_JSON, produces=APPLICATION_JSON):
        self.path            = path
        self.method          = method
        self.expected_status = expected_status
        self.consumes        = consumes
        self.produces        = produces

    def format_path(self, params):
        return API(self.path.format_map(params), self.method, self.expected_status, self.consumes, self.produces)

    def format_path_with_params(self, *params):
        path = self.path

        for par in params:
            path += "/" + par

        return API(path, self.method, self.expected_status, self.consumes, self.produces)


class CustomEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, set):
            return list(o)

        return o.__dict__


class AtlasBaseModelObject:
    def __init__(self, guid=None):
        self.guid = guid if guid is not None else "-" + str(s_nextId)