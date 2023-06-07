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
import copy
import json
import logging
from urllib.parse import urljoin

from apache_atlas.client.admin import AdminClient
from apache_atlas.client.discovery import DiscoveryClient
from apache_atlas.client.entity import EntityClient
from apache_atlas.client.glossary import GlossaryClient
from apache_atlas.client.lineage import LineageClient
from apache_atlas.client.relationship import RelationshipClient
from apache_atlas.client.typedef import TypeDefClient
from apache_atlas.exceptions import AtlasServiceException
from apache_atlas.utils import HTTPStatus, type_coerce
from requests import Session

log = logging.getLogger('apache_atlas')


class AtlasClient:
    def __init__(self, host, auth):
        session = Session()
        session.auth = auth

        self.host = host.rstrip('/')
        self.session = session
        self.request_params = {'headers': {}}
        self.typedef = TypeDefClient(self)
        self.entity = EntityClient(self)
        self.lineage = LineageClient(self)
        self.discovery = DiscoveryClient(self)
        self.glossary = GlossaryClient(self)
        self.relationship = RelationshipClient(self)
        self.admin = AdminClient(self)

        logging.getLogger("requests").setLevel(logging.WARNING)

    def call_api(self, api, response_type=None, query_params=None, request_obj=None):
        params = copy.deepcopy(self.request_params)
        path = urljoin(self.host, api.path.lstrip('/'))

        params['headers']['Accept'] = api.consumes
        params['headers']['Content-type'] = api.produces

        if query_params is not None:
            params['params'] = query_params

        if request_obj is not None:
            params['data'] = json.dumps(request_obj)

        log.debug("------------------------------------------------------")
        log.debug("Call         : %s %s", api.method, path)
        log.debug("Content-type : %s", api.consumes)
        log.debug("Accept       : %s", api.produces)

        response = self.session.request(api.method.value, path, **params)
        log.debug("HTTP Status: %s", response.status_code)

        if response.status_code == api.expected_status:
            if response_type is None:
                return None
                
            if response.content is None:
                return None

            log.debug("<== __call_api(%s,%s,%s), result = %s", vars(api), params, request_obj, response)
            try:
                log.debug(response.json())

                if response_type == str:
                    return json.dumps(response.json())

                return type_coerce(response.json(), response_type)
            except Exception as e:
                log.exception("Exception occurred while parsing response")
                raise AtlasServiceException(api, response) from e

        if response.status_code == HTTPStatus.SERVICE_UNAVAILABLE:
            log.error("Atlas Service unavailable. HTTP Status: %s", HTTPStatus.SERVICE_UNAVAILABLE)

        raise AtlasServiceException(api, response)
