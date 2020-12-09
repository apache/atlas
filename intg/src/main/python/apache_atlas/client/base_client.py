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
import os
import json
import logging

from requests                         import Session
from apache_atlas.client.discovery    import DiscoveryClient
from apache_atlas.client.entity       import EntityClient
from apache_atlas.client.glossary     import GlossaryClient
from apache_atlas.client.lineage      import LineageClient
from apache_atlas.client.relationship import RelationshipClient
from apache_atlas.client.typedef      import TypeDefClient
from apache_atlas.exceptions          import AtlasServiceException
from apache_atlas.utils               import HttpMethod, HTTPStatus, type_coerce


LOG = logging.getLogger('apache_atlas')


class AtlasClient:
    def __init__(self, host, auth):
        session      = Session()
        session.auth = auth

        self.host           = host
        self.session        = session
        self.request_params = {'headers': {}}
        self.typedef        = TypeDefClient(self)
        self.entity         = EntityClient(self)
        self.lineage        = LineageClient(self)
        self.discovery      = DiscoveryClient(self)
        self.glossary       = GlossaryClient(self)
        self.relationship   = RelationshipClient(self)

        logging.getLogger("requests").setLevel(logging.WARNING)


    def call_api(self, api, response_type=None, query_params=None, request_obj=None):
        params = copy.deepcopy(self.request_params)
        path   = os.path.join(self.host, api.path)

        params['headers']['Accept']       = api.consumes
        params['headers']['Content-type'] = api.produces

        if query_params:
            params['params'] = query_params

        if request_obj:
            params['data'] = json.dumps(request_obj)

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("------------------------------------------------------")
            LOG.debug("Call         : %s %s", api.method, path)
            LOG.debug("Content-type : %s", api.consumes)
            LOG.debug("Accept       : %s", api.produces)

        response = None

        if api.method == HttpMethod.GET:
            response = self.session.get(path, **params)
        elif api.method == HttpMethod.POST:
            response = self.session.post(path, **params)
        elif api.method == HttpMethod.PUT:
            response = self.session.put(path, **params)
        elif api.method == HttpMethod.DELETE:
            response = self.session.delete(path, **params)

        if response is not None:
            LOG.debug("HTTP Status: %s", response.status_code)

        if response is None:
            return None
        elif response.status_code == api.expected_status:
            if not response_type:
                return None

            try:
                if response.content:
                    if LOG.isEnabledFor(logging.DEBUG):
                        LOG.debug("<== __call_api(%s,%s,%s), result = %s", vars(api), params, request_obj, response)

                        LOG.debug(response.json())
                    if response_type == str:
                        return json.dumps(response.json())

                    return type_coerce(response.json(), response_type)
                else:
                    return None
            except Exception as e:
                print(e)

                LOG.exception("Exception occurred while parsing response with msg: %s", e)

                raise AtlasServiceException(api, response)
        elif response.status_code == HTTPStatus.SERVICE_UNAVAILABLE:
            LOG.error("Atlas Service unavailable. HTTP Status: %s", HTTPStatus.SERVICE_UNAVAILABLE)

            return None
        else:
            raise AtlasServiceException(api, response)
