#!/usr/bin/env python

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

import unittest

from unittest.mock import Mock, patch

from apache_atlas.exceptions import AtlasServiceException
from apache_atlas.model.enums import EntityOperation, EntityStatus
from apache_atlas.model.instance import AtlasClassification, AtlasEntity, AtlasEntityWithExtInfo
from apache_atlas.utils import API, HTTPMethod, HTTPStatus, type_coerce

try:
    from apache_atlas.client.base_client import AtlasClient
    from apache_atlas.client.entity import EntityClient
except ModuleNotFoundError:  # requests not installed
    exit()  # skipping unit tests


class MockResponse:
    def __init__(self, status_code, response=None, content=None):
        self.status_code = status_code
        self.response    = response
        self.content     = content

    def json(self):
        return self.response


class TestAtlasClient(unittest.TestCase):
    URL  = 'http://localhost:21000'
    AUTH = ('admin', 'atlasR0cks!')

    @patch('apache_atlas.client.base_client.Session')
    def test_get_entity_service_unavailable(self, mock_session):
        mock_session.return_value.request.return_value = MockResponse(HTTPStatus.SERVICE_UNAVAILABLE)

        with self.assertRaises(AtlasServiceException):
            AtlasClient(self.URL, self.AUTH).entity.get_entity_by_guid('guid-1')

    @patch('apache_atlas.client.base_client.Session')
    def test_get_entity_success(self, mock_session):
        response = {
            'entity': {
                'typeName': 'hive_db',
                'attributes': {'name': 'test_db', 'qualifiedName': 'test_db@prod'},
            },
        }
        mock_session.return_value.request.return_value = MockResponse(
            HTTPStatus.OK, response=response, content='Success')

        result = AtlasClient(self.URL, self.AUTH).entity.get_entity_by_guid('guid-1')

        self.assertIsInstance(result, AtlasEntityWithExtInfo)
        self.assertEqual('hive_db', result.entity.typeName)

    @patch('apache_atlas.client.base_client.Session')
    def test_get_entity_unexpected_status_code(self, mock_session):
        mock_response = Mock()
        mock_response.text        = 'Internal Server Error'
        mock_response.content     = 'Internal Server Error'
        mock_response.status_code = 500
        mock_session.return_value.request.return_value = mock_response

        with self.assertRaises(AtlasServiceException) as context:
            AtlasClient(self.URL, self.AUTH).entity.get_entity_by_guid('guid-1')

        self.assertIn('500', str(context.exception))

    def test_url_missing_format(self):
        with self.assertRaises(KeyError):
            API("{arg1}test{arg2}path{arg3}", HTTPMethod.GET, HTTPStatus.OK).format_path(
                {'arg1': 1, 'arg2': 2})

    def test_url_invalid_format(self):
        with self.assertRaises(TypeError):
            API("{}test{}path{}", HTTPMethod.GET, HTTPStatus.OK).format_path({'1', '2'})

    def test_multipart_urljoin(self):
        api = API("api/atlas/v2/entity/", HTTPMethod.GET, HTTPStatus.OK)
        path = api.multipart_urljoin("api/atlas/v2/entity/", "guid", "header")

        self.assertEqual("api/atlas/v2/entity/guid/header", path)


class TestAtlasModel(unittest.TestCase):
    def test_entity_type_coercion(self):
        entity = AtlasEntity({
            'typeName': 'hive_table',
            'attributes': {'name': 'test_tbl', 'qualifiedName': 'test_db.test_tbl@prod'},
            'classifications': [{
                'typeName': 'PII',
                'entityGuid': 'guid-1',
            }],
        })

        entity.type_coerce_attrs()

        self.assertIsInstance(entity.classifications[0], AtlasClassification)
        self.assertEqual('PII', entity.classifications[0].typeName)

    def test_type_coerce_from_dict(self):
        result = type_coerce(
            {'typeName': 'hive_db', 'attributes': {'name': 'test_db'}},
            AtlasEntity,
        )

        self.assertIsInstance(result, AtlasEntity)
        self.assertEqual('hive_db', result.typeName)

    def test_entity_operation_enum(self):
        self.assertEqual('CREATE', EntityOperation.CREATE.name)
        self.assertEqual('DELETE', EntityOperation.DELETE.name)

    def test_entity_status_enum(self):
        self.assertEqual(0, EntityStatus.ACTIVE.value)


class TestEntityClient(unittest.TestCase):
    def test_get_entity_by_guid_calls_expected_api(self):
        atlas_client = Mock()
        atlas_client.call_api.return_value = AtlasEntityWithExtInfo({
            'entity': {'typeName': 'hive_db', 'attributes': {'name': 'test_db'}},
        })

        result = EntityClient(atlas_client).get_entity_by_guid(
            'guid-1', min_ext_info=True, ignore_relationships=True)

        atlas_client.call_api.assert_called_once()
        call_args = atlas_client.call_api.call_args
        self.assertEqual(AtlasEntityWithExtInfo, call_args[0][1])
        query_params = call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get('query_params')
        self.assertEqual({'minExtInfo': True, 'ignoreRelationships': True}, query_params)
        self.assertIsInstance(result, AtlasEntityWithExtInfo)


if __name__ == '__main__':
    unittest.main()
