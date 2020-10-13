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

import json
import logging

from apache_atlas.model.entity import AtlasEntityWithExtInfo, EntityMutations

LOG = logging.getLogger('entity-example')


class EntityExample:
    DATABASE_NAME                = "employee_db_entity"
    TABLE_NAME                   = "employee_table_entity"
    PROCESS_NAME                 = "employee_process_entity"
    METADATA_NAMESPACE_SUFFIX    = "@cl1"
    MANAGED_TABLE                = "Managed"
    ATTR_NAME                    = "name"
    ATTR_DESCRIPTION             = "description"
    ATTR_QUALIFIED_NAME          = "qualifiedName"
    REFERENCEABLE_ATTRIBUTE_NAME = ATTR_QUALIFIED_NAME
    ATTR_TIME_ID_COLUMN          = "time_id"
    ATTR_CUSTOMER_ID_COLUMN      = "customer_id"
    ATTR_COMPANY_ID_COLUMN       = "company_id"

    def __init__(self, client):
        self.client              = client
        self.db_entity           = None
        self.entity_table_us     = None
        self.entity_table_canada = None
        self.load_process        = None

    def create_entities(self):
        self.__create_db()
        self.__create_us_table()
        self.__create_canada_table()
        self.__create_process()

    def get_table_entity(self):
        if self.entity_table_us:
            return self.entity_table_us

        return None

    def get_entity_by_guid(self, entity_guid):
        entity_with_ext_info = self.client.entity.get_entity_by_guid(entity_guid)

        if entity_with_ext_info:
            LOG.info("Entity info is : %s", entity_with_ext_info)

    def remove_entities(self):
        delete_entity_list = [self.load_process['guid'], self.entity_table_us['guid'], self.entity_table_canada['guid'], self.db_entity['guid']]
        response           = self.client.entity.purge_entities_by_guids(delete_entity_list)

        if not response:
            LOG.info("There is no entity to delete!")

        LOG.info("Deletion complete for DB entity: %s, US entity: %s, Canada entity: %s and Process entity: %s",
                 self.db_entity['typeName'],
                 self.entity_table_us['typeName'],
                 self.entity_table_canada['typeName'],
                 self.load_process['typeName'])

    def __create_db(self):
        if not self.db_entity:
            with open('request_json/entity_create_db.json') as f:
                entity_db      = json.load(f)
                self.db_entity = self.__create_db_helper(entity_db)

        if self.db_entity:
            LOG.info("Created database entity: %s", self.db_entity['typeName'])
        else:
            LOG.info("Database entity not created")

    def __create_db_helper(self, ext_info):
        instance_entity = ext_info['entity']
        entity          = self.__create_entity(ext_info)

        if entity and entity['guid']:
            instance_entity['guid'] = entity['guid']

        return instance_entity

    def __create_us_table(self):
        if not self.entity_table_us:
            with open('request_json/entity_create_table_us.json') as f:
                entity_table_us      = json.load(f)
                self.entity_table_us = self.__create_table_helper(entity_table_us)

        if self.entity_table_us:
            LOG.info("Created table entity for US: %s", self.entity_table_us['typeName'])
        else:
            LOG.info("Table entity for US not created")

    def __create_canada_table(self):
        if not self.entity_table_canada:
            with open('request_json/entity_create_table_canada.json') as f:
                entity_table_canada      = json.load(f)
                self.entity_table_canada = self.__create_table_helper(entity_table_canada)

        if self.entity_table_canada:
            LOG.info("Created table entity for Canada: %s", self.entity_table_canada['typeName'])
        else:
            LOG.info("Table entity for Canada not created")

    def __create_table_helper(self, ext_info):
        instance_entity = ext_info['entity']

        if self.db_entity:
            instance_entity['relationshipAttributes']['db']['guid']     = self.db_entity['guid']
            instance_entity['relationshipAttributes']['db']['typeName'] = self.db_entity['typeName']

        ext_info['entity'] = instance_entity

        entity = self.__create_entity(ext_info)

        if entity and entity['guid']:
            instance_entity['guid'] = entity['guid']

        return instance_entity

    def __create_process(self):
        if not self.load_process:
            with open('request_json/entity_create_process.json') as f:
                entity_process    = json.load(f)
                self.load_process = self.__create_process_helper(entity_process)

        if self.load_process:
            LOG.info("Created process Entity: %s", self.load_process['typeName'])
        else:
            LOG.info("Process Entity not created")

    def __create_process_helper(self, ext_info):
        instance_entity = ext_info['entity']

        if self.entity_table_us:
            input_list = []
            input_data = {'guid': self.entity_table_us['guid'],
                          'typeName': self.entity_table_us['typeName']
                          }

            input_list.append(input_data)

            instance_entity['relationshipAttributes']['inputs'] = input_list

        if self.entity_table_canada:
            output_list = []
            output_data = {'guid': self.entity_table_canada['guid'],
                           'typeName': self.entity_table_canada['typeName']
                           }

            output_list.append(output_data)

            instance_entity['relationshipAttributes']['outputs'] = output_list

        ext_info['entity'] = instance_entity

        return self.__create_entity(ext_info)

    def __create_entity(self, ext_info):
        try:
            entity      = self.client.entity.create_entity(ext_info)
            create_enum = EntityMutations.entity_operation_enum.CREATE.name

            if entity and entity.mutatedEntities and entity.mutatedEntities[create_enum]:
                header_list = entity.mutatedEntities[create_enum]

                if len(header_list) > 0:
                    return header_list[0]
        except Exception as e:
            LOG.exception("failed to create entity %s", ext_info['entity'])

        return None