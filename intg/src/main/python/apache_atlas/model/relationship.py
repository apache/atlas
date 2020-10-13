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

from apache_atlas.model.entity import AtlasStruct


class AtlasRelationship(AtlasStruct):

    propagateTags_enum = enum.Enum('propagateTags_enum', 'NONE ONE_TO_TWO TWO_TO_ONE BOTH', module=__name__)
    status_enum        = enum.Enum('status_enum', 'ACTIVE DELETED', module=__name__)

    def __init__(self, typeName=None, attributes=None, guid=None, homeId=None, provenanceType=None, end1=None, end2=None,
                 label=None, propagateTags=None, status=None, createdBy=None, updatedBy=None, createTime=None, updateTime=None,
                 version=0, propagatedClassifications=None, blockedPropagatedClassifications=None):

        super().__init__(typeName, attributes)

        self.guid                             = guid
        self.homeId                           = homeId
        self.provenanceType                   = provenanceType
        self.end1                             = end1
        self.end2                             = end2
        self.label                            = label
        self.propagateTags                    = propagateTags if propagateTags is not None else AtlasRelationship.propagateTags_enum.NONE.name
        self.status                           = status if status is not None else AtlasRelationship.status_enum.ACTIVE.name
        self.createdBy                        = createdBy
        self.updatedBy                        = updatedBy
        self.createTime                       = createTime
        self.updateTime                       = updateTime
        self.version                          = version
        self.propagatedClassifications        = propagatedClassifications if propagatedClassifications is not None else set()
        self.blockedPropagatedClassifications = blockedPropagatedClassifications if blockedPropagatedClassifications is not None else set()


class AtlasRelationshipWithExtInfo:

    def __init__(self, relationship=None, referredEntities=None):
        self.relationship     = relationship
        self.referredEntities = referredEntities if referredEntities is not None else {}
