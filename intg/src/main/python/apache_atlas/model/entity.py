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

from apache_atlas.utils import s_nextId


class AtlasStruct:

    def __init__(self, typeName=None, attributes=None):
        self.typeName   = typeName
        self.attributes = attributes if attributes is not None else {}


class AtlasEntity(AtlasStruct):
    status_enum = enum.Enum('status_enum', 'ACTIVE DELETED PURGED', module=__name__)

    def __init__(self, typeName=None, attributes=None, guid=None, homeId=None, isIncomplete=None, provenanceType=None,
                 status=None, createdBy=None, updatedBy=None, createTime=None, updateTime=None, version=None,
                 relationshipAttributes=None, classifications=None, meanings=None, customAttributes=None,
                 businessAttributes=None, labels=None, proxy=None):

        super().__init__(typeName, attributes)

        self.guid                   = guid if guid is not None else "-" + str(s_nextId)
        self.homeId                 = homeId
        self.isIncomplete           = isIncomplete
        self.provenanceType         = provenanceType
        self.status                 = status
        self.createdBy              = createdBy
        self.updatedBy              = updatedBy
        self.createTime             = createTime
        self.updateTime             = updateTime
        self.version                = version
        self.relationshipAttributes = relationshipAttributes if relationshipAttributes is not None else {}
        self.classifications        = classifications if classifications is not None else []
        self.meanings               = meanings if meanings is not None else []
        self.customAttributes       = customAttributes if customAttributes is not None else {}
        self.businessAttributes     = businessAttributes if businessAttributes is not None else {}
        self.labels                 = labels if labels is not None else set()
        self.proxy                  = proxy


class AtlasEntityExtInfo:

    def __init__(self, referredEntities=None):

        self.referredEntities = referredEntities if referredEntities is not None else {}


class AtlasEntityWithExtInfo(AtlasEntityExtInfo):

    def __init__(self, referredEntities=None, entity=None):
        super().__init__(referredEntities)

        self.entity = entity


class AtlasEntitiesWithExtInfo(AtlasEntityExtInfo):

    def __init__(self, referredEntities=None, entities=None):
        super().__init__(referredEntities)

        self.entities = entities if entities is not None else {}


class AtlasEntityHeader(AtlasStruct):
    status_enum = enum.Enum('status_enum', 'ACTIVE DELETED PURGED', module=__name__)

    def __init__(self, typeName=None, attributes=None, guid=None, status=None, displayText=None, classificationNames=None,
                 classifications=None, meaningNames=None, meanings=None, isIncomplete=None, labels=None):
        super().__init__(typeName, attributes)

        self.guid                = guid if guid is not None else "-" + str(s_nextId)
        self.status              = status
        self.displayText         = displayText
        self.classificationNames = classificationNames if classificationNames is not None else []
        self.classifications     = classifications
        self.meaningNames        = meaningNames
        self.meanings            = meanings if meanings is not None else []
        self.isIncomplete        = isIncomplete
        self.labels              = labels if labels is not None else set()


class AtlasClassification(AtlasStruct):
    entityStatus_enum = enum.Enum('entityStatus_enum', 'ACTIVE DELETED PURGED', module=__name__)

    def __init__(self, typeName=None, attributes=None, entityGuid=None, entityStatus=None, propagate=None,
                 validityPeriods=None, removePropagationsOnEntityDelete=None):
        self.typeName                         = typeName
        self.attributes                       = attributes
        self.entityGuid                       = entityGuid
        self.entityStatus                     = entityStatus
        self.propagate                        = propagate
        self.validityPeriods                  = validityPeriods if validityPeriods is not None else []
        self.removePropagationsOnEntityDelete = removePropagationsOnEntityDelete


class TimeBoundary:

    def __init__(self, startTime=None, endTime=None, timeZone=None):
        self.startTime = startTime
        self.endTime   = endTime
        self.timeZone  = timeZone


class Plist:
    sortType_enum = enum.Enum('sortType_enum', 'NONE ASC DESC', module=__name__)

    def __init__(self, list=None, startIndex=None, pageSize=None, totalCount=None, sortBy=None, sortType=None):
        self.list       = list
        self.startIndex = startIndex
        self.pageSize   = pageSize
        self.totalCount = totalCount
        self.sortBy     = sortBy
        self.sortType   = sortType


class AtlasClassifications(Plist):
    sortType_enum = enum.Enum('sortType_enum', 'NONE ASC DESC', module=__name__)

    def __init__(self, list=None, startIndex=None, pageSize=None, totalCount=None, sortBy=None, sortType=None):
        super().__init__(list, startIndex, pageSize, totalCount, sortBy, sortType)


class AtlasEntityHeaders:

    def __init__(self, guidHeaderMap=None):
        self.guidHeaderMap = guidHeaderMap if guidHeaderMap is not None else {}


class EntityMutationResponse:

    def __init__(self, mutatedEntities=None, guidAssignments=None):
        self.mutatedEntities = mutatedEntities if mutatedEntities is not None else {}
        self.guidAssignments = guidAssignments if guidAssignments is not None else {}


class EntityMutations:
    entity_operation_enum = enum.Enum('entity_operation_enum', 'CREATE UPDATE PARTIAL_UPDATE DELETE PURGE', module=__name__)

    def __init__(self, entity_mutations):
        self.entity_mutations = entity_mutations if entity_mutations is not None else []


class EntityMutation:

    def __init__(self, op, entity):
        self.op     = op
        self.entity = entity