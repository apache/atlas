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


class AtlasTypesDef:

    def __init__(self, enumDefs=None, structDefs=None, classificationDefs=None,
                 entityDefs=None, relationshipDefs=None, businessMetadataDefs=None):
        self.enumDefs             = enumDefs if enumDefs is not None else []
        self.structDefs           = structDefs if structDefs is not None else []
        self.classificationDefs   = classificationDefs if classificationDefs is not None else []
        self.entityDefs           = entityDefs if entityDefs is not None else []
        self.relationshipDefs     = relationshipDefs if relationshipDefs is not None else []
        self.businessMetadataDefs = businessMetadataDefs if businessMetadataDefs is not None else []


class AtlasBaseTypeDef:

    def __init__(self, category=None, guid=None, createdBy=None, updatedBy=None, createTime=None, updateTime=None,
                 version=None, name=None, description=None, typeVersion=None, serviceType=None, options=None):
        self.category    = category
        self.guid        = guid
        self.createdBy   = createdBy
        self.updatedBy   = updatedBy
        self.createTime  = createTime
        self.updateTime  = updateTime
        self.version     = version
        self.name        = name
        self.description = description
        self.typeVersion = typeVersion
        self.serviceType = serviceType
        self.options     = options if options is not None else {}


category_enum = enum.Enum('category_enum', 'PRIMITIVE OBJECT_ID_TYPE ENUM STRUCT CLASSIFICATION ENTITY ARRAY MAP RELATIONSHIP BUSINESS_METADATA', module=__name__)


class AtlasEnumDef(AtlasBaseTypeDef):

    def __init__(self, category=category_enum.ENUM.name, guid=None, createdBy=None, updatedBy=None, createTime=None,
                 updateTime=None, version=None, name=None, description=None, typeVersion=None,
                 serviceType=None, options=None, elementDefs=None, defaultValue=None):

        super().__init__(category, guid, createdBy, updatedBy, createTime, updateTime, version, name, description, typeVersion, serviceType, options)

        self.elementDefs  = elementDefs if elementDefs is not None else []
        self.defaultValue = defaultValue


class AtlasEnumElementDef:

    def __init__(self, value=None, description=None, ordinal=None):
        self.value       = value
        self.description = description
        self.ordinal     = ordinal


class AtlasStructDef(AtlasBaseTypeDef):

    def __init__(self, category=category_enum.STRUCT.name, guid=None, createdBy=None, updatedBy=None, createTime=None, updateTime=None, version=None,
                 name=None, description=None, typeVersion=None, serviceType=None, options=None, attributeDefs=None):

        super().__init__(category, guid, createdBy, updatedBy, createTime, updateTime, version, name, description, typeVersion, serviceType, options)

        self.attributeDefs = attributeDefs if attributeDefs is not None else []


class AtlasAttributeDef:
    cardinality_enum = enum.Enum('cardinality_enum', 'SINGLE LIST SET', module=__name__)
    indexType_enum   = enum.Enum('indexType_enum', 'DEFAULT STRING', module=__name__)

    def __init__(self, name=None, typeName=None, isOptional=None, cardinality=None, valuesMinCount=None, valuesMaxCount=None,
                 isUnique=None, isIndexable=None, includeInNotification=None, defaultValue=None, description=None, searchWeight= -1,
                 indexType=None, constraints=None, options=None, displayName=None):

        self.name                  = name
        self.typeName              = typeName
        self.isOptional            = isOptional
        self.cardinality           = cardinality
        self.valuesMinCount        = valuesMinCount
        self.valuesMaxCount        = valuesMaxCount
        self.isUnique              = isUnique
        self.isIndexable           = isIndexable
        self.includeInNotification = includeInNotification
        self.defaultValue          = defaultValue
        self.description           = description
        self.searchWeight          = searchWeight
        self.indexType             = indexType
        self.constraints           = constraints if constraints is not None else []
        self.options               = options if options is not None else {}
        self.displayName           = displayName


class AtlasConstraintDef:

    def __init__(self, type=None, params=None):
        self.type   = type
        self.params = params if params is not None else {}


class AtlasClassificationDef(AtlasStructDef):

    def __init__(self, category=category_enum.CLASSIFICATION.name, guid=None, createdBy=None, updatedBy=None, createTime=None, updateTime=None,
                 version=None, name=None, description=None, typeVersion=None, serviceType=None, options=None,
                 attributeDefs=None, superTypes=None, entityTypes=None, subTypes=None):

        super().__init__(category, guid, createdBy, updatedBy, createTime, updateTime, version,
                         name, description, typeVersion, serviceType, options, attributeDefs)

        self.superTypes  = superTypes if superTypes is not None else set()
        self.entityTypes = entityTypes if entityTypes is not None else set()
        self.subTypes    = subTypes if subTypes is not None else set()


class AtlasEntityDef(AtlasStructDef):
    def __init__(self, category=category_enum.CLASSIFICATION.name, guid=None, createdBy=None, updatedBy=None, createTime=None, updateTime=None,
                 version=None, name=None, description=None, typeVersion=None, serviceType=None, options=None,
                 attributeDefs=None, superTypes=None, subTypes=None, relationshipAttributeDefs=None, businessAttributeDefs=None):

        super().__init__(category, guid, createdBy, updatedBy, createTime, updateTime, version,
                         name, description, typeVersion, serviceType, options, attributeDefs)

        self.superTypes                = superTypes if superTypes is not None else set()
        self.subTypes                  = subTypes if subTypes is not None else set()
        self.relationshipAttributeDefs = relationshipAttributeDefs if relationshipAttributeDefs is not None else []
        self.businessAttributeDefs     = businessAttributeDefs if businessAttributeDefs is not None else {}


class AtlasRelationshipAttributeDef(AtlasAttributeDef):
    cardinality_enum = enum.Enum('cardinality_enum', 'SINGLE LIST SET', module=__name__)
    indexType_enum   = enum.Enum('indexType_enum', 'DEFAULT STRING', module=__name__)

    def __init__(self, name=None, typeName=None, isOptional=None, cardinality=None, valuesMinCount=None,
                 valuesMaxCount=None, isUnique=None, isIndexable=None, includeInNotification=None,
                 defaultValue=None, description=None, searchWeight=None, indexType=None, constraints=None,
                 options=None, displayName=None, relationshipTypeName=None, isLegacyAttribute=None):

        super().__init__(name, typeName, isOptional, cardinality, valuesMinCount, valuesMaxCount, isUnique, isIndexable,
                         includeInNotification, defaultValue, description, searchWeight, indexType, constraints, options, displayName)

        self.relationshipTypeName = relationshipTypeName
        self.isLegacyAttribute    = isLegacyAttribute


class AtlasBusinessMetadataDef(AtlasStructDef):

    def __init__(self, category=category_enum.BUSINESS_METADATA.name, guid=None, createdBy=None, updatedBy=None, createTime=None,
                 updateTime=None, version=None, name=None, description=None, typeVersion=None,
                 serviceType=None, options=None, attributeDefs=None):

        super().__init__(category, guid, createdBy, updatedBy, createTime, updateTime, version,
                         name, description, typeVersion, serviceType, options, attributeDefs)


class AtlasRelationshipDef(AtlasStructDef):
    relationshipCategory_enum = enum.Enum('relationshipCategory_enum', 'ASSOCIATION AGGREGATION COMPOSITION', module=__name__)
    propagateTags_enum        = enum.Enum('propagateTags_enum', 'NONE ONE_TO_TWO TWO_TO_ONE BOTH', module=__name__)

    def __init__(self, category=category_enum.RELATIONSHIP.name, guid=None, createdBy=None, updatedBy=None, createTime=None,
                 updateTime=None, version=None, name=None, description=None, typeVersion=None,
                 serviceType=None, options=None, attributeDefs=None, relationshipCategory=None,
                 relationshipLabel=None, propagateTags=None, endDef1=None, endDef2=None):

        super().__init__(category, guid, createdBy, updatedBy, createTime, updateTime, version,
                         name, description, typeVersion, serviceType, options, attributeDefs)

        self.relationshipCategory = relationshipCategory
        self.relationshipLabel    = relationshipLabel
        self.propagateTags        = propagateTags
        self.endDef1              = endDef1
        self.endDef2              = endDef2


class AtlasRelationshipEndDef:
    cardinality_enum = enum.Enum('cardinality_enum', 'SINGLE LIST SET', module=__name__)

    def __init__(self, type=None, name=None, isContainer=None, cardinality=None, isLegacyAttribute=None, description=None):
        self.type              = type
        self.name              = name
        self.isContainer       = isContainer
        self.cardinality       = cardinality if cardinality is not None else AtlasRelationshipEndDef.cardinality_enum.SINGLE.name
        self.isLegacyAttribute = isLegacyAttribute
        self.description       = description