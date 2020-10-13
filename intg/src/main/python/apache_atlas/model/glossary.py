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

from apache_atlas.utils import AtlasBaseModelObject


class AtlasGlossaryBaseObject(AtlasBaseModelObject):

    def __init__(self, guid=None, qualifiedName=None, name=None, shortDescription=None,
                 longDescription=None, additionalAttributes=None, classifications=None):

        super().__init__(guid)

        self.qualifiedName        = qualifiedName
        self.name                 = name
        self.shortDescription     = shortDescription
        self.longDescription      = longDescription
        self.additionalAttributes = additionalAttributes if additionalAttributes is not None else {}
        self.classifications      = classifications if classifications is not None else []


class AtlasGlossary(AtlasGlossaryBaseObject):

    def __init__(self, guid=None, qualifiedName=None, name=None, shortDescription=None, longDescription=None,
                 additionalAttributes=None, classifications=None, language=None, usage=None, terms=None, categories=None):

        super().__init__(guid, qualifiedName, name, shortDescription, longDescription, additionalAttributes, classifications)

        self.language   = language
        self.usage      = usage
        self.terms      = terms if terms is not None else set()
        self.categories = categories if categories is not None else set()


class AtlasRelatedTermHeader:
    status_enum = enum.Enum('status_enum', 'DRAFT ACTIVE DEPRECATED OBSOLETE OTHER', module=__name__)

    def __init__(self, termGuid=None, relationGuid=None, displayText=None, description=None,
                 expression=None, steward=None, source=None, status=None):
        self.termGuid     = termGuid
        self.relationGuid = relationGuid
        self.displayText  = displayText
        self.description  = description
        self.expression   = expression
        self.steward      = steward
        self.source       = source
        self.status       = status


class AtlasRelatedCategoryHeader:

    def __init__(self, categoryGuid=None, parentCategoryGuid=None, relationGuid=None, displayText=None, description=None):
        self.categoryGuid       = categoryGuid
        self.parentCategoryGuid = parentCategoryGuid
        self.relationGuid       = relationGuid
        self.displayText        = displayText
        self.description        = description


class AtlasGlossaryExtInfo(AtlasGlossary):

    def __init__(self, guid=None, qualifiedName=None, name=None, shortDescription=None, longDescription=None, additionalAttributes=None,
                 classifications=None, language=None, usage=None, terms=None, categories=None, termInfo=None, categoryInfo=None):

        super().__init__(guid, qualifiedName, name, shortDescription, longDescription,
                         additionalAttributes, classifications, language, usage, terms, categories)

        self.termInfo     = termInfo if termInfo is not None else {}
        self.categoryInfo = categoryInfo if categoryInfo is not None else {}


class AtlasTermRelationshipStatus(enum.Enum):
    DRAFT      = 0
    ACTIVE     = 1
    DEPRECATED = 2
    OBSOLETE   = 3
    OTHER      = 99


class AtlasGlossaryTerm(AtlasGlossaryBaseObject):

    def __init__(self, guid=None, qualifiedName=None, name=None, shortDescription=None, longDescription=None,
                 additionalAttributes=None, classifications=None, examples=None, abbreviation=None, usage=None, anchor=None,
                 assignedEntities=None, categories=None, seeAlso=None, synonyms=None, antonyms=None, preferredTerms=None,
                 preferredToTerms=None, replacementTerms=None, replacedBy=None, translationTerms=None, translatedTerms=None,
                 isA=None, classifies=None, validValues=None, validValuesFor=None):

        super().__init__(guid, qualifiedName, name, shortDescription, longDescription, additionalAttributes, classifications)

        # Core attributes
        self.examples     = examples if examples is not None else []
        self.abbreviation = abbreviation
        self.usage        = usage

        # Attributes derived from relationships
        self.anchor           = anchor
        self.assignedEntities = assignedEntities if assignedEntities is not None else set()
        self.categories       = categories if categories is not None else set()

        # Related Terms
        self.seeAlso = seeAlso if seeAlso is not None else set()

        # Term Synonyms
        self.synonyms = synonyms if synonyms is not None else set()

        # Term antonyms
        self.antonyms = antonyms if antonyms is not None else set()

        # Term preference
        self.preferredTerms   = preferredTerms if preferredTerms is not None else set()
        self.preferredToTerms = preferredToTerms if preferredToTerms is not None else set()

        # Term replacements
        self.replacementTerms = replacementTerms if replacementTerms is not None else set()
        self.replacedBy       = replacedBy if replacedBy is not None else set()

        # Term translations
        self.translationTerms = translationTerms if translationTerms is not None else set()
        self.translatedTerms  = translatedTerms if translatedTerms is not None else set()

        # Term classification
        self.isA        = isA if isA is not None else set()
        self.classifies = classifies if classifies is not None else set()

        # Values for terms
        self.validValues    = validValues if validValues is not None else set()
        self.validValuesFor = validValuesFor if validValuesFor is not None else set()


class AtlasGlossaryHeader:

    def __init__(self, glossaryGuid=None, relationGuid=None, displayText=None):
        self.glossaryGuid = glossaryGuid if glossaryGuid is not None else ""
        self.relationGuid = relationGuid
        self.displayText  = displayText


class AtlasObjectId:

    def __init__(self, guid=None, typeName=None, uniqueAttributes=None):
        self.guid             = guid if guid is not None else ""
        self.typeName         = typeName
        self.uniqueAttributes = uniqueAttributes if uniqueAttributes is not None else {}


class AtlasRelatedObjectId(AtlasObjectId):
    entityStatus_enum       = enum.Enum('entityStatus_enum', 'ACTIVE DELETED PURGED', module=__name__)
    relationshipStatus_enum = enum.Enum('relationshipStatus_enum', 'ACTIVE DELETED', module=__name__)

    def __init__(self, guid=None, typeName=None, uniqueAttributes=None, entityStatus=None, displayText=None,
                 relationshipType=None, relationshipGuid=None, relationshipStatus=None, relationshipAttributes=None):

        super().__init__(guid, typeName, uniqueAttributes)

        self.entityStatus           = entityStatus
        self.displayText            = displayText
        self.relationshipType       = relationshipType
        self.relationshipGuid       = relationshipGuid
        self.relationshipStatus     = relationshipStatus
        self.relationshipAttributes = relationshipAttributes


class AtlasTermCategorizationHeader:
    status_enum = enum.Enum('status_enum', 'DRAFT ACTIVE DEPRECATED OBSOLETE OTHER', module=__name__)

    def __init__(self, categoryGuid=None, relationGuid=None, description=None, displayText=None, status=None):
        self.categoryGuid = categoryGuid if categoryGuid is not None else ""
        self.relationGuid = relationGuid
        self.description  = description
        self.displayText  = displayText
        self.status       = status


class AtlasGlossaryCategory(AtlasGlossaryBaseObject):

    def __init__(self, guid=None, qualifiedName=None, name=None, shortDescription=None, longDescription=None,
                 additionalAttributes=None, classifications=None, anchor=None, parentCategory=None, childrenCategories=None, terms=None):

        super().__init__(guid, qualifiedName, name, shortDescription, longDescription, additionalAttributes, classifications)

        # Inherited attributes from relations
        self.anchor = anchor

        # Category hierarchy links
        self.parentCategory     = parentCategory
        self.childrenCategories = childrenCategories

        # Terms associated with this category
        self.terms = terms if terms is not None else set()