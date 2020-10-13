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
# Unless required by applicabwle law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import enum
import sys

from apache_atlas.utils import AtlasBaseModelObject


class SearchFilter:
    sortType_enum = enum.Enum('sortType_enum', 'NONE ASC DESC', module=__name__)

    def __init__(self, params=None, startIndex=0, maxRows=sys.maxsize, getCount=True, sortBy=None, sortType=None):
        self.params     = params if params is not None else {}
        self.startIndex = startIndex
        self.maxRows    = maxRows
        self.getCount   = getCount
        self.sortBy     = sortBy
        self.sortType   = sortType


class AtlasSearchResult:
    queryType_enum = enum.Enum('queryType_enum', 'DSL FULL_TEXT GREMLIN BASIC ATTRIBUTE RELATIONSHIP', module=__name__)

    def __init__(self, queryType=None, searchParameters=None, queryText=None, type=None, classification=None,
                 entities=None, attributes=None, fullTextResult=None, referredEntities=None, approximateCount=None):
        self.queryType        = queryType if queryType is not None else AtlasSearchResult.queryType_enum.BASIC.name
        self.searchParameters = searchParameters
        self.queryText        = queryText
        self.type             = type
        self.classification   = classification
        self.entities         = entities
        self.attributes       = attributes
        self.fullTextResult   = fullTextResult if fullTextResult is not None else []
        self.referredEntities = referredEntities if fullTextResult is not None else {}
        self.approximateCount = approximateCount


class SearchParameters:
    sortOrder_enum = enum.Enum('sortOrder_enum', 'ASCENDING DESCENDING', module=__name__)

    def __init__(self, query=None, typeName=None, classification=None, termName=None, sortBy=None, excludeDeletedEntities=None,
                 includeClassificationAttributes=None, includeSubTypes=None, includeSubClassifications=None, limit=None,
                 offset=None, entityFilters=None, tagFilters=None, attributes=None, sortOrder=None):
        self.query                           = query
        self.typeName                        = typeName
        self.classification                  = classification
        self.termName                        = termName
        self.sortBy                          = sortBy
        self.excludeDeletedEntities          = excludeDeletedEntities
        self.includeClassificationAttributes = includeClassificationAttributes
        self.includeSubTypes                 = includeSubTypes
        self.includeSubClassifications       = includeSubClassifications
        self.limit                           = limit
        self.offset                          = offset
        self.entityFilters                   = entityFilters
        self.tagFilters                      = tagFilters
        self.attributes                      = attributes if attributes is not None else set()
        self.sortOrder                       = sortOrder


class FilterCriteria:
    operator_enum = enum.Enum('operator_enum',
                              '< > <= >= = != in like startsWith endsWith contains not_contains containsAny containsAll isNull notNull',
                              module=__name__)
    condition_enum = enum.Enum('condition_enum', 'AND OR', module=__name__)

    def __init__(self, attributeName=None, operator=None, attributeValue=None, condition=None, criterion=None):
        self.attributeName  = attributeName
        self.operator       = operator
        self.attributeValue = attributeValue
        self.condition      = condition
        self.criterion      = criterion if criterion is not None else []


class Operator(enum.Enum):
    LT = ("<", "lt")
    GT = ('>', 'gt')
    LTE = ('<=', 'lte')
    GTE = ('>=', 'gte')
    EQ = ('=', 'eq')
    NEQ = ('!=', 'neq')
    IN = ('in', 'IN')
    LIKE = ('like', 'LIKE')
    STARTS_WITH = ('startsWith', 'STARTSWITH', 'begins_with', 'BEGINS_WITH')
    ENDS_WITH = ('endsWith', 'ENDSWITH', 'ends_with', 'ENDS_WITH')
    CONTAINS = ('contains', 'CONTAINS')
    NOT_CONTAINS = ('not_contains', 'NOT_CONTAINS')
    CONTAINS_ANY = ('containsAny', 'CONTAINSANY', 'contains_any', 'CONTAINS_ANY')
    CONTAINS_ALL = ('containsAll', 'CONTAINSALL', 'contains_all', 'CONTAINS_ALL')
    IS_NULL = ('isNull', 'ISNULL', 'is_null', 'IS_NULL')
    NOT_NULL = ('notNull', 'NOTNULL', 'not_null', 'NOT_NULL')


class SortOrder(enum.Enum):
    sort_order = enum.Enum('sort_order', 'ASCENDING DESCENDING', module=__name__)


class AttributeSearchResult:

    def __init__(self, name=None, values=None):
        self.name   = name
        self.values = values if values is not None else []


class AtlasFullTextResult:

    def __init__(self, entity=None, score=None):
        self.entity = entity
        self.score  = score


class AtlasQuickSearchResult:

    def __init__(self, searchResults=None, aggregationMetrics=None):
        self.searchResults      = searchResults
        self.aggregationMetrics = aggregationMetrics if aggregationMetrics is not None else {}


class AtlasAggregationEntry:

    def __init__(self, name=None, count=None):
        self.name  = name
        self.count = count


class QuickSearchParameters:

    def __init__(self, query=None, typeName=None, entityFilters=None, includeSubTypes=None,
                 excludeDeletedEntities=None, offset=None, limit=None, attributes=None):
        self.query                  = query
        self.typeName               = typeName
        self.entityFilters          = entityFilters
        self.includeSubTypes        = includeSubTypes
        self.excludeDeletedEntities = excludeDeletedEntities
        self.offset                 = offset
        self.limit                  = limit
        self.attributes             = attributes if attributes is not None else set()


class AtlasSuggestionsResult:

    def __init__(self, suggestions=None, prefixString=None, fieldName=None):
        self.suggestions  = suggestions if suggestions is not None else []
        self.prefixString = prefixString
        self.fieldName    = fieldName


class AtlasUserSavedSearch(AtlasBaseModelObject):
    saved_search_type_enum = enum.Enum('saved_search_type_enum', 'BASIC ADVANCED', module=__name__)

    def __init__(self, guid=None, ownerName=None, name=None, searchType=None, searchParameters=None, uiParameters=None):
        super().__init__(guid)

        self.ownerName        = ownerName
        self.name             = name
        self.searchType       = searchType
        self.searchParameters = searchParameters
        self.uiParameters     = uiParameters