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


class AtlasLineageInfo:
    lineageDirection_enum = enum.Enum('lineageDirection_enum', 'INPUT OUTPUT BOTH', module=__name__)

    def __init__(self, baseEntityGuid=None, lineageDirection=None, lineageDepth=None, guidEntityMap=None, relations=None):
        self.baseEntityGuid   = baseEntityGuid
        self.lineageDirection = lineageDirection
        self.lineageDepth     = lineageDepth
        self.guidEntityMap    = guidEntityMap if guidEntityMap is not None else {}
        self.relations        = relations if relations is not None else set()


class LineageRelation:

    def __init__(self, fromEntityId=None, toEntityId=None, relationshipId=None):
        self.fromEntityId   = fromEntityId
        self.toEntityId     = toEntityId
        self.relationshipId = relationshipId