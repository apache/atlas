/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * When entity GET uses ignoreRelationships=true, relationshipAttributes.meanings
 * is omitted. GET /v2/entity/guid/{guid}/header still returns meanings
 * (AtlasTermAssignmentHeader[]). Map them to the shape used by entity detail
 * (guid, relationshipGuid for remove / ShowMoreView).
 */
export const mapHeaderMeaningsToRelationshipMeanings = (
  meanings: unknown
): any[] => {
  if (!Array.isArray(meanings) || meanings.length === 0) {
    return [];
  }
  return meanings.map((m: any) => {
    const guid = m.guid ?? m.termGuid;
    const relationshipGuid = m.relationshipGuid ?? m.relationGuid;
    const relationshipStatus =
      m.relationshipStatus ??
      (m.status != null ? String(m.status) : "ACTIVE");
    return {
      ...m,
      guid,
      relationshipGuid,
      relationshipStatus,
      termGuid: m.termGuid ?? guid
    };
  });
};

export const shouldMergeMeaningsFromEntityHeader = (entity: any): boolean => {
  if (!entity || !entity.typeName) {
    return false;
  }
  if (String(entity.typeName).startsWith("AtlasGlossary")) {
    return false;
  }
  const existing = entity.relationshipAttributes?.meanings;
  return !Array.isArray(existing) || existing.length === 0;
};
