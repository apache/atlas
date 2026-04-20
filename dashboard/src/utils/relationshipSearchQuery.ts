/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** Extra attributes requested for schema tab rows (type, position ordering). */
export const SCHEMA_RELATIONSHIP_EXTRA_ATTRIBUTES = ["type", "position"];

/**
 * Schema tab always requests active + deleted (`excludeDeletedEntities: false`).
 * "Show historical entities" filters cached rows only — it must not change params.
 */
export const SCHEMA_RELATIONSHIP_INCLUDE_DELETED = true;

export const DEFAULT_RELATIONSHIP_PAGE_LIMIT = 100;

/**
 * Atlas may return approximateCount -1 when getApproximateCount is false or unknown.
 * Only non-negative finite values are usable for pagination totals.
 */
export const normalizeRelationshipApproximateCount = (
  raw: unknown
): number | undefined => {
  if (raw === undefined || raw === null) {
    return undefined;
  }
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) {
    return undefined;
  }
  return n;
};

export const getMinPageLimitForTotal = (totalCount?: number): number => {
  if (typeof totalCount !== "number" || !Number.isFinite(totalCount)) {
    return 1;
  }
  if (totalCount <= 0) {
    return 1;
  }
  return totalCount <= 1 ? 1 : 2;
};

export interface RelationshipSearchParamsInput {
  guid: string;
  relation: string;
  limit: number;
  offset: number;
  isSorted: boolean;
  showDeleted: boolean;
  getApproximateCount: boolean;
  /** When set, adds repeated attributes= query params (e.g. type, position). */
  extraAttributes?: string[];
}

/**
 * Builds the flat param object for GET /v2/search/relationship (same semantics
 * as relationship cards on the entity detail page).
 */
export const buildRelationshipSearchParams = (
  opts: RelationshipSearchParamsInput
): Record<string, unknown> => {
  const base: Record<string, unknown> = {
    limit: opts.limit,
    offset: opts.offset,
    guid: opts.guid,
    sortBy: opts.isSorted ? "name" : undefined,
    sortOrder: opts.isSorted ? "ASCENDING" : undefined,
    disableDefaultSorting: !opts.isSorted,
    excludeDeletedEntities: !opts.showDeleted,
    includeSubClassifications: true,
    includeSubTypes: true,
    includeClassificationAttributes: true,
    relation: opts.relation,
    getApproximateCount: opts.getApproximateCount
  };
  if (opts.extraAttributes && opts.extraAttributes.length > 0) {
    base.attributes = opts.extraAttributes;
  }
  return base;
};

/**
 * Serializes relationship search params for GET query string.
 * Repeats `attributes` as attributes=a&attributes=b (JAX-RS style).
 */
export const serializeRelationshipSearchParams = (
  params: Record<string, unknown>
): string => {
  const parts: string[] = [];
  for (const [key, val] of Object.entries(params)) {
    if (val === undefined || val === null) {
      continue;
    }
    if (key === "attributes" && Array.isArray(val)) {
      val.forEach((a) => {
        parts.push(
          `${encodeURIComponent(key)}=${encodeURIComponent(String(a))}`
        );
      });
      continue;
    }
    if (typeof val === "boolean") {
      parts.push(
        `${encodeURIComponent(key)}=${encodeURIComponent(val ? "true" : "false")}`
      );
      continue;
    }
    parts.push(
      `${encodeURIComponent(key)}=${encodeURIComponent(String(val))}`
    );
  }
  return parts.join("&");
};
