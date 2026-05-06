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

import { searchApiUrl } from "../apiUrlLinks/searchApiUrl";
import { fetchApi } from "./fetchApi";
import { serializeRelationshipSearchParams } from "@utils/relationshipSearchQuery";

const getBasicSearchResult = (params: any, searchType: string | null) => {
  const config: any = {
    method: searchType == "dsl" ? "GET" : "POST",
    ...params
  };
  return fetchApi(searchApiUrl(searchType || ""), config);
};
const getRelationShipResult = (params: any) => {
  const config: any = {
    method: "POST",
    ...params
  };
  return fetchApi(searchApiUrl("relations"), config);
};

const getGlobalSearchResult = (searchTerm: string, params: any) => {
  const config: any = {
    method: "GET",
    ...params
  };
  return fetchApi(searchApiUrl(searchTerm), config);
};

const getRelationShip = (params: any) => {
  const config: any = {
    method: "GET",
    ...params
  };
  return fetchApi(searchApiUrl("relationship"), config);
};

const getRelationShipV2 = (params: { params: Record<string, unknown> }) => {
  const qs = serializeRelationshipSearchParams(params.params);
  const url = `${searchApiUrl("relationship")}?${qs}`;
  return fetchApi(url, { method: "GET" });
};

/** Atlas `SearchParameters.sortBy` / `sortOrder` (see `SortOrder` enum: DESCENDING). */
const LATEST_ENTITIES_TIMESTAMP_SORT = "__timestamp" as const;

type LatestEntitiesSearchOptions = {
  limit: number;
  includeSubClassifications: boolean;
};

/**
 * Request `__timestamp` so sort + “Created … ago” work. Do not set
 * `excludeHeaderAttributes`: for `_ALL_ENTITY_TYPES`, Atlas validates each
 * `attributes` entry against `__ENTITY_ROOT` and rejects `name` / `qualifiedName`
 * / `guid` (see `excludeHeaderAttributesAllEntityType` in Atlas tests). Normal
 * headers then include name, guid, typeName like the main basic search.
 */
const buildLatestEntitiesBasicBody = (opts: LatestEntitiesSearchOptions) => {
  return {
    typeName: "_ALL_ENTITY_TYPES",
    excludeDeletedEntities: true,
    includeClassificationAttributes: true,
    includeSubTypes: true,
    includeSubClassifications: opts.includeSubClassifications,
    limit: opts.limit,
    offset: 0,
    tagFilters: null,
    entityFilters: null,
    classification: null,
    termName: null,
    relationshipFilters: null,
    attributes: ["__timestamp"],
    sortBy: LATEST_ENTITIES_TIMESTAMP_SORT,
    sortOrder: "DESCENDING",
  };
};

/**
 * Dashboard card only: newest entities by `__timestamp`, no entity filters,
 * sub-classifications off, full entity headers for name/guid/type.
 */
const getLatestEntities = () => {
  return getBasicSearchResult(
    {
      data: buildLatestEntitiesBasicBody({
        limit: 7,
        includeSubClassifications: false,
      }),
    },
    "basic"
  );
};

export {
	getBasicSearchResult,
	getRelationShipResult,
	getGlobalSearchResult,
	getRelationShip,
	getRelationShipV2,
	getLatestEntities
};
