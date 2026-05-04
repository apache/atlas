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

/**
 * Dashboard: latest entities via basic search.
 * Request `__timestamp` only in `attributes` so responses include system created time
 * (Atlas often returns `createTime: 0`; `__timestamp` holds the real ms value).
 */
const getLatestEntities = (limit: number) => {
  const timestampMs = Date.now();
  return getBasicSearchResult(
    {
      data: {
        typeName: "_ALL_ENTITY_TYPES",
        excludeDeletedEntities: true,
        includeClassificationAttributes: true,
        includeSubClassifications: true,
        includeSubTypes: true,
        limit,
        offset: 0,
        tagFilters: null,
        entityFilters: {
          condition: "AND",
          criterion: [
            {
              attributeName: "__timestamp",
              operator: "lte",
              attributeValue: String(timestampMs),
              type: "date"
            }
          ]
        },
        classification: null,
        termName: null,
        relationshipFilters: null,
        attributes: ["__timestamp"]
      }
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
