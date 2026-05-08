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

import type { NavigateFunction } from "react-router-dom";
import { attributeFilter } from "@utils/CommonViewFunction";

export type DashboardSearchType =
	| "all_entities"
	| "all_classifications"
	| "entity_status"
	| "entity_type";

export interface DashboardSearchParams {
	type?: string;
	tag?: string;
	includeDE?: boolean;
	entityFilters?: {
		condition: string;
		criterion: Array<{ attributeName: string; operator: string; attributeValue: string }>;
	};
}

const buildSearchParams = (
	type: DashboardSearchType,
	extra?: Partial<DashboardSearchParams>
): URLSearchParams => {
	const params = new URLSearchParams();
	params.set("searchType", "basic");

	if (type === "all_classifications") {
		params.set("tag", extra?.tag ?? "_ALL_CLASSIFICATION_TYPES");
		params.set("type", "");
	} else {
		params.set("type", extra?.type ?? "_ALL_ENTITY_TYPES");
	}
	if (extra?.includeDE) {
		params.set("includeDE", "true");
	}
	if (extra?.entityFilters) {
		const urlStr = attributeFilter.generateUrl({ value: extra.entityFilters });
		if (urlStr) params.set("entityFilters", urlStr);
	}
	return params;
};

export const navigateToTaggedSearch = (navigate: NavigateFunction): void => {
	navigateToSearch(navigate, "all_classifications");
};

export const navigateToClassificationSearch = (
	navigate: NavigateFunction,
	tagName: string
): void => {
	navigateToSearch(navigate, "all_classifications", { tag: tagName });
};

/** "View All" from Latest Entities card: basic all-types search, newest first. */
export const navigateToLatestEntitiesSearch = (navigate: NavigateFunction): void => {
	const params = buildSearchParams("all_entities");
	params.set("pageLimit", "25");
	params.set("pageOffset", "0");
	params.set("sortBy", "__timestamp");
	params.set("sortOrder", "DESCENDING");
	/* Same as dashboard card: request system create time in the basic-search body. */
	params.set("attributes", "__timestamp");
	navigate({ pathname: "/search/searchResult", search: params.toString() });
};

export const navigateToEntityTypeSearch = (
	navigate: NavigateFunction,
	typeName: string,
	includeDeleted: boolean
): void => {
	const extra: Partial<DashboardSearchParams> = {
		type: typeName,
		includeDE: includeDeleted
	};
	if (includeDeleted) {
		extra.entityFilters = {
			condition: "AND",
			criterion: [{ attributeName: "__state", operator: "eq", attributeValue: "DELETED" }]
		};
	}
	navigateToSearch(navigate, "entity_type", extra);
};

/** Search all entity typedefs that belong to one service-type bucket (sidebar grouping). */
export const navigateToServiceTypeEntitySearch = (
	navigate: NavigateFunction,
	typeNames: string[],
	includeDeleted: boolean
): void => {
	const cleaned = [...new Set(typeNames.filter(Boolean))].sort((a, b) => a.localeCompare(b));
	if (cleaned.length === 0) {
		navigateToSearch(navigate, "all_entities");
		return;
	}
	if (cleaned.length === 1) {
		navigateToEntityTypeSearch(navigate, cleaned[0], includeDeleted);
		return;
	}
	const criterion = cleaned.map((typeName) => ({
		attributeName: "__typeName",
		operator: "eq",
		attributeValue: typeName,
	}));
	navigateToSearch(navigate, "all_entities", {
		type: "_ALL_ENTITY_TYPES",
		includeDE: includeDeleted,
		entityFilters: { condition: "OR", criterion },
	});
};

export const navigateToSearch = (
	navigate: NavigateFunction,
	type: DashboardSearchType,
	extra?: Partial<DashboardSearchParams>
): void => {
	const search = buildSearchParams(type, extra).toString();
	navigate({ pathname: "/search/searchResult", search });
};

/** Basic search using free-text `query` (e.g. entity typedef name). */
export const navigateToBasicTextQuery = (
	navigate: NavigateFunction,
	query: string
): void => {
	const params = new URLSearchParams();
	params.set("searchType", "basic");
	params.set("query", query.trim());
	params.set("pageLimit", "25");
	params.set("pageOffset", "0");
	navigate({ pathname: "/search/searchResult", search: params.toString() });
};

export const navigateToClassificationDetailPage = (
	navigate: NavigateFunction,
	classificationName: string
): void => {
	const sp = new URLSearchParams();
	sp.set("tag", classificationName);
	navigate({
		pathname: `/tag/tagAttribute/${encodeURIComponent(classificationName)}`,
		search: sp.toString()
	});
};

export const navigateToGlossaryTermDetailPage = (
	navigate: NavigateFunction,
	args: {
		termGuid: string;
		termId: string;
		glossaryGuid: string;
		parentName: string;
	}
): void => {
	const sp = new URLSearchParams();
	sp.set("gid", args.glossaryGuid);
	sp.set("term", `${args.termId}@${args.parentName}`);
	sp.set("gtype", "term");
	sp.set("viewType", "term");
	sp.set("guid", args.termGuid);
	sp.set("searchType", "basic");
	navigate({
		pathname: `/glossary/${args.termGuid}`,
		search: sp.toString()
	});
};

export const navigateToBusinessMetadataDetailPage = (
	navigate: NavigateFunction,
	bmGuid: string
): void => {
	navigate({ pathname: `/administrator/businessMetadata/${bmGuid}` });
};
