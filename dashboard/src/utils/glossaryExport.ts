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

import {
  createGlossaryExportFile,
  searchGlossary
} from "@api/apiMethods/glossaryApiMethod";

export const GLOSSARY_SEARCH_DEFAULTS = {
  limit: 25,
  offset: 0,
  sortBy: "glossaryName",
  sortOrder: "DESCENDING"
} as const;

export type GlossarySearchSortOrder = "ASCENDING" | "DESCENDING";

export type GlossarySearchGlossaryType = "ALL" | "TERM" | "CATEGORY";

export interface GlossarySearchRequest {
  limit: number;
  offset: number;
  sortBy: string;
  sortOrder: GlossarySearchSortOrder;
  glossaryType?: GlossarySearchGlossaryType;
  status?: string;
  searchQuery?: string;
  glossary?: { name?: string; guid?: string };
  excludeDeleted?: boolean;
}

export interface GlossarySearchClassification {
  typeName?: string;
  entityGuid?: string;
  attributes?: Record<string, unknown>;
}

export interface GlossarySearchTermDetail {
  name?: string;
  qualifiedName?: string;
  guid?: string;
  shortDescription?: string;
  longDescription?: string;
  status?: string;
  classifications?: GlossarySearchClassification[];
  customAttributes?: Record<string, unknown>;
}

export interface GlossarySearchCategoryDetail {
  name?: string;
  qualifiedName?: string;
  guid?: string;
  shortDescription?: string;
  longDescription?: string;
  status?: string;
  classifications?: GlossarySearchClassification[];
  customAttributes?: Record<string, unknown>;
}

export interface GlossarySearchGlossaryDetail {
  name?: string;
  guid?: string;
  terms?: GlossarySearchTermDetail[];
  categories?: GlossarySearchCategoryDetail[];
}

export interface GlossarySearchResponse {
  glossary?: GlossarySearchGlossaryDetail[];
  approximateCount?: number;
}

export interface GlossaryBrowseFilters {
  recordType: "all" | "Term" | "Category";
  glossaryName: string;
  statusFilter: string;
  searchText: string;
}

export type GlossaryExportFormat = "CSV" | "XLSX";

export const GLOSSARY_EXPORT_FORMAT_OPTIONS = [
  { label: "CSV", value: "CSV" as const },
  { label: "XLSX", value: "XLSX" as const }
] as const;

export const GLOSSARY_STATUS_FILTER_OPTIONS = [
  { label: "All", value: "" },
  { label: "Draft", value: "DRAFT" },
  { label: "Active", value: "ACTIVE" },
  { label: "Deprecated", value: "DEPRECATED" }
] as const;

export type GlossaryTableRow = {
  id: string;
  recordType: "Term" | "Category";
  name: string;
  guid: string;
  qualifiedName: string;
  glossaryName: string;
  glossaryGuid: string;
  shortDescription: string;
  longDescription: string;
  status: string;
  classifications: GlossarySearchClassification[];
  customAttributes: Record<string, unknown>;
};

export const createDefaultGlossaryBrowseFilters = (): GlossaryBrowseFilters => ({
  recordType: "all",
  glossaryName: "",
  statusFilter: "",
  searchText: ""
});

export const mapRecordTypeToGlossaryType = (
  recordType: GlossaryBrowseFilters["recordType"]
): GlossarySearchGlossaryType | undefined => {
  if (recordType === "Term") {
    return "TERM";
  }
  if (recordType === "Category") {
    return "CATEGORY";
  }
  return undefined;
};

export const buildGlossarySearchRequest = (
  filters: GlossaryBrowseFilters,
  page: number,
  rowsPerPage: number,
  glossaryGuidByName: Record<string, string> = {}
): GlossarySearchRequest => {
  const request: GlossarySearchRequest = {
    limit: rowsPerPage,
    offset: page * rowsPerPage,
    sortBy: GLOSSARY_SEARCH_DEFAULTS.sortBy,
    sortOrder: GLOSSARY_SEARCH_DEFAULTS.sortOrder,
    excludeDeleted: true
  };

  const glossaryType = mapRecordTypeToGlossaryType(filters.recordType);
  if (glossaryType) {
    request.glossaryType = glossaryType;
  }

  const status = filters.statusFilter.trim();
  if (status) {
    request.status = status;
  }

  const searchQuery = filters.searchText.trim();
  if (searchQuery) {
    request.searchQuery = searchQuery;
  }

  const glossaryName = filters.glossaryName.trim();
  if (glossaryName) {
    request.glossary = {
      name: glossaryName,
      guid: glossaryGuidByName[glossaryName]
    };
  }

  return request;
};

export const buildGlossarySearchExportRequest = (
  filters: GlossaryBrowseFilters,
  glossaryGuidByName: Record<string, string> = {},
  format: GlossaryExportFormat = "CSV"
): GlossarySearchRequest & { format: GlossaryExportFormat } => ({
  ...buildGlossarySearchRequest(filters, 0, 0, glossaryGuidByName),
  limit: 0,
  offset: 0,
  format
});

const empty = (v: unknown): string =>
  v === null || v === undefined ? "" : String(v);

export const formatCustomAttributesForDisplay = (
  attrs: Record<string, unknown> | null | undefined
): string => {
  if (!attrs || typeof attrs !== "object" || Object.keys(attrs).length === 0) {
    return "";
  }
  return Object.entries(attrs)
    .map(([key, value]) => `${key}: ${empty(value)}`)
    .join(", ");
};

export const formatCustomAttributesForTooltip = (
  attrs: Record<string, unknown> | null | undefined
): string => {
  if (!attrs || typeof attrs !== "object" || Object.keys(attrs).length === 0) {
    return "";
  }
  try {
    return JSON.stringify(attrs, null, 2);
  } catch {
    return formatCustomAttributesForDisplay(attrs);
  }
};

const mapDetailToTableRow = (
  detail: GlossarySearchTermDetail | GlossarySearchCategoryDetail,
  recordType: GlossaryTableRow["recordType"],
  glossaryName: string,
  glossaryGuid: string
): GlossaryTableRow => ({
  id: `${recordType.toLowerCase()}-${empty(detail.guid) || empty(detail.name)}`,
  recordType,
  name: empty(detail.name),
  guid: empty(detail.guid),
  qualifiedName: empty(detail.qualifiedName),
  glossaryName,
  glossaryGuid,
  shortDescription: empty(detail.shortDescription),
  longDescription: empty(detail.longDescription),
  status: empty(detail.status),
  classifications: detail.classifications ?? [],
  customAttributes: detail.customAttributes ?? {}
});

export const mapGlossarySearchResultToTableRows = (
  response: GlossarySearchResponse | null | undefined
): GlossaryTableRow[] => {
  const glossaries = response?.glossary;
  if (!Array.isArray(glossaries)) {
    return [];
  }

  const rows: GlossaryTableRow[] = [];
  glossaries.forEach((glossary) => {
    const glossaryName = empty(glossary?.name);
    const glossaryGuid = empty(glossary?.guid);
    (glossary?.terms ?? []).forEach((term) => {
      rows.push(mapDetailToTableRow(term, "Term", glossaryName, glossaryGuid));
    });
    (glossary?.categories ?? []).forEach((category) => {
      rows.push(
        mapDetailToTableRow(category, "Category", glossaryName, glossaryGuid)
      );
    });
  });

  return rows;
};

/** Axios returns { data }; unwrap to GlossarySearchResponse body. */
export const unwrapGlossarySearchResponse = (
  response: unknown
): GlossarySearchResponse => {
  if (
    response &&
    typeof response === "object" &&
    "data" in response &&
    (response as { data: unknown }).data &&
    typeof (response as { data: unknown }).data === "object"
  ) {
    return (response as { data: GlossarySearchResponse }).data;
  }
  return (response as GlossarySearchResponse) ?? {};
};

export async function fetchGlossarySearchPage(
  request: GlossarySearchRequest
): Promise<{
  rows: GlossaryTableRow[];
  totalCount: number;
}> {
  const raw = await searchGlossary(request);
  const response = unwrapGlossarySearchResponse(raw);
  const rows = mapGlossarySearchResultToTableRows(response);
  const totalCount =
    typeof response?.approximateCount === "number"
      ? response.approximateCount
      : rows.length;

  return { rows, totalCount };
}

export async function requestGlossaryExportDownload(
  filters: GlossaryBrowseFilters,
  glossaryGuidByName: Record<string, string> = {},
  format: GlossaryExportFormat = "CSV"
): Promise<void> {
  const request = buildGlossarySearchExportRequest(
    filters,
    glossaryGuidByName,
    format
  );
  await createGlossaryExportFile(request);
}
