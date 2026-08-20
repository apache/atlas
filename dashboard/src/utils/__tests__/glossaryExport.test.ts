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
import {
  buildGlossarySearchExportRequest,
  buildGlossarySearchRequest,
  createDefaultGlossaryBrowseFilters,
  fetchGlossarySearchPage,
  formatCustomAttributesForDisplay,
  formatCustomAttributesForTooltip,
  GLOSSARY_STATUS_FILTER_OPTIONS,
  mapGlossarySearchResultToTableRows,
  mapRecordTypeToGlossaryType,
  requestGlossaryExportDownload,
  unwrapGlossarySearchResponse,
  type GlossarySearchResponse
} from "../glossaryExport";

jest.mock("@api/apiMethods/glossaryApiMethod", () => ({
  searchGlossary: jest.fn(),
  createGlossaryExportFile: jest.fn()
}));

const mockSearchGlossary = searchGlossary as jest.MockedFunction<
  typeof searchGlossary
>;
const mockCreateGlossaryExportFile =
  createGlossaryExportFile as jest.MockedFunction<
    typeof createGlossaryExportFile
  >;

describe("glossaryExport utilities", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("createDefaultGlossaryBrowseFilters returns empty filters", () => {
    expect(createDefaultGlossaryBrowseFilters()).toEqual({
      recordType: "all",
      glossaryName: "",
      statusFilter: "",
      searchText: ""
    });
  });

  it("mapRecordTypeToGlossaryType maps UI filters to API enum", () => {
    expect(mapRecordTypeToGlossaryType("Term")).toBe("TERM");
    expect(mapRecordTypeToGlossaryType("Category")).toBe("CATEGORY");
    expect(mapRecordTypeToGlossaryType("all")).toBeUndefined();
  });

  it("buildGlossarySearchRequest applies defaults and filters", () => {
    const request = buildGlossarySearchRequest(
      {
        ...createDefaultGlossaryBrowseFilters(),
        recordType: "Term",
        glossaryName: "Bank",
        statusFilter: "DRAFT",
        searchText: "capital"
      },
      1,
      25,
      { Bank: "guid-1" }
    );
    expect(request).toEqual({
      limit: 25,
      offset: 25,
      sortBy: "glossaryName",
      sortOrder: "DESCENDING",
      excludeDeleted: true,
      glossaryType: "TERM",
      status: "DRAFT",
      searchQuery: "capital",
      glossary: { name: "Bank", guid: "guid-1" }
    });
  });

  it("buildGlossarySearchExportRequest zeroes pagination for full export", () => {
    const request = buildGlossarySearchExportRequest(
      {
        ...createDefaultGlossaryBrowseFilters(),
        recordType: "Category",
        glossaryName: "Bank",
        statusFilter: "ACTIVE",
        searchText: "branch"
      },
      { Bank: "guid-1" }
    );

    expect(request).toEqual({
      limit: 0,
      offset: 0,
      sortBy: "glossaryName",
      sortOrder: "DESCENDING",
      excludeDeleted: true,
      glossaryType: "CATEGORY",
      status: "ACTIVE",
      searchQuery: "branch",
      glossary: { name: "Bank", guid: "guid-1" },
      format: "CSV"
    });
  });

  it("formatCustomAttributesForDisplay renders key-value string", () => {
    expect(
      formatCustomAttributesForDisplay({
        department: "Retail",
        status: "Active"
      })
    ).toBe("department: Retail, status: Active");
  });

  it("formatCustomAttributesForDisplay returns empty string for nullish values", () => {
    expect(formatCustomAttributesForDisplay(null)).toBe("");
    expect(formatCustomAttributesForDisplay(undefined)).toBe("");
    expect(formatCustomAttributesForDisplay({})).toBe("");
  });

  it("formatCustomAttributesForTooltip renders JSON details", () => {
    expect(
      formatCustomAttributesForTooltip({
        department: "Retail",
        status: "Active"
      })
    ).toContain('"department": "Retail"');
  });

  it("exposes glossary status filter dropdown options", () => {
    expect(GLOSSARY_STATUS_FILTER_OPTIONS).toEqual([
      { label: "All", value: "" },
      { label: "Draft", value: "DRAFT" },
      { label: "Active", value: "ACTIVE" },
      { label: "Deprecated", value: "DEPRECATED" }
    ]);
  });

  it("mapGlossarySearchResultToTableRows maps terms and categories with guids", () => {
    const response: GlossarySearchResponse = {
      approximateCount: 2,
      glossary: [
        {
          name: "testBankingGlossary",
          guid: "glossary-guid-1",
          terms: [
            {
              name: "CapitalTerm075",
              guid: "term-guid-1",
              qualifiedName: "CapitalTerm075@testBankingGlossary",
              status: "ACTIVE",
              classifications: [{ typeName: "class3" }],
              customAttributes: { department: "Retail", status: "Active" }
            }
          ],
          categories: [
            {
              name: "BranchCategory",
              guid: "category-guid-1",
              status: "ACTIVE"
            }
          ]
        }
      ]
    };
    const rows = mapGlossarySearchResultToTableRows(response);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      recordType: "Term",
      name: "CapitalTerm075",
      guid: "term-guid-1",
      glossaryGuid: "glossary-guid-1",
      classifications: [{ typeName: "class3" }],
      customAttributes: { department: "Retail", status: "Active" }
    });
    expect(rows[1]).toMatchObject({
      recordType: "Category",
      name: "BranchCategory",
      guid: "category-guid-1",
      glossaryGuid: "glossary-guid-1",
      customAttributes: {}
    });
  });

  it("mapGlossarySearchResultToTableRows returns empty array for invalid responses", () => {
    expect(mapGlossarySearchResultToTableRows(null)).toEqual([]);
    expect(mapGlossarySearchResultToTableRows(undefined)).toEqual([]);
    expect(
      mapGlossarySearchResultToTableRows({
        glossary: "not-array" as unknown as GlossarySearchResponse["glossary"]
      })
    ).toEqual([]);
  });

  it("unwrapGlossarySearchResponse reads axios data wrapper", () => {
    const body: GlossarySearchResponse = {
      approximateCount: 1,
      glossary: [{ name: "G1", guid: "guid-1", terms: [{ name: "T1" }] }]
    };
    expect(unwrapGlossarySearchResponse({ data: body })).toEqual(body);
    expect(unwrapGlossarySearchResponse(body)).toEqual(body);
  });

  it("fetchGlossarySearchPage maps API response to rows and total count", async () => {
    mockSearchGlossary.mockResolvedValue({
      data: {
        approximateCount: 1,
        glossary: [
          {
            name: "G1",
            guid: "g-1",
            terms: [{ name: "Term1", guid: "t-1" }]
          }
        ]
      }
    });

    const result = await fetchGlossarySearchPage({
      limit: 25,
      offset: 0,
      sortBy: "glossaryName",
      sortOrder: "DESCENDING"
    });

    expect(mockSearchGlossary).toHaveBeenCalled();
    expect(result.rows).toHaveLength(1);
    expect(result.totalCount).toBe(1);
  });

  it("fetchGlossarySearchPage rejects when searchGlossary fails", async () => {
    const error = new Error("search failed");
    mockSearchGlossary.mockRejectedValue(error);

    await expect(
      fetchGlossarySearchPage({
        limit: 25,
        offset: 0,
        sortBy: "glossaryName",
        sortOrder: "DESCENDING"
      })
    ).rejects.toThrow("search failed");
  });

  it("requestGlossaryExportDownload enqueues export via create_file API", async () => {
    mockCreateGlossaryExportFile.mockResolvedValue(undefined);

    await requestGlossaryExportDownload(
      createDefaultGlossaryBrowseFilters(),
      { Bank: "guid-1" }
    );

    expect(mockCreateGlossaryExportFile).toHaveBeenCalledWith({
      limit: 0,
      offset: 0,
      sortBy: "glossaryName",
      sortOrder: "DESCENDING",
      excludeDeleted: true,
      format: "CSV"
    });
  });

  it("requestGlossaryExportDownload sends selected XLSX format", async () => {
    mockCreateGlossaryExportFile.mockResolvedValue(undefined);

    await requestGlossaryExportDownload(
      createDefaultGlossaryBrowseFilters(),
      { Bank: "guid-1" },
      "XLSX"
    );

    expect(mockCreateGlossaryExportFile).toHaveBeenCalledWith(
      expect.objectContaining({ format: "XLSX" })
    );
  });

  it("requestGlossaryExportDownload rejects when createGlossaryExportFile fails", async () => {
    const error = new Error("export failed");
    mockCreateGlossaryExportFile.mockRejectedValue(error);

    await expect(
      requestGlossaryExportDownload(createDefaultGlossaryBrowseFilters())
    ).rejects.toThrow("export failed");
  });
});
