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

import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { createGlossaryTermsListColumns } from "../glossaryTermsListColumns";
import type { GlossaryTableRow } from "@utils/glossaryExport";

jest.mock("@components/DialogShowMoreLess", () => ({
  __esModule: true,
  default: ({ value }: { value: { classifications?: Array<{ typeName?: string }> } }) => (
    <div data-testid="dialog-show-more-less">
      {(value.classifications ?? []).map((item) => item.typeName).join(",")}
    </div>
  )
}));

const sampleRow: GlossaryTableRow = {
  id: "term-1",
  recordType: "Term",
  name: "MetricTerm015",
  guid: "term-guid-1",
  qualifiedName: "MetricTerm015@testBankingGlossary",
  glossaryName: "testBankingGlossary",
  glossaryGuid: "glossary-guid-1",
  shortDescription: "Short desc text",
  longDescription: "Long desc text",
  status: "ACTIVE",
  classifications: [{ typeName: "class3" }],
  customAttributes: { department: "Retail", status: "Active" }
};

const glossaryTermsListColumns = createGlossaryTermsListColumns(jest.fn());

const renderColumnCell = (
  accessorKey: keyof GlossaryTableRow,
  row: GlossaryTableRow = sampleRow
) => {
  const column = glossaryTermsListColumns.find(
    (c) => "accessorKey" in c && c.accessorKey === accessorKey
  );
  if (!column || !column.cell || typeof column.cell !== "function") {
    throw new Error(`Column ${accessorKey} not found`);
  }

  const cellFn = column.cell as (ctx: {
    getValue: () => unknown;
    row: { original: GlossaryTableRow };
  }) => React.ReactNode;

  return render(
    <MemoryRouter>
      {cellFn({
        getValue: () => row[accessorKey],
        row: { original: row }
      })}
    </MemoryRouter>
  );
};

describe("glossaryTermsListColumns", () => {
  it("orders glossary name before name and omits related categories column", () => {
    const keys = glossaryTermsListColumns
      .map((column) =>
        "accessorKey" in column ? column.accessorKey : undefined
      )
      .filter(Boolean);
    expect(keys).toEqual([
      "glossaryName",
      "name",
      "recordType",
      "shortDescription",
      "longDescription",
      "classifications",
      "customAttributes",
      "status"
    ]);
  });

  it("renders name as a plain search-result style link", () => {
    renderColumnCell("name");
    const link = screen.getByRole("link", { name: "MetricTerm015" });
    expect(link).toHaveClass("entity-name");
    expect(link).toHaveAttribute(
      "href",
      "/glossary/term-guid-1?gid=glossary-guid-1&gtype=term&viewType=term&fromView=entity&term=MetricTerm015%40testBankingGlossary"
    );
  });

  it("renders glossary name with tooltip text", () => {
    renderColumnCell("glossaryName");
    expect(screen.getByText("testBankingGlossary")).toBeInTheDocument();
  });

  it("renders short and long descriptions as plain text", () => {
    renderColumnCell("shortDescription");
    expect(screen.getByText("Short desc text")).toBeInTheDocument();

    renderColumnCell("longDescription");
    expect(screen.getByText("Long desc text")).toBeInTheDocument();
  });

  it("renders classifications for terms", () => {
    renderColumnCell("classifications");
    expect(screen.getByTestId("dialog-show-more-less")).toHaveTextContent(
      "class3"
    );
  });

  it("renders custom attributes as readable string", () => {
    renderColumnCell("customAttributes");
    expect(
      screen.getByText("department: Retail, status: Active")
    ).toBeInTheDocument();
  });

  it("renders status as a colored badge without icons", () => {
    renderColumnCell("status");
    const badge = screen.getByLabelText("Active status");
    expect(badge).toBeInTheDocument();
    expect(badge).toHaveTextContent("Active");
    expect(badge).toHaveClass("MuiChip-colorSuccess");
    expect(badge.querySelector(".MuiChip-icon")).not.toBeInTheDocument();
  });

  it("uses a narrower status column width", () => {
    const statusColumn = glossaryTermsListColumns.find(
      (column) => "accessorKey" in column && column.accessorKey === "status"
    );
    expect(statusColumn?.size).toBe(76);
  });

  it("renders name as plain text when guid is missing", () => {
    renderColumnCell("name", { ...sampleRow, guid: "" });
    expect(screen.getByText("MetricTerm015")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "MetricTerm015" })).not.toBeInTheDocument();
  });

  it("renders empty custom attributes as null", () => {
    const { container } = renderColumnCell("customAttributes", {
      ...sampleRow,
      customAttributes: {}
    });
    expect(container).toBeEmptyDOMElement();
  });

  it("renders empty classifications for category rows", () => {
    renderColumnCell("classifications", {
      ...sampleRow,
      recordType: "Category",
      classifications: []
    });
    expect(screen.getByTestId("dialog-show-more-less")).toHaveTextContent("");
  });
});
