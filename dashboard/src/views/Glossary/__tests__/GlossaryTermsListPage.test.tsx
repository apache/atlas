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

import React from "react";
import { render, screen, fireEvent, waitFor, act } from "@testing-library/react";
import { Provider } from "react-redux";
import { configureStore } from "@reduxjs/toolkit";
import GlossaryTermsListPage from "../GlossaryTermsListPage";

const mockFetchGlossarySearchPage = jest.fn();
const mockRequestGlossaryExportDownload = jest.fn();
const mockFetchGlossaryData = jest.fn(() => ({ type: "glossary/fetch" }));
const mockServerError = jest.fn();

jest.mock("@utils/glossaryExport", () => ({
  buildGlossarySearchRequest: jest.fn(() => ({})),
  createDefaultGlossaryBrowseFilters: jest.fn(() => ({
    recordType: "all",
    glossaryName: "",
    statusFilter: "",
    searchText: ""
  })),
  fetchGlossarySearchPage: (...args: unknown[]) =>
    mockFetchGlossarySearchPage(...args),
  GLOSSARY_STATUS_FILTER_OPTIONS: [
    { label: "All", value: "" },
    { label: "Draft", value: "DRAFT" },
    { label: "Active", value: "ACTIVE" },
    { label: "Deprecated", value: "DEPRECATED" }
  ],
  GLOSSARY_EXPORT_FORMAT_OPTIONS: [
    { label: "CSV", value: "CSV" },
    { label: "XLSX", value: "XLSX" }
  ],
  requestGlossaryExportDownload: (...args: unknown[]) =>
    mockRequestGlossaryExportDownload(...args)
}));

jest.mock("@redux/slice/glossarySlice", () => ({
  fetchGlossaryData: (...args: unknown[]) => mockFetchGlossaryData(...args)
}));

jest.mock("@utils/Utils", () => ({
  serverError: (...args: unknown[]) => mockServerError(...args)
}));

jest.mock("../glossaryTermsListColumns", () => ({
  createGlossaryTermsListColumns: jest.fn(() => [
    { accessorKey: "name", header: "Name" }
  ])
}));

jest.mock("@components/Table/TableLayout", () => ({
  TableLayout: ({ fetchData }: { fetchData: (args: unknown) => void }) => {
    React.useEffect(() => {
      void fetchData({ pagination: { pageIndex: 0, pageSize: 25 } });
    }, [fetchData]);
    return <div data-testid="table-layout">Table</div>;
  }
}));

jest.mock("react-toastify", () => ({
  toast: {
    success: jest.fn(),
    dismiss: jest.fn()
  }
}));

const createStore = () =>
  configureStore({
    reducer: {
      glossary: () => ({
        glossaryData: [{ name: "testGlossary", guid: "g-1" }],
        loading: false
      })
    }
  });

const openDownloadMenu = async () => {
  const downloadButton = await screen.findByRole("button", { name: /download/i });
  await waitFor(() => {
    expect(downloadButton).not.toBeDisabled();
  });
  fireEvent.click(downloadButton);
};

describe("GlossaryTermsListPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockFetchGlossarySearchPage.mockResolvedValue({
      rows: [{ id: "term-1", name: "Term1" }],
      totalCount: 1
    });
    mockRequestGlossaryExportDownload.mockResolvedValue(undefined);
  });

  it("renders page layout with aligned wrapper and filters", async () => {
    const { container } = render(
      <Provider store={createStore()}>
        <GlossaryTermsListPage />
      </Provider>
    );

    expect(
      container.querySelector(".glossary-terms-list-page")
    ).toBeInTheDocument();
    expect(
      container.querySelector(".glossary-terms-filters")
    ).toBeInTheDocument();
    expect(
      container.querySelector(".glossary-terms-table")
    ).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: /glossary terms & categories/i })
    ).toBeInTheDocument();

    await waitFor(() => {
      expect(screen.getByTestId("table-layout")).toBeInTheDocument();
    });
  });

  it("does not enqueue export when only opening the download menu", async () => {
    render(
      <Provider store={createStore()}>
        <GlossaryTermsListPage />
      </Provider>
    );

    await waitFor(() => {
      expect(mockFetchGlossarySearchPage).toHaveBeenCalled();
    });

    await openDownloadMenu();

    expect(await screen.findByRole("menuitem", { name: /^CSV$/i })).toBeInTheDocument();
    expect(screen.getByRole("menuitem", { name: /^XLSX$/i })).toBeInTheDocument();
    expect(mockRequestGlossaryExportDownload).not.toHaveBeenCalled();
  });

  it("enqueues glossary export as CSV from the download menu", async () => {
    render(
      <Provider store={createStore()}>
        <GlossaryTermsListPage />
      </Provider>
    );

    await waitFor(() => {
      expect(mockFetchGlossarySearchPage).toHaveBeenCalled();
    });

    await openDownloadMenu();
    fireEvent.click(await screen.findByRole("menuitem", { name: /^CSV$/i }));

    await waitFor(() => {
      expect(mockRequestGlossaryExportDownload).toHaveBeenCalledWith(
        expect.objectContaining({
          recordType: "all",
          glossaryName: "",
          statusFilter: "",
          searchText: ""
        }),
        expect.objectContaining({ testGlossary: "g-1" }),
        "CSV"
      );
    });
  });

  it("enqueues glossary export as XLSX from the download menu", async () => {
    render(
      <Provider store={createStore()}>
        <GlossaryTermsListPage />
      </Provider>
    );

    await waitFor(() => {
      expect(mockFetchGlossarySearchPage).toHaveBeenCalled();
    });

    await openDownloadMenu();
    fireEvent.click(await screen.findByRole("menuitem", { name: /^XLSX$/i }));

    await waitFor(() => {
      expect(mockRequestGlossaryExportDownload).toHaveBeenCalledWith(
        expect.any(Object),
        expect.any(Object),
        "XLSX"
      );
    });
  });

  it("disables download when table has no rows", async () => {
    mockFetchGlossarySearchPage.mockResolvedValue({
      rows: [],
      totalCount: 0
    });

    render(
      <Provider store={createStore()}>
        <GlossaryTermsListPage />
      </Provider>
    );

    await waitFor(() => {
      expect(mockFetchGlossarySearchPage).toHaveBeenCalled();
    });

    expect(screen.getByRole("button", { name: /download/i })).toBeDisabled();
  });

  it("shows serverError when glossary search fetch fails", async () => {
    const fetchError = new Error("search failed");
    mockFetchGlossarySearchPage.mockRejectedValue(fetchError);

    render(
      <Provider store={createStore()}>
        <GlossaryTermsListPage />
      </Provider>
    );

    await waitFor(() => {
      expect(mockServerError).toHaveBeenCalledWith(
        fetchError,
        expect.any(Object)
      );
    });
  });

  it("shows serverError and re-enables download when export fails", async () => {
    const exportError = new Error("export failed");
    mockRequestGlossaryExportDownload.mockRejectedValue(exportError);

    render(
      <Provider store={createStore()}>
        <GlossaryTermsListPage />
      </Provider>
    );

    await waitFor(() => {
      expect(mockFetchGlossarySearchPage).toHaveBeenCalled();
    });

    const downloadButton = await screen.findByRole("button", { name: /download/i });
    await waitFor(() => {
      expect(downloadButton).not.toBeDisabled();
    });
    await openDownloadMenu();
    fireEvent.click(await screen.findByRole("menuitem", { name: /^CSV$/i }));

    await waitFor(() => {
      expect(mockServerError).toHaveBeenCalledWith(
        exportError,
        expect.any(Object)
      );
    });

    expect(downloadButton).not.toBeDisabled();
  });

  it("refetches table data when record type filter changes", async () => {
    render(
      <Provider store={createStore()}>
        <GlossaryTermsListPage />
      </Provider>
    );

    await waitFor(() => {
      expect(mockFetchGlossarySearchPage).toHaveBeenCalledTimes(1);
    });

    fireEvent.mouseDown(screen.getByLabelText(/record type/i));
    fireEvent.click(await screen.findByRole("option", { name: /terms only/i }));

    await waitFor(() => {
      expect(mockFetchGlossarySearchPage).toHaveBeenCalledTimes(2);
    });
  });

  it("debounces search text filter refresh before refetching", async () => {
    jest.useFakeTimers();

    render(
      <Provider store={createStore()}>
        <GlossaryTermsListPage />
      </Provider>
    );

    await waitFor(() => {
      expect(mockFetchGlossarySearchPage).toHaveBeenCalledTimes(1);
    });

    fireEvent.change(screen.getByLabelText(/search all columns/i), {
      target: { value: "capital" }
    });

    expect(mockFetchGlossarySearchPage).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(400);
    });

    await waitFor(() => {
      expect(mockFetchGlossarySearchPage).toHaveBeenCalledTimes(2);
    });

    jest.useRealTimers();
  });
});
