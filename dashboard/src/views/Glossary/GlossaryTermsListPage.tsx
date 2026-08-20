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

import { TableLayout } from "@components/Table/TableLayout";
import { CustomButton, LightTooltip } from "@components/muiComponents";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { fetchGlossaryData } from "@redux/slice/glossarySlice";
import {
  buildGlossarySearchRequest,
  createDefaultGlossaryBrowseFilters,
  fetchGlossarySearchPage,
  GLOSSARY_EXPORT_FORMAT_OPTIONS,
  GLOSSARY_STATUS_FILTER_OPTIONS,
  requestGlossaryExportDownload,
  type GlossaryBrowseFilters,
  type GlossaryExportFormat,
  type GlossaryTableRow
} from "@utils/glossaryExport";
import { Item } from "@utils/Muiutils";
import { serverError } from "@utils/Utils";
import Autocomplete from "@mui/material/Autocomplete";
import DownloadOutlinedIcon from "@mui/icons-material/DownloadOutlined";
import ArrowDropDownIcon from "@mui/icons-material/ArrowDropDown";
import RefreshIcon from "@mui/icons-material/Refresh";
import TextSnippetOutlinedIcon from "@mui/icons-material/TextSnippetOutlined";
import {
  CircularProgress,
  FormControl,
  Grid,
  InputLabel,
  Menu,
  MenuItem,
  Select,
  type SelectChangeEvent,
  Stack,
  TextField,
  Typography
} from "@mui/material";
import moment from "moment-timezone";
import {
  ChangeEvent,
  MouseEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState
} from "react";
import { toast } from "react-toastify";
import { createGlossaryTermsListColumns } from "./glossaryTermsListColumns";

const FILTER_DEBOUNCE_MS = 400;

const GlossaryTermsListPage = () => {
  const toastRef = useRef<string | number | undefined>(undefined);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const filtersRef = useRef<GlossaryBrowseFilters>(
    createDefaultGlossaryBrowseFilters()
  );
  const glossaryGuidByNameRef = useRef<Record<string, string>>({});
  const dispatch = useAppDispatch();
  const { glossaryData, loading: glossaryLoading } = useAppSelector(
    (state) => state.glossary
  );

  const [loading, setLoading] = useState(true);
  const [downloading, setDownloading] = useState(false);
  const [tableRows, setTableRows] = useState<GlossaryTableRow[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [pageCount, setPageCount] = useState(0);
  const [filters, setFilters] = useState<GlossaryBrowseFilters>(
    createDefaultGlossaryBrowseFilters()
  );
  const [downloadMenuAnchor, setDownloadMenuAnchor] =
    useState<null | HTMLElement>(null);
  const [tableKey, setTableKey] = useState(0);
  const [updateTable, setUpdateTable] = useState(moment.now());

  filtersRef.current = filters;

  useEffect(() => {
    if (!glossaryData && !glossaryLoading) {
      void dispatch(fetchGlossaryData());
    }
  }, [dispatch, glossaryData, glossaryLoading]);

  const glossaryGuidByName = useMemo(() => {
    const map: Record<string, string> = {};
    const glossaries = Array.isArray(glossaryData) ? glossaryData : [];
    glossaries.forEach((g: { name?: string; guid?: string }) => {
      if (g?.name && g?.guid) {
        map[g.name] = g.guid;
      }
    });
    return map;
  }, [glossaryData]);

  glossaryGuidByNameRef.current = glossaryGuidByName;

  const glossaryOptions = useMemo(() => {
    const glossaries = Array.isArray(glossaryData) ? glossaryData : [];
    return glossaries
      .map((g: { name?: string }) => g?.name ?? "")
      .filter((name) => name !== "")
      .sort((a, b) => a.localeCompare(b));
  }, [glossaryData]);

  const scheduleFilterRefresh = useCallback(() => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }
    debounceRef.current = setTimeout(() => {
      setTableKey((k) => k + 1);
    }, FILTER_DEBOUNCE_MS);
  }, []);

  const fetchGlossaryTableData = useCallback(
    async ({
      pagination
    }: {
      pagination: { pageIndex: number; pageSize: number };
    }) => {
      setLoading(true);
      try {
        const request = buildGlossarySearchRequest(
          filtersRef.current,
          pagination.pageIndex,
          pagination.pageSize,
          glossaryGuidByNameRef.current
        );
        const { rows, totalCount: count } = await fetchGlossarySearchPage(
          request
        );
        setTableRows(rows);
        setTotalCount(count);
        setPageCount(
          Math.ceil(count / (pagination.pageSize || 25)) || 0
        );
      } catch (e) {
        serverError(e, toastRef);
        setTableRows([]);
        setTotalCount(0);
        setPageCount(0);
      } finally {
        setLoading(false);
      }
    },
    [updateTable]
  );

  const handleRefresh = () => {
    setUpdateTable(moment.now());
  };

  const handleDownloadMenuOpen = (event: MouseEvent<HTMLButtonElement>) => {
    setDownloadMenuAnchor(event.currentTarget);
  };

  const handleDownloadMenuClose = () => {
    setDownloadMenuAnchor(null);
  };

  const handleDownload = async (format: GlossaryExportFormat) => {
    handleDownloadMenuClose();
    try {
      setDownloading(true);
      await requestGlossaryExportDownload(
        filtersRef.current,
        glossaryGuidByNameRef.current,
        format
      );
      toast.dismiss(toastRef.current);
      toastRef.current = toast.success(
        "The current glossary export has been enqueued for download. You can access the file by clicking the download icon at the top of the page."
      );
    } catch (e) {
      serverError(e, toastRef);
    } finally {
      setDownloading(false);
    }
  };

  const handleSearchTextChange = (
    e: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const v = e.target.value;
    setFilters((f) => ({ ...f, searchText: v }));
    scheduleFilterRefresh();
  };

  const handleRecordTypeChange = (e: SelectChangeEvent<string>) => {
    setFilters((f) => ({
      ...f,
      recordType: e.target.value as GlossaryBrowseFilters["recordType"]
    }));
    setTableKey((k) => k + 1);
  };

  const handleStatusFilterChange = (e: SelectChangeEvent<string>) => {
    setFilters((f) => ({ ...f, statusFilter: e.target.value }));
    setTableKey((k) => k + 1);
  };

  const downloadDisabled = loading || downloading || totalCount === 0;

  const columns = useMemo(
    () => createGlossaryTermsListColumns(setUpdateTable),
    [setUpdateTable]
  );

  return (
    <Item
      variant="outlined"
      className="glossary-terms-list-page"
      sx={{ background: "white", width: "100%", textAlign: "left" }}
    >
      <Stack spacing={0} sx={{ height: "100%", overflow: "hidden" }}>
        <Stack
          direction="row"
          alignItems="center"
          justifyContent="space-between"
          flexWrap="wrap"
          gap={1}
          className="glossary-terms-list-header"
        >
          <Stack direction="row" alignItems="center" gap={1}>
            <TextSnippetOutlinedIcon color="primary" />
            <Typography
              variant="h6"
              component="h1"
              className="glossary-terms-list-title"
            >
              Glossary terms &amp; categories
            </Typography>
          </Stack>
          <Stack direction="row" gap={1} flexWrap="wrap" alignItems="center">
            <LightTooltip title="Reload from server">
              <span>
                <CustomButton
                  variant="outlined"
                  size="small"
                  disabled={loading}
                  onClick={handleRefresh}
                  startIcon={<RefreshIcon />}
                  data-cy="glossaryTermsListRefresh"
                >
                  Refresh
                </CustomButton>
              </span>
            </LightTooltip>
            <CustomButton
              variant="outlined"
              size="small"
              onClick={handleDownloadMenuOpen}
              disabled={downloadDisabled}
              startIcon={
                downloading ? (
                  <CircularProgress size={16} color="inherit" />
                ) : (
                  <DownloadOutlinedIcon />
                )
              }
              endIcon={downloading ? undefined : <ArrowDropDownIcon />}
              aria-haspopup="true"
              aria-expanded={Boolean(downloadMenuAnchor)}
              aria-controls={
                downloadMenuAnchor ? "glossary-download-menu" : undefined
              }
              data-cy="glossaryTermsListDownload"
            >
              Download
            </CustomButton>
            <Menu
              id="glossary-download-menu"
              anchorEl={downloadMenuAnchor}
              open={Boolean(downloadMenuAnchor)}
              onClose={handleDownloadMenuClose}
              anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
              transformOrigin={{ vertical: "top", horizontal: "right" }}
            >
              {GLOSSARY_EXPORT_FORMAT_OPTIONS.map((option) => (
                <MenuItem
                  key={option.value}
                  onClick={() => void handleDownload(option.value)}
                  data-cy={`glossaryTermsListDownload${option.value}`}
                >
                  {option.label}
                </MenuItem>
              ))}
            </Menu>
          </Stack>
        </Stack>

        <div className="glossary-terms-filters">
          <Typography
            variant="subtitle2"
            className="glossary-terms-filters-label"
          >
            Filters
          </Typography>
          <Grid container spacing={2}>
            <Grid item xs={12} sm={6} md={3}>
              <Autocomplete
                size="small"
                fullWidth
                options={glossaryOptions}
                value={filters.glossaryName || null}
                onChange={(_e, v) => {
                  setFilters((f) => ({ ...f, glossaryName: v ?? "" }));
                  setTableKey((k) => k + 1);
                }}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    label="Glossary"
                    placeholder="All glossaries"
                  />
                )}
                data-cy="glossaryTermsFilterGlossary"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <FormControl size="small" fullWidth>
                <InputLabel id="glossary-record-type-label">
                  Record type
                </InputLabel>
                <Select
                  labelId="glossary-record-type-label"
                  label="Record type"
                  value={filters.recordType}
                  onChange={handleRecordTypeChange}
                  data-cy="glossaryTermsFilterRecordType"
                >
                  <MenuItem value="all">All</MenuItem>
                  <MenuItem value="Term">Terms only</MenuItem>
                  <MenuItem value="Category">Categories only</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <FormControl size="small" fullWidth>
                <InputLabel id="glossary-status-filter-label">Status</InputLabel>
                <Select
                  labelId="glossary-status-filter-label"
                  label="Status"
                  value={filters.statusFilter}
                  onChange={handleStatusFilterChange}
                  data-cy="glossaryTermsFilterStatus"
                >
                  {GLOSSARY_STATUS_FILTER_OPTIONS.map((option) => (
                    <MenuItem key={option.label} value={option.value}>
                      {option.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <TextField
                size="small"
                fullWidth
                label="Search all columns"
                value={filters.searchText}
                onChange={handleSearchTextChange}
                data-cy="glossaryTermsFilterSearch"
              />
            </Grid>
          </Grid>
        </div>

        <div className="glossary-terms-table search-result-table-wrapper">
          <TableLayout
            key={`glossary-terms-table-${tableKey}`}
            fetchData={fetchGlossaryTableData}
            data={tableRows}
            columns={columns}
            emptyText="No rows match the current filters."
            isFetching={loading}
            pageCount={pageCount}
            totalCount={totalCount}
            isClientSidePagination={false}
            clientSideSorting={true}
            showPagination={true}
            showGoToPage={true}
            showRowSelection={false}
            columnVisibility={false}
            columnSort={true}
            tableFilters={false}
            queryBuilder={false}
            allTableFilters={false}
            isfilterQuery={false}
            setUpdateTable={setUpdateTable}
          />
        </div>
      </Stack>
    </Item>
  );
};

export default GlossaryTermsListPage;
