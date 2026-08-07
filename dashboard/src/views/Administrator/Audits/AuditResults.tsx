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

import { Grid, Link, List, ListItem, ListItemText, Typography, Box, Drawer, IconButton, Stack, Tooltip, TextField, InputAdornment, CircularProgress, Pagination, PaginationItem, Skeleton } from "@mui/material";
import KeyboardDoubleArrowLeftIcon from "@mui/icons-material/KeyboardDoubleArrowLeft";
import KeyboardDoubleArrowRightIcon from "@mui/icons-material/KeyboardDoubleArrowRight";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import SearchIcon from "@mui/icons-material/Search";
import { auditAction, category, AuditOperation, PurgeActiveView } from "@utils/Enum";
import { isEmpty, jsonParse } from "@utils/Utils";
import { useVirtualization } from "@hooks/useVirtualization";
import CustomModal from "@components/Modal";
import TypeDefAuditDetailModal from "@components/TypeDefAuditDetailModal";
import { useRef, useState, useEffect } from "react";
import AuditsTab from "@views/DetailPage/EntityDetailTabs/AuditsTab";
import ImportExportAudits from "./ImportExportAudits";
import { LightTooltip } from "@components/muiComponents";
import { fetchApi } from "@api/apiMethods/fetchApi";
import "./AuditResults.scss";
interface AuditEntry {
  guid: string;
  operation: string;
  params?: string;
  result?: string;
  runId?: string;
  [key: string]: unknown;
}

interface AuditResultsProps {
  componentProps?: {
    auditData?: AuditEntry[];
  };
  row: {
    original: {
      guid: string;
      runId?: string;
      [key: string]: unknown;
    };
  };
}

const AuditResults = ({ componentProps, row }: AuditResultsProps) => {
  const { auditData } = componentProps || {};
  const [openModal, setOpenModal] = useState<boolean>(false);
  const [openPurgeModal, setOpenPurgeModal] = useState<boolean>(false);
  const [currentResultObj, setCurrentObj] = useState<Record<string, unknown> | undefined>();
  // Stores the guid of the clicked purged entity
  const [currentPurgeResultObj, setCurrentPurgeResultObj] = useState<string | undefined>();
  const [activePurgeView, setActivePurgeView] = useState<PurgeActiveView>(PurgeActiveView.NONE);
  const [drawerSearchText, setDrawerSearchText] = useState<string>('');
  const [drawerPage, setDrawerPage] = useState<number>(1);
  const [drawerPageSize, setDrawerPageSize] = useState<number>(25);
  const [drawerPageSizeInput, setDrawerPageSizeInput] = useState<string>('25');
  const [scrollTop, setScrollTop] = useState<number>(0);
  const [copiedRunId, setCopiedRunId] = useState<boolean>(false);
  const [purgedApiGuids, setPurgedApiGuids] = useState<string[]>([]);
  const [loadingPurgedApi, setLoadingPurgedApi] = useState<boolean>(false);
  const [purgedTotalCount, setPurgedTotalCount] = useState<number>(0);
  const [summaryData, setSummaryData] = useState<Record<string, unknown> | null>(null);
  const [loadingSummary, setLoadingSummary] = useState<boolean>(false);
  const drawerScrollTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);


  const handleCloseModal = () => {
    setOpenModal(false);
  };
  const handleClosePurgeModal = () => {
    setOpenPurgeModal(false);
  };

  const auditObj: AuditEntry | undefined = !isEmpty(auditData)
    ? (auditData as AuditEntry[]).find((obj) => obj.guid === row.original.guid)
    : undefined;

  const operation = auditObj?.operation ?? '';
  const params = auditObj?.params;
  const result = auditObj?.result;

  let isPurgeOperation = operation === AuditOperation.PURGE || operation === AuditOperation.AUTO_PURGE;
  const summaryGuid = auditObj?.guid ?? row.original.guid;

  useEffect(() => {
    if (isPurgeOperation && summaryGuid) {
      setLoadingSummary(true);
      fetchApi(`/api/atlas/admin/audit/${summaryGuid}/summary`, {
        method: "GET",
        headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' }
      })
        .then(res => {
          if (res.data && typeof res.data === 'object') {
            setSummaryData(res.data);
          }
        })
        .catch(err => {
          console.error("Failed to fetch purge summary", err);
        })
        .finally(() => {
          setLoadingSummary(false);
        });
    }
  }, [isPurgeOperation, summaryGuid]);

  let summary: Record<string, unknown> = summaryData || {};
  let requestedEntitiesList: string[] = [];
  let legacyPurgedList: string[] = [];

  if (isPurgeOperation) {
    if (!summaryData) {
      try {
        const parsed = typeof result === "string" ? JSON.parse(result) : result;
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          summary = (parsed as Record<string, unknown>).summary
            ? (parsed as Record<string, unknown>).summary as Record<string, unknown>
            : parsed as Record<string, unknown>;
        } else if (Array.isArray(parsed)) {
          legacyPurgedList = (parsed as unknown[]).map((item) =>
            typeof item === "string" ? item : (item as { guid?: string }).guid || String(item)
          );
        }
      } catch (_e) {
        if (typeof result === "string" && !result.startsWith("{")) {
          legacyPurgedList = result.replace(/^\[|\]$/g, "").split(",").map(s => s.trim()).filter(Boolean);
        }
      }
    } else {
      if (typeof result === "string" && !result.startsWith("{")) {
        legacyPurgedList = result.replace(/^\[|\]$/g, "").split(",").map(s => s.trim()).filter(Boolean);
      }
    }

    if (params) {
      try {
        const parsedParams = JSON.parse(params);
        if (Array.isArray(parsedParams)) {
          requestedEntitiesList = parsedParams as string[];
        } else if (typeof params === "string") {
          requestedEntitiesList = params.replace(/^\[|\]$/g, "").split(",").map(s => s.trim()).filter(Boolean);
        }
      } catch (_e) {
        requestedEntitiesList = typeof params === "string"
          ? params.replace(/^\[|\]$/g, "").split(",").map(s => s.trim()).filter(Boolean)
          : [];
      }
    }
  } else {
    try {
      summary = jsonParse(result) as Record<string, unknown>;
    } catch (_e) {
      summary = {};
    }
  }

  const runId = (row.original.runId as string | undefined)
    ?? (summary?.runId as string | undefined)
    ?? (auditObj?.runId as string | undefined)
    ?? 'N/A';

  const isSummaryRow = (runId !== 'N/A') && isPurgeOperation;

  const requestedCount = (summary?.requestedCount as number | undefined) ?? requestedEntitiesList.length;
  const purgedCount = (summary?.purgedCount as number | undefined) ?? legacyPurgedList.length;
  const purgedDependenciesCount = (summary?.purgedDependenciesCount as number | undefined) ?? 0;
  const totalPurgedCount = (purgedCount as number) + (purgedDependenciesCount as number);
  const failedCount = (summary?.failedCount as number | undefined) ?? 0;
  const failedDependenciesCount = (summary?.failedDependenciesCount as number | undefined) ?? 0;
  const totalFailedCount = failedCount + failedDependenciesCount;
  const skippedCount = (summary?.skippedCount as number | undefined) ?? 0;
  const executionFailed = (summary?.executionFailed as boolean | undefined) || (totalFailedCount) > 0;

  // Fetching purged entities from an API is disabled for now.
  // We simply use the raw `result` string as requested.
  const fetchPurged = () => {
    // Disabled. The UI will just use the `result` string.
  };

  // Handle clicking Total Purged card: opens drawer and fetches first page
  const handleOpenPurgedDrawer = () => {
    if (totalPurgedCount === 0) return;
    setPurgedTotalCount(totalPurgedCount);
    setActivePurgeView(PurgeActiveView.PURGED);
    setDrawerPage(1);
    setScrollTop(0);
    // As requested, Total Purged simply uses the raw `result` object string (legacyPurgedList)
    setPurgedApiGuids(legacyPurgedList);
    setLoadingPurgedApi(false);
  };

  return (
    <>
      <TypeDefAuditDetailModal
        open={openModal}
        onClose={handleCloseModal}
        detailObject={currentResultObj ?? null}
        maxWidth="md"
      />

      <CustomModal
        open={openPurgeModal}
        onClose={handleClosePurgeModal}
        title={`Purged Entity Details: ${currentPurgeResultObj}`}
        button1Handler={undefined}
        button2Handler={undefined}
        maxWidth="md"
        footer={false}
      >
        <AuditsTab auditResultGuid={currentPurgeResultObj} />
      </CustomModal>

      {operation === "TYPE_DEF_CREATE" ||
        operation === "TYPE_DEF_UPDATE" ||
        operation === "TYPE_DEF_DELETE" ? (
        <List className="audit-results-list">
          {summary &&
            Object.keys(summary).map((key: string) => {
              const rawItems = summary[key];
              const items: Array<Record<string, unknown> | string> = Array.isArray(rawItems)
                ? (rawItems as Array<Record<string, unknown> | string>)
                : [];
              return (
                <div key={key}>
                  <Typography className="audit-list-header">
                    {`${category[key as keyof typeof category] || key} ${auditAction[operation as keyof typeof auditAction] || operation}`}
                  </Typography>
                  {items.map((obj: Record<string, unknown> | string, idx: number) => {
                    const name = typeof obj === 'object' && obj !== null
                      ? (obj.name as string) || String(obj)
                      : String(obj);
                    return (
                      <ListItem key={name + idx} className="audit-results-list-item">
                        <ListItemText
                          primary={
                            <Link
                              className="audit-results-entityid audit-list-link"
                              component="button"
                              variant="body2"
                              onClick={() => {
                                setOpenModal(true);
                                setCurrentObj(typeof obj === "object" ? obj : { name: obj });
                              }}
                              title={name}
                            >
                              {name}
                            </Link>
                          }
                        />
                      </ListItem>
                    );
                  })}
                </div>
              );
            })}
        </List>
      ) : operation === "IMPORT" || operation === "EXPORT" ? (
        <ImportExportAudits auditObj={auditObj} />
      ) : !isPurgeOperation ? (
        <Typography>No Results Found</Typography>
      ) : null}

      {/* Purge Audit View */}
      {isPurgeOperation ? (
        <Box className="purge-audit-view">
          {loadingSummary && Object.keys(summary).length === 0 && legacyPurgedList.length === 0 && !result ? (
            <Box sx={{ p: 2 }}>
              <Skeleton variant="text" width="40%" height={30} sx={{ mb: 2 }} />
              <Grid container spacing={2}>
                <Grid item xs={6} sm={3}><Skeleton variant="rectangular" height={70} sx={{ borderRadius: 1 }} /></Grid>
                <Grid item xs={6} sm={3}><Skeleton variant="rectangular" height={70} sx={{ borderRadius: 1 }} /></Grid>
                <Grid item xs={6} sm={3}><Skeleton variant="rectangular" height={70} sx={{ borderRadius: 1 }} /></Grid>
                <Grid item xs={6} sm={3}><Skeleton variant="rectangular" height={70} sx={{ borderRadius: 1 }} /></Grid>
              </Grid>
            </Box>
          ) : (
            <Box className="purge-summary-container">

              {/* Run Id Header with Copy Action */}
              {runId !== 'N/A' && (
                <Box className="purge-runid-header">
                  <Typography variant="body2" color="textSecondary" className="runid-text">
                    <strong>Run Id:</strong> {runId}
                  </Typography>
                  <Tooltip title={copiedRunId ? "Copied!" : "Copy Run Id"}>
                    <IconButton
                      size="small"
                      onClick={() => {
                        if (navigator.clipboard) {
                          navigator.clipboard.writeText(runId);
                        } else {
                          const textField = document.createElement('textarea');
                          textField.innerText = runId;
                          document.body.appendChild(textField);
                          textField.select();
                          document.execCommand('copy');
                          textField.remove();
                        }
                        setCopiedRunId(true);
                        setTimeout(() => setCopiedRunId(false), 2000);
                      }}
                      className="purge-runid-copy"
                    >
                      <ContentCopyIcon className={`copy-icon ${copiedRunId ? "copied" : ""}`} />
                    </IconButton>
                  </Tooltip>
                </Box>
              )}

              {/* 4 Cards Grid: Requested, Total Purged, Failed (Display Only), Skipped (Display Only) */}
              <Grid container spacing={2}>
                {/* 1. Clickable Requested Card */}
                {isSummaryRow && (
                  <Grid item xs={6} sm={3}>
                    <Box
                      onClick={() => {
                        setActivePurgeView(PurgeActiveView.REQUESTED);
                        setDrawerPage(1);
                        setScrollTop(0);
                      }}
                      className="purge-card purge-card-requested"
                    >
                      <Typography variant="caption" color="primary.main" display="block" className="card-title">
                        Requested
                      </Typography>
                      <Typography variant="h5" color="primary.main" className="card-count">
                        {requestedCount}
                      </Typography>
                    </Box>
                  </Grid>
                )}

                {/* 2. Clickable Total Purged Card */}
                <Grid item xs={isSummaryRow ? 6 : 12} sm={isSummaryRow ? 3 : 4}>
                  <Box
                    onClick={handleOpenPurgedDrawer}
                    className={`purge-card purge-card-purged ${totalPurgedCount > 0 ? "clickable" : ""}`}
                  >
                    <Typography variant="caption" color="success.main" display="block" className="card-title">
                      PURGED
                    </Typography>
                    <Typography variant="h5" color="success.main" className="card-count">
                      {totalPurgedCount}
                    </Typography>
                  </Box>
                </Grid>

                {/* 3 & 4. Display-Only Failed and Skipped Cards */}
                {isSummaryRow && (
                  <>
                    <Grid item xs={6} sm={3}>
                      <LightTooltip
                        title={
                          totalFailedCount > 0 || executionFailed
                            ? "Some entities failed to purge. Please check ${atlas.log.dir}/purgefailure.log for details."
                            : "No failed entities during this purge operation."
                        }
                        arrow
                        placement="top"
                      >
                        <Box
                          className={`purge-card ${totalFailedCount > 0 ? "purge-card-failed" : "purge-card-failed-empty"}`}
                        >
                          <Typography variant="caption" color={totalFailedCount > 0 ? "error.main" : "textSecondary"} display="block" className="card-title">
                            Failed
                          </Typography>
                          <Typography variant="h5" color={totalFailedCount > 0 ? "error.main" : "textPrimary"} className="card-count">
                            {totalFailedCount}
                          </Typography>
                        </Box>
                      </LightTooltip>
                    </Grid>

                    {/* 4. Display-Only Skipped Card */}
                    <Grid item xs={6} sm={3}>
                      <LightTooltip
                        title={
                          skippedCount > 0 || executionFailed
                            ? "Some entities were skipped during purge. Please check ${atlas.log.dir}/purgefailure.log for details."
                            : "No skipped entities during this purge operation."
                        }
                        arrow
                        placement="top"
                      >
                        <Box
                          className={`purge-card ${skippedCount > 0 ? "purge-card-skipped" : "purge-card-skipped-empty"}`}
                        >
                          <Typography variant="caption" color={skippedCount > 0 ? "warning.main" : "textSecondary"} display="block" className="card-title">
                            Skipped
                          </Typography>
                          <Typography variant="h5" color={skippedCount > 0 ? "warning.main" : "textPrimary"} className="card-count">
                            {skippedCount}
                          </Typography>
                        </Box>
                      </LightTooltip>
                    </Grid>
                  </>
                )}
              </Grid>
            </Box>
          )}

          {/* Right Side Drawer — server-side pagination for Purged, client-side for Requested */}
          <PurgeEntitiesDrawer
            activePurgeView={activePurgeView}
            setActivePurgeView={setActivePurgeView}
            isSummaryRow={isSummaryRow}
            requestedEntitiesList={requestedEntitiesList}
            purgedApiGuids={purgedApiGuids}
            drawerSearchText={drawerSearchText}
            setDrawerSearchText={setDrawerSearchText}
            drawerPage={drawerPage}
            setDrawerPage={setDrawerPage}
            drawerPageSize={drawerPageSize}
            setDrawerPageSize={setDrawerPageSize}
            scrollTop={scrollTop}
            setScrollTop={setScrollTop}
            purgedTotalCount={purgedTotalCount}
            drawerScrollTimerRef={drawerScrollTimerRef}
            loadingPurgedApi={loadingPurgedApi}
            fetchPurged={fetchPurged}
            runId={runId}
            copiedRunId={copiedRunId}
            setCopiedRunId={setCopiedRunId}
            setOpenPurgeModal={setOpenPurgeModal}
            setCurrentPurgeResultObj={setCurrentPurgeResultObj}
            drawerPageSizeInput={drawerPageSizeInput}
            setDrawerPageSizeInput={setDrawerPageSizeInput}
          />
        </Box>
      ) : null}
    </>
  );
};


interface PurgeEntitiesDrawerProps {
  activePurgeView: PurgeActiveView;
  setActivePurgeView: (view: PurgeActiveView) => void;
  isSummaryRow: boolean;
requestedEntitiesList: string[];
  purgedApiGuids: string[];
  drawerSearchText: string;
  setDrawerSearchText: (text: string) => void;
  drawerPage: number;
  setDrawerPage: React.Dispatch<React.SetStateAction<number>>;
  drawerPageSize: number;
  setDrawerPageSize: React.Dispatch<React.SetStateAction<number>>;
  scrollTop: number;
  setScrollTop: React.Dispatch<React.SetStateAction<number>>;
  purgedTotalCount: number;
  drawerScrollTimerRef: React.MutableRefObject<ReturnType<typeof setTimeout> | null>;
  loadingPurgedApi: boolean;
  fetchPurged: (append: boolean, limitOverride?: number) => void;
  runId: string;
  copiedRunId: boolean;
  setCopiedRunId: (copied: boolean) => void;
  setOpenPurgeModal: (open: boolean) => void;
  setCurrentPurgeResultObj: (guid: string) => void;
  drawerPageSizeInput: string;
  setDrawerPageSizeInput: (input: string) => void;
}

const PurgeEntitiesDrawer: React.FC<PurgeEntitiesDrawerProps> = ({
  activePurgeView,
  setActivePurgeView,
  requestedEntitiesList,
  purgedApiGuids,
  drawerSearchText,
  setDrawerSearchText,
  drawerPage,
  setDrawerPage,
  drawerPageSize,
  setDrawerPageSize,
  scrollTop,
  setScrollTop,
  loadingPurgedApi,
  runId,
  copiedRunId,
  setCopiedRunId,
  setOpenPurgeModal,
  setCurrentPurgeResultObj,
  drawerPageSizeInput,
  setDrawerPageSizeInput,
}) => {
  const listRef = useRef<HTMLUListElement | null>(null);

  useEffect(() => {
    if (listRef.current) {
      listRef.current.scrollTop = 0;
    }
  }, [drawerPage, drawerSearchText, activePurgeView]);

  const isPurgedView = activePurgeView === PurgeActiveView.PURGED;
  const rawListForView: any[] = activePurgeView === PurgeActiveView.REQUESTED
    ? requestedEntitiesList
    : purgedApiGuids;

  const filteredList = rawListForView.filter((item: any) => {
    if (!drawerSearchText) return true;
    const guidStr = typeof item === 'object' && item !== null ? item.guid : item;
    const nameStr = typeof item === 'object' && item !== null ? item.attributes?.name : '';
    const searchLower = drawerSearchText.trim().toLowerCase();
    return (guidStr && guidStr.toLowerCase().includes(searchLower)) ||
      (nameStr && nameStr.toLowerCase().includes(searchLower));
  });

  const displayItems = filteredList.slice((drawerPage - 1) * drawerPageSize, drawerPage * drawerPageSize);

  const { visibleItems, paddingTop, paddingBottom, startIndex } = useVirtualization({
    items: displayItems,
    scrollTop,
    itemHeight: 24
  });

  const displayTotal = filteredList.length;

  const handleDrawerScroll = (e: React.UIEvent<HTMLUListElement>) => {
    setScrollTop(e.currentTarget.scrollTop);
  };

  return (
    <Drawer
      anchor="right"
      open={activePurgeView !== PurgeActiveView.NONE}
      onClose={() => {
        setActivePurgeView(PurgeActiveView.NONE);
        setDrawerSearchText('');
        setDrawerPage(1);
        setScrollTop(0);
      }}
      PaperProps={{ className: "drawer-paper" }}
    >
      <Box className="drawer-content-wrapper">
        <Stack direction="row" justifyContent="space-between" alignItems="center" className="drawer-search-container">
          <Typography className="drawer-title">
            {activePurgeView === PurgeActiveView.REQUESTED ? 'Requested Entities' : 'Purged Entities'}
          </Typography>
          <IconButton
            onClick={() => {
              setActivePurgeView(PurgeActiveView.NONE);
              setDrawerSearchText('');
              setDrawerPage(1);
              setScrollTop(0);
            }}
            size="small"
          >
            ✕
          </IconButton>
        </Stack>

        {runId !== 'N/A' && (
          <Box className="drawer-runid-container">
            <Typography variant="caption" color="textSecondary" className="drawer-runid-text">
              <strong>Run Id:</strong> {runId}
            </Typography>
            <Tooltip title={copiedRunId ? "Copied!" : "Copy Run Id"}>
              <IconButton
                size="small"
                onClick={() => {
                  if (navigator.clipboard) {
                    navigator.clipboard.writeText(runId);
                  } else {
                    const textField = document.createElement('textarea');
                    textField.innerText = runId;
                    document.body.appendChild(textField);
                    textField.select();
                    document.execCommand('copy');
                    textField.remove();
                  }
                  setCopiedRunId(true);
                  setTimeout(() => setCopiedRunId(false), 2000);
                }}
                className="drawer-copy-btn"
              >
                <ContentCopyIcon className={`drawer-copy-icon ${copiedRunId ? "copied" : ""}`} />
              </IconButton>
            </Tooltip>
          </Box>
        )}

        {(activePurgeView === 'requested' ? requestedEntitiesList.length > 0 : purgedApiGuids.length > 0 || loadingPurgedApi) && (
          <Box className="drawer-search-container">
            <TextField
              fullWidth
              size="small"
              variant="standard"
              placeholder="Search GUIDs..."
              value={drawerSearchText}
              onChange={(e) => {
                setDrawerSearchText(e.target.value);
                setDrawerPage(1);
                setScrollTop(0);
              }}
              InputProps={{
                disableUnderline: true,
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon className="drawer-search-icon" />
                  </InputAdornment>
                ),
                endAdornment: drawerSearchText ? (
                  <InputAdornment position="end">
                    <IconButton size="small" onClick={() => {
                      setDrawerSearchText('');
                      setDrawerPage(1);
                      setScrollTop(0);
                    }}>
                      ✕
                    </IconButton>
                  </InputAdornment>
                ) : null
              }}
              className="drawer-search-input"
            />
          </Box>
        )}



        {isPurgedView && loadingPurgedApi && purgedApiGuids.length === 0 ? (
          <Box className="drawer-loader">
            <CircularProgress size={30} />
          </Box>
        ) : (
          <List dense className="drawer-list-container" onScroll={handleDrawerScroll} onWheel={handleDrawerScroll} ref={listRef}>
            {(() => {
              if (displayItems.length === 0) {
                return (
                  <Typography variant="body2" color="textSecondary" className="drawer-list-empty">
                    No matching GUIDs found
                  </Typography>
                );
              }

              return (
                <>
                  {paddingTop > 0 && <div style={{ height: paddingTop }} />}
                  {visibleItems.map((item: string | Record<string, any>, localIndex: number) => {
                    const index = startIndex + localIndex;
                    const globalIndex = (drawerPage - 1) * drawerPageSize + index + 1;
                    const isObj = typeof item === 'object' && item !== null;
                    const guidStr = isObj ? item.guid : item;
                    return (
                      <ListItem key={guidStr + index} className="drawer-list-item">
                        <Typography variant="body2" className="drawer-list-index">{globalIndex}.</Typography>
                        <Link
                          component="button"
                          variant="body2"
                          underline="hover"
                          onClick={() => {
                            setOpenPurgeModal(true);
                            setCurrentPurgeResultObj(guidStr);
                          }}
                          title={guidStr}
                          className="drawer-list-link"
                        >
                          {guidStr}
                        </Link>
                      </ListItem>
                    );
                  })}
                  {paddingBottom > 0 && <div style={{ height: paddingBottom }} />}
                </>
              );
            })()}


          </List>
        )}

        {displayTotal > 0 && (
          <Box className="drawer-footer">
            <Typography variant="caption" color="textSecondary" className="drawer-footer-count">
              {Math.min((drawerPage - 1) * drawerPageSize + 1, displayTotal)}-{Math.min(drawerPage * drawerPageSize, displayTotal)} of {displayTotal}
            </Typography>

            <Box className="drawer-pagination-container">
              <Pagination
                count={Math.ceil(displayTotal / drawerPageSize) || 1}
                page={drawerPage}
                onChange={(_e, val) => {
                  setDrawerPage(val);
                  setScrollTop(0);
                }}
                size="small"
                color="primary"
                siblingCount={0}
                boundaryCount={0}
                showFirstButton
                showLastButton
                className="purge-pagination"
                renderItem={(item) => (
                  <PaginationItem
                    slots={{ first: KeyboardDoubleArrowLeftIcon, last: KeyboardDoubleArrowRightIcon }}
                    {...item}
                  />
                )}
              />

              <Box className="drawer-limit-container">
                <Typography variant="caption" color="textSecondary" className="drawer-limit-label">Limit</Typography>
                <Box
                  component="input"
                  type="number"
                  min={1}
                  value={drawerPageSizeInput}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                    setDrawerPageSizeInput(e.target.value);
                  }}
                  onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => {
                    if (e.key === 'Enter') {
                      const parsed = parseInt((e.target as HTMLInputElement).value, 10);
                      if (Number.isFinite(parsed) && parsed > 0) {
                        const clamped = Math.min(parsed, displayTotal);
                        setDrawerPageSize(clamped);
                        setDrawerPageSizeInput(String(clamped));
                        setDrawerPage(1);
                        setScrollTop(0);
                      }
                    }
                  }}
                  className="drawer-limit-input"
                />
              </Box>
            </Box>
          </Box>
        )}
      </Box>
    </Drawer>
  );
};

export default AuditResults;
