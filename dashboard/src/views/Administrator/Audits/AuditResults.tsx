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

import { Grid, Link, List, ListItem, ListItemText, Typography, Divider, Alert, AlertTitle, Box, Drawer, IconButton, Stack, Tooltip, TextField, InputAdornment, CircularProgress } from "@mui/material";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import SearchIcon from "@mui/icons-material/Search";
import { auditAction, category, AuditOperation, PurgeActiveView } from "@utils/Enum";
import { isEmpty, jsonParse } from "@utils/Utils";
import { useVirtualization } from '@hooks/useVirtualization';
import CustomModal from "@components/Modal";
import TypeDefAuditDetailModal from "@components/TypeDefAuditDetailModal";
import { useRef, useState } from "react";
import AuditsTab from "@views/DetailPage/EntityDetailTabs/AuditsTab";
import ImportExportAudits from "./ImportExportAudits";
import { LightTooltip } from '@components/muiComponents';

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
  const [drawerPageSize, setDrawerPageSize] = useState<number>(10);
  const [drawerPageSizeInput, setDrawerPageSizeInput] = useState<string>('10');
  const [scrollTop, setScrollTop] = useState<number>(0);
  const [copiedRunId, setCopiedRunId] = useState<boolean>(false);
  const [purgedApiGuids, setPurgedApiGuids] = useState<string[]>([]);
  const [loadingPurgedApi, setLoadingPurgedApi] = useState<boolean>(false);
  const [purgedTotalCount, setPurgedTotalCount] = useState<number>(0);
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
  let summary: Record<string, unknown> = {};
  let requestedEntitiesList: string[] = [];
  let legacyPurgedList: string[] = [];

  if (isPurgeOperation) {
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
    } catch (e) {
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
      } catch (e) {
        requestedEntitiesList = typeof params === "string"
          ? params.replace(/^\[|\]$/g, "").split(",").map(s => s.trim()).filter(Boolean)
          : [];
      }
    }
  } else {
    try {
      summary = jsonParse(result) as Record<string, unknown>;
    } catch (e) {
      summary = {};
    }
  }

  const summaryGuid = auditObj?.guid ?? row.original.guid;
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
  const skippedCount = (summary?.skippedCount as number | undefined) ?? 0;
  const executionFailed = (summary?.executionFailed as boolean | undefined) || (failedCount as number) > 0;

  // Fetch a page of purged entities server-side using limit & offset, can append to existing
  const fetchPurged = (append: boolean = false, limitOverride?: number) => {
    if (!summaryGuid) return;
    const pageSize = limitOverride ?? drawerPageSize;
    const offset = append ? purgedApiGuids.length : 0;

    setLoadingPurgedApi(true);
    if (!append) {
      setPurgedApiGuids([]);
    }

    fetch(`/api/atlas/admin/audit/${summaryGuid}/purgedEntities?limit=${pageSize}&offset=${offset}`, {
      headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' }
    })
      .then(res => res.ok ? res.json() : [])
      .then((data: string[]) => {
        if (Array.isArray(data)) {
          setPurgedApiGuids(prev => append ? [...prev, ...data] : data);
        }
      })
      .catch(() => { })
      .finally(() => {
        setLoadingPurgedApi(false);
      });
  };

  // Handle clicking Total Purged card: opens drawer and fetches first page
  const handleOpenPurgedDrawer = () => {
    if (totalPurgedCount === 0) return;
    setPurgedTotalCount(totalPurgedCount);
    setActivePurgeView(PurgeActiveView.PURGED);
    setDrawerPage(1);
    setScrollTop(0);
    if (isSummaryRow) {
      fetchPurged(false, drawerPageSize);
    } else {
      setPurgedApiGuids(legacyPurgedList);
      setLoadingPurgedApi(false);
    }
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
                  <Typography sx={{ padding: "1rem 0 0 1rem", textAlign: "left" }}>
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
                              className="audit-results-entityid"
                              component="button"
                              variant="body2"
                              onClick={() => {
                                setOpenModal(true);
                                setCurrentObj(typeof obj === "object" ? obj : { name: obj });
                              }}
                              title={name}
                              sx={{
                                display: "inline-block",
                                maxWidth: "100%",
                                textOverflow: "ellipsis",
                                overflow: "hidden",
                                whiteSpace: "nowrap",
                                textAlign: "left",
                                verticalAlign: "bottom"
                              }}
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
        <Box sx={{ mt: 2, mb: 2 }}>
          <Box sx={{
            p: 2.5,
            borderRadius: 2,
            backgroundColor: '#f0f4f8',
            border: '1px solid rgba(0,0,0,0.08)',
            boxShadow: '0 2px 8px rgba(0,0,0,0.02)'
          }}>

            {/* Run Id Header with Copy Action */}
            {runId !== 'N/A' && (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <Typography variant="body2" color="textSecondary" sx={{ fontFamily: 'monospace' }}>
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
                    sx={{ p: 0.5 }}
                  >
                    <ContentCopyIcon sx={{ fontSize: '15px', color: copiedRunId ? 'success.main' : 'text.secondary' }} />
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
                    sx={{
                      p: 1.5,
                      borderRadius: 2,
                      backgroundColor: '#eff6ff',
                      border: '1px solid #bfdbfe',
                      transition: 'all 0.2s ease-in-out',
                      cursor: 'pointer',
                      boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
                      '&:hover': {
                        transform: 'translateY(-2px)',
                        boxShadow: '0 4px 12px rgba(59,130,246,0.15)',
                        borderColor: '#60a5fa'
                      }
                    }}
                  >
                    <Typography variant="caption" color="primary.main" display="block" sx={{ textTransform: 'uppercase', fontWeight: 'bold', letterSpacing: '0.5px', fontSize: '11px' }}>
                      Requested
                    </Typography>
                    <Typography variant="h5" color="primary.main" sx={{ fontWeight: 'bold', mt: 0.5 }}>
                      {requestedCount}
                    </Typography>
                  </Box>
                </Grid>
              )}

              {/* 2. Clickable Total Purged Card */}
              <Grid item xs={isSummaryRow ? 6 : 12} sm={isSummaryRow ? 3 : 4}>
                <Box
                  onClick={handleOpenPurgedDrawer}
                  sx={{
                    p: 1.5,
                    borderRadius: 2,
                    backgroundColor: '#f0fdf4',
                    border: '1px solid #bbf7d0',
                    transition: 'all 0.2s ease-in-out',
                    cursor: totalPurgedCount > 0 ? 'pointer' : 'default',
                    boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
                    '&:hover': totalPurgedCount > 0 ? {
                      transform: 'translateY(-2px)',
                      boxShadow: '0 4px 12px rgba(34,197,94,0.15)',
                      borderColor: '#4ade80'
                    } : {}
                  }}
                >
                  <Typography variant="caption" color="success.main" display="block" sx={{ textTransform: 'uppercase', fontWeight: 'bold', letterSpacing: '0.5px', fontSize: '11px' }}>
                    {isSummaryRow ? 'Total Purged' : 'Purged Entities'}
                  </Typography>
                  <Typography variant="h5" color="success.main" sx={{ fontWeight: 'bold', mt: 0.5 }}>
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
                        failedCount > 0 || executionFailed
                          ? "Some entities failed to purge. Please check <ATLAS_HOME>/logs/purgefailure.log for details."
                          : "No failed entities during this purge operation."
                      }
                      arrow
                      placement="top"
                    >
                      <Box
                        sx={{
                          p: 1.5,
                          borderRadius: 2,
                          backgroundColor: failedCount > 0 ? '#fef2f2' : '#fafafa',
                          border: failedCount > 0 ? '1px solid #fecaca' : '1px solid rgba(0,0,0,0.08)',
                          boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
                          cursor: 'default'
                        }}
                      >
                        <Typography variant="caption" color={failedCount > 0 ? "error.main" : "textSecondary"} display="block" sx={{ textTransform: 'uppercase', fontWeight: 'bold', letterSpacing: '0.5px', fontSize: '11px' }}>
                          Failed
                        </Typography>
                        <Typography variant="h5" color={failedCount > 0 ? "error.main" : "textPrimary"} sx={{ fontWeight: 'bold', mt: 0.5 }}>
                          {failedCount}
                        </Typography>
                      </Box>
                    </LightTooltip>
                  </Grid>

                  {/* 4. Display-Only Skipped Card */}
                  <Grid item xs={6} sm={3}>
                    <LightTooltip
                      title={
                        skippedCount > 0 || executionFailed
                          ? "Some entities were skipped during purge. Please check <ATLAS_HOME>/logs/purgefailure.log for details."
                          : "No skipped entities during this purge operation."
                      }
                      arrow
                      placement="top"
                    >
                      <Box
                        sx={{
                          p: 1.5,
                          borderRadius: 2,
                          backgroundColor: skippedCount > 0 ? '#fffbeb' : '#fafafa',
                          border: skippedCount > 0 ? '1px solid #fef08a' : '1px solid rgba(0,0,0,0.08)',
                          boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
                          cursor: 'default'
                        }}
                      >
                        <Typography variant="caption" color={skippedCount > 0 ? "warning.main" : "textSecondary"} display="block" sx={{ textTransform: 'uppercase', fontWeight: 'bold', letterSpacing: '0.5px', fontSize: '11px' }}>
                          Skipped
                        </Typography>
                        <Typography variant="h5" color={skippedCount > 0 ? "warning.main" : "textPrimary"} sx={{ fontWeight: 'bold', mt: 0.5 }}>
                          {skippedCount}
                        </Typography>
                      </Box>
                    </LightTooltip>
                  </Grid>
                </>
              )}
            </Grid>

            {/* Status Alert if execution failure or failedCount > 0 */}
            {executionFailed && (
              <Box sx={{ mt: 2 }}>
                <Alert severity="warning">
                  <AlertTitle sx={{ fontWeight: 'bold', fontSize: '13px' }}>Partial success</AlertTitle>
                  Some entities failed to purge. Check <strong>purgefailure.log</strong> for details.
                </Alert>
              </Box>
            )}
          </Box>

          {/* Right Side Drawer — server-side pagination for Purged, client-side for Requested */}
          {(() => {
            // For REQUESTED view: client-side filter + paginate
            // For PURGED view: server-side paginate, client-side search on current page
            const isPurgedView = activePurgeView === PurgeActiveView.PURGED;
            const isServerSidePagination = isPurgedView && isSummaryRow;

            // Determine the raw list for the current view
            const rawListForView: string[] = activePurgeView === PurgeActiveView.REQUESTED
              ? requestedEntitiesList
              : purgedApiGuids; // full list for non-summary, page list for summary

            // Client-side search filter (on current page for purged, on full list for requested)
            const filteredList = rawListForView.filter((guidStr: string) => {
              if (!drawerSearchText) return true;
              return guidStr.toLowerCase().includes(drawerSearchText.trim().toLowerCase());
            });

            // For client-side pagination (requested OR non-summary purged)
            const clientSideItems = !isServerSidePagination
              ? filteredList.slice(0, drawerPage * drawerPageSize)
              : [];

            const displayItems = isServerSidePagination ? filteredList : clientSideItems;

            const { visibleItems, paddingTop, paddingBottom, startIndex } = useVirtualization({
                items: displayItems,
                scrollTop,
                itemHeight: 37
            });

            // ---- Purged: server-side pages ----
            // Server side infinite scroll handles data loading, we just display what we have
            const displayTotal = isServerSidePagination ? purgedTotalCount : filteredList.length;

            const handleDrawerScroll = (e: React.UIEvent<HTMLUListElement>) => {
              setScrollTop(e.currentTarget.scrollTop);
              if (drawerScrollTimerRef.current) return;

              // If it's a wheel event and they are scrolling up, ignore it
              const { scrollTop, scrollHeight, clientHeight } = e.currentTarget;
              const isNearBottom = scrollHeight - scrollTop - clientHeight < 20;
              if (isNearBottom) {
                drawerScrollTimerRef.current = setTimeout(() => {
                  drawerScrollTimerRef.current = null;
                  if (isServerSidePagination && !loadingPurgedApi && purgedApiGuids.length < displayTotal) {
                    fetchPurged(true);
                  } else if (!isServerSidePagination && visibleItems.length < displayTotal) {
                    setDrawerPage(prev => prev + 1);
                  }
                }, 250);
              }
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
                PaperProps={{ sx: { width: '460px', p: 2.5, display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' } }}
              >
                {/* 1. Drawer Header */}
                <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 2 }}>
                  <Typography sx={{ fontWeight: 600, color: 'text.primary', fontSize: '20px' }}>
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

                {/* 2. Run Id Header inside Drawer */}
                {runId !== 'N/A' && (
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2, py: 1, px: 1.5, bgcolor: '#f8fafc', borderRadius: 1.5, border: '1px solid #e2e8f0' }}>
                    <Typography variant="caption" color="textSecondary" sx={{ fontSize: '12px' }}>
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
                        sx={{ p: 0.5, ml: 'auto' }}
                      >
                        <ContentCopyIcon sx={{ fontSize: '15px', color: copiedRunId ? 'success.main' : 'text.secondary' }} />
                      </IconButton>
                    </Tooltip>
                  </Box>
                )}

                {/* 3. Search Bar */}
                {(activePurgeView === 'requested' ? requestedEntitiesList.length > 0 : purgedApiGuids.length > 0 || loadingPurgedApi) && (
                  <Box sx={{ mb: 2 }}>
                    <TextField
                      fullWidth
                      size="small"
                      placeholder="Search GUIDs..."
                      value={drawerSearchText}
                      onChange={(e) => {
                        setDrawerSearchText(e.target.value);
                        setDrawerPage(1);
                        setScrollTop(0);
                        // For purged view: re-fetch page 1 with new search is not supported server-side;
                        // search is applied client-side on the current page's data
                      }}
                      InputProps={{
                        startAdornment: (
                          <InputAdornment position="start">
                            <SearchIcon sx={{ fontSize: 18, color: 'text.secondary' }} />
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
                      sx={{
                        '& .MuiOutlinedInput-root': {
                          borderRadius: 2,
                          bgcolor: '#fff'
                        }
                      }}
                    />
                  </Box>
                )}

                <Divider sx={{ mb: 2 }} />

                {/* 4. GUID List */}
                {isPurgedView && loadingPurgedApi && purgedApiGuids.length === 0 ? (
                  <Box sx={{ display: 'flex', justifyContent: 'center', py: 5 }}>
                    <CircularProgress size={30} />
                  </Box>
                ) : (
                  <List dense sx={{ overflowY: 'scroll', flexGrow: 1, minHeight: 0 }} onScroll={handleDrawerScroll} onWheel={handleDrawerScroll}>
                    {(() => {
                      if (displayItems.length === 0) {
                        return (
                          <Typography variant="body2" color="textSecondary" sx={{ py: 3, textAlign: 'center' }}>
                            No matching GUIDs found
                          </Typography>
                        );
                      }

                      return (
                        <>
                          {paddingTop > 0 && <div style={{ height: paddingTop }} />}
                          {visibleItems.map((guidStr: string, localIndex: number) => {
                            const index = startIndex + localIndex;
                            const globalIndex = index + 1;
                            return (
                              <ListItem key={guidStr + index} sx={{ borderBottom: '1px solid rgba(0,0,0,0.04)', py: 1, height: '37px', boxSizing: 'border-box' }}>
                                <Typography variant="body2" sx={{ mr: 1, minWidth: '24px', color: 'text.secondary' }}>{globalIndex}.</Typography>
                                <Link
                                  component="button"
                                  variant="body2"
                                  underline="hover"
                                  onClick={() => {
                                    setOpenPurgeModal(true);
                                    setCurrentPurgeResultObj(guidStr);
                                  }}
                                  title={guidStr}
                                  sx={{
                                    display: "inline-block",
                                    maxWidth: "100%",
                                    textOverflow: "ellipsis",
                                    overflow: "hidden",
                                    whiteSpace: "nowrap",
                                    textAlign: "left"
                                  }}
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

                    {displayItems.length > 0 && displayItems.length < displayTotal && (
                      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', py: 2, gap: 1 }}>
                        {isPurgedView && loadingPurgedApi && <CircularProgress size={16} />}
                        <Typography
                          variant="caption"
                          color="text.secondary"
                          sx={{ fontStyle: 'italic', cursor: 'default' }}
                        >
                          {isPurgedView && loadingPurgedApi ? 'Loading more...' : 'Scroll to load more data'}
                        </Typography>
                      </Box>
                    )}
                  </List>
                )}

                {/* 5. Relationship-card-style Footer: Showing X of Y | Limit [input] */}
                {displayTotal > 0 && (
                  <Box sx={{
                    mt: 'auto',
                    pt: 1.5,
                    borderTop: '1px solid #e2e8f0',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    flexShrink: 0
                  }}>
                    <Typography variant="caption" color="textSecondary">
                      Showing {displayItems.length} of {displayTotal}
                    </Typography>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
                      <Typography variant="caption" color="textSecondary">Limit</Typography>
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
                              if (isServerSidePagination) {
                                fetchPurged(false, clamped);
                              }
                            }
                          }
                        }}
                        sx={{
                          width: '56px',
                          fontSize: '12px',
                          border: '1px solid #cbd5e1',
                          borderRadius: '4px',
                          px: 0.75,
                          py: 0.25,
                          textAlign: 'center',
                          outline: 'none',
                          '&:focus': { borderColor: '#90caf9' }
                        }}
                      />
                    </Box>
                  </Box>
                )}
              </Drawer>
            );
          })()}
        </Box>
      ) : null}
    </>
  );
};

export default AuditResults;
// trigger hmr
