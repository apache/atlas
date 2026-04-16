/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
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
  FormControlLabel,
  FormGroup,
  Grid,
  IconButton,
  Stack,
  Typography
} from "@mui/material";
import { TableLayout } from "@components/Table/TableLayout";
import { EntityState } from "@models/relationshipSearchType";
import { SchemaTabCacheState } from "@models/schemaTabTypes";
import { useSelector } from "react-redux";
import { useAppDispatch } from "@hooks/reducerHook";
import { getEntity } from "@api/apiMethods/entityFormApiMethod";
import { mergeReferredEntities } from "@redux/slice/detailPageSlice";
import { isEmpty, pick } from "@utils/Utils";
import { LightTooltip } from "@components/muiComponents";
import { Link } from "react-router-dom";
import { entityStateReadOnly } from "@utils/Enum";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import DialogShowMoreLess from "@components/DialogShowMoreLess";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AntSwitch } from "@utils/Muiutils";
import { getRelationShipV2 } from "@api/apiMethods/searchApiMethod";
import { useParams } from "react-router-dom";
import { serverError } from "@utils/Utils";
import {
  buildRelationshipSearchParams,
  DEFAULT_RELATIONSHIP_PAGE_LIMIT,
  getMinPageLimitForTotal,
  normalizeRelationshipApproximateCount,
  SCHEMA_RELATIONSHIP_EXTRA_ATTRIBUTES,
  SCHEMA_RELATIONSHIP_INCLUDE_DELETED
} from "@utils/relationshipSearchQuery";
import {
  buildFullMergedResolvedRows,
  countHistoricalInFullMerged,
  getVisibleSchemaRowsFromFullMerged,
  isSchemaRowHistorical,
  sortRowsInRelation
} from "@utils/schemaHistoricalRows";
import { removeClassification } from "@api/apiMethods/classificationApiMethod";
import { ColumnDef, PaginationState, SortingState } from "@tanstack/react-table";

interface SchemaTabProps {
  entity: any;
  referredEntities: Record<string, any> | undefined;
  loading: boolean;
  schemaRelationNames: string[];
  schemaCache: SchemaTabCacheState | null;
  setSchemaCache: React.Dispatch<
    React.SetStateAction<SchemaTabCacheState | null>
  >;
}

const getRelationshipLimitForSnapshot = (
  relationName: string,
  snap: SchemaTabCacheState,
  totalCount?: number
): number => {
  const minL = getMinPageLimitForTotal(totalCount);
  const currentLimit = snap.pageLimitByRelation[relationName];
  if (typeof currentLimit !== "number") {
    return Math.max(minL, DEFAULT_RELATIONSHIP_PAGE_LIMIT);
  }
  if (
    typeof totalCount === "number" &&
    Number.isFinite(totalCount) &&
    currentLimit > totalCount
  ) {
    return totalCount;
  }
  return Math.max(minL, currentLimit);
};

const mergeReferredFromRelationshipResponse = (
  prev: Record<string, any>,
  resp: any
): Record<string, any> => {
  const next = { ...prev };
  if (resp?.referredEntities && typeof resp.referredEntities === "object") {
    Object.assign(next, resp.referredEntities);
  }
  return next;
};

const SchemaTab = ({
  entity,
  referredEntities: detailReferredEntities,
  loading: detailLoading,
  schemaRelationNames,
  schemaCache,
  setSchemaCache
}: SchemaTabProps) => {
  const dispatch = useAppDispatch();
  const { guid } = useParams<{ guid: string }>();
  const toastId = useRef<any>(null);
  const schemaCacheRef = useRef<SchemaTabCacheState | null>(null);
  const mergedRowCountRef = useRef<number>(0);
  const hasMoreRef = useRef<boolean>(false);
  const loadLockRef = useRef<boolean>(false);
  const initialFetchRef = useRef<boolean>(false);
  /** Current client pagination (synced with TableLayout) for load-more chunking. */
  const paginationRef = useRef<PaginationState>({
    pageIndex: 0,
    pageSize: DEFAULT_RELATIONSHIP_PAGE_LIMIT
  });

  const { entityData } = useSelector((state: EntityState) => state.entity);

  const [checked, setChecked] = useState<boolean>(
    !isEmpty(entity) && entityStateReadOnly[entity.status]
  );
  /** Keeps pagination `fetchData` stable when toggling historical — no relationship API replay. */
  const checkedRef = useRef<boolean>(checked);
  useEffect(() => {
    checkedRef.current = checked;
  }, [checked]);

  const [loadingMore, setLoadingMore] = useState<boolean>(false);
  const [initialLoading, setInitialLoading] = useState<boolean>(false);

  useEffect(() => {
    schemaCacheRef.current = schemaCache;
  }, [schemaCache]);

  useEffect(() => {
    paginationRef.current = {
      pageIndex: 0,
      pageSize: DEFAULT_RELATIONSHIP_PAGE_LIMIT
    };
  }, [guid]);

  useEffect(() => {
    if (!schemaCache) {
      setSchemaCache({
        rowsByRelation: {},
        totals: {},
        referredEntities: {},
        initialLoadDone: false,
        hasError: false,
        pageLimitByRelation: {}
      });
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps -- only when parent clears cache
  }, [schemaCache, setSchemaCache]);

  const fetchRelationPage = useCallback(
    async (
      relationName: string,
      offset: number,
      cacheSnapshot: SchemaTabCacheState,
      options?: { requestLimit?: number }
    ) => {
      if (!guid) {
        return { entities: [], approximateCount: undefined, referredEntities: {} };
      }
      const totalCount = cacheSnapshot.totals[relationName];
      const defaultLimit = getRelationshipLimitForSnapshot(
        relationName,
        cacheSnapshot,
        totalCount
      );
      const limit =
        typeof options?.requestLimit === "number" &&
        Number.isFinite(options.requestLimit) &&
        options.requestLimit > 0
          ? Math.max(1, Math.floor(options.requestLimit))
          : defaultLimit;
      const params = buildRelationshipSearchParams({
        guid,
        relation: relationName,
        limit,
        offset,
        isSorted: false,
        showDeleted: SCHEMA_RELATIONSHIP_INCLUDE_DELETED,
        getApproximateCount: offset === 0,
        extraAttributes: SCHEMA_RELATIONSHIP_EXTRA_ATTRIBUTES
      });
      const response = await getRelationShipV2({ params });
      const data = response?.data;
      const rawCount = data?.approximateCount ?? data?.totalCount;
      /** Chunk requests use getApproximateCount=false; API may return -1 — never use that for totals. */
      const approximateCount =
        offset === 0
          ? normalizeRelationshipApproximateCount(rawCount)
          : undefined;
      return {
        entities: data?.entities || [],
        approximateCount,
        referredEntities: data?.referredEntities
      };
    },
    [guid]
  );

  const runInitialFetch = useCallback(async () => {
    if (
      !guid ||
      !schemaCache ||
      schemaCache.initialLoadDone ||
      isEmpty(schemaRelationNames) ||
      initialFetchRef.current
    ) {
      return;
    }
    initialFetchRef.current = true;
    setInitialLoading(true);
    let mergedRef = {
      ...(schemaCache.referredEntities || {}),
      ...(detailReferredEntities || {})
    };
    const nextRows: Record<string, any[]> = { ...schemaCache.rowsByRelation };
    const nextTotals: Record<string, number> = { ...schemaCache.totals };

    try {
      for (const relationName of schemaRelationNames) {
        const res = await fetchRelationPage(relationName, 0, {
          ...schemaCache,
          referredEntities: mergedRef
        });
        const list = Array.isArray(res.entities) ? res.entities : [];
        mergedRef = mergeReferredFromRelationshipResponse(mergedRef, {
          referredEntities: res.referredEntities
        });
        nextRows[relationName] = sortRowsInRelation(list);
        if (res.approximateCount !== undefined) {
          nextTotals[relationName] = res.approximateCount;
        }
      }
      const initialPageLimits = schemaRelationNames.reduce(
        (acc: Record<string, number>, r: string) => {
          acc[r] = DEFAULT_RELATIONSHIP_PAGE_LIMIT;
          return acc;
        },
        {}
      );
      setSchemaCache({
        rowsByRelation: nextRows,
        totals: nextTotals,
        referredEntities: mergedRef,
        initialLoadDone: true,
        hasError: false,
        pageLimitByRelation: {
          ...initialPageLimits,
          ...schemaCache.pageLimitByRelation
        }
      });
    } catch (e: any) {
      console.error("Schema tab fetch failed", e);
      serverError(e, toastId);
      setSchemaCache((prev) =>
        prev
          ? {
              ...prev,
              initialLoadDone: true,
              hasError: true
            }
          : prev
      );
    } finally {
      setInitialLoading(false);
      initialFetchRef.current = false;
    }
  }, [
    guid,
    schemaCache,
    schemaRelationNames,
    fetchRelationPage,
    detailReferredEntities,
    setSchemaCache
  ]);

  useEffect(() => {
    if (
      schemaCache &&
      !schemaCache.initialLoadDone &&
      !isEmpty(schemaRelationNames)
    ) {
      void runInitialFetch();
    }
  }, [schemaCache, schemaRelationNames, runInitialFetch]);

  /** Redux detail `referredEntities` (e.g. after GET by child guid) must override schema cache. */
  const mergedReferred = useMemo(() => {
    return {
      ...(schemaCache?.referredEntities || {}),
      ...(detailReferredEntities || {})
    };
  }, [detailReferredEntities, schemaCache?.referredEntities]);

  const resolveEntity = useCallback(
    (row: any) => {
      const g = row?.guid;
      if (g && mergedReferred[g]) {
        const r = mergedReferred[g];
        /* Fresh GET may omit classifications when empty; ?? row would keep stale chips. */
        const fullish =
          r &&
          typeof r === "object" &&
          (r.typeName != null || r.status != null);
        let classifications = row.classifications;
        if (Object.prototype.hasOwnProperty.call(r, "classifications")) {
          classifications = r.classifications ?? [];
        } else if (fullish && r.guid === g) {
          classifications = [];
        }
        let classificationNames = row.classificationNames;
        if (Object.prototype.hasOwnProperty.call(r, "classificationNames")) {
          classificationNames = r.classificationNames;
        }
        return {
          ...row,
          ...r,
          attributes: r.attributes || row.attributes,
          classifications,
          classificationNames
        };
      }
      return row;
    },
    [mergedReferred]
  );

  const firstRowForDef = useMemo(() => {
    if (!schemaCache?.initialLoadDone) {
      return null;
    }
    for (const name of schemaRelationNames) {
      const rows = schemaCache.rowsByRelation[name];
      if (rows && rows.length > 0) {
        return resolveEntity(rows[0]);
      }
    }
    const rel0 = schemaRelationNames[0];
    const embedded =
      entity?.relationshipAttributes?.[rel0]?.[0] ||
      entity?.attributes?.[rel0]?.[0];
    return embedded ? resolveEntity(embedded) : null;
  }, [schemaCache, schemaRelationNames, entity, resolveEntity]);

  const defObj = useMemo(() => {
    if (!firstRowForDef?.typeName || !entityData?.entityDefs) {
      return null;
    }
    return entityData.entityDefs.find(
      (obj: { name: string }) => obj.name === firstRowForDef.typeName
    );
  }, [entityData, firstRowForDef]);

  const schemaTableAttribute = useMemo(() => {
    if (!defObj?.options?.schemaAttributes || !firstRowForDef) {
      return null;
    }
    try {
      const mapObj = JSON.parse(defObj.options.schemaAttributes);
      return pick(firstRowForDef.attributes || {}, mapObj);
    } catch {
      return null;
    }
  }, [defObj, firstRowForDef]);

  const fullMergedRows = useMemo(() => {
    if (!schemaCache?.initialLoadDone) {
      return [];
    }
    return buildFullMergedResolvedRows(
      schemaCache.rowsByRelation,
      schemaRelationNames,
      resolveEntity
    );
  }, [schemaCache, schemaRelationNames, resolveEntity]);

  const tableData = useMemo(() => {
    return getVisibleSchemaRowsFromFullMerged(fullMergedRows, checked);
  }, [fullMergedRows, checked]);

  const historicalRowCount = useMemo(
    () => countHistoricalInFullMerged(fullMergedRows),
    [fullMergedRows]
  );

  /**
   * Sum of per-relation approximate totals from relationship search (offset=0).
   * Drives client pagination footer + Next until chunks load; must not use only
   * loaded row count (e.g. 100) when approximateCount is ~230k.
   */
  const mergedApproximateTotal = useMemo(() => {
    if (!schemaCache?.initialLoadDone) {
      return 0;
    }
    return schemaRelationNames.reduce((sum, name) => {
      const loaded = schemaCache.rowsByRelation[name]?.length ?? 0;
      const approx = schemaCache.totals[name];
      const t =
        typeof approx === "number" && Number.isFinite(approx) && approx >= 0
          ? Math.max(approx, loaded)
          : loaded;
      return sum + t;
    }, 0);
  }, [schemaCache, schemaRelationNames]);

  /** When "historical" is on but cache has none, footer should match empty table (0), not API total. */
  const schemaPaginationTotalCount = useMemo(() => {
    if (!schemaCache?.initialLoadDone) {
      return 0;
    }
    if (
      checked &&
      historicalRowCount === 0 &&
      fullMergedRows.length > 0
    ) {
      return 0;
    }
    return mergedApproximateTotal;
  }, [
    schemaCache?.initialLoadDone,
    checked,
    historicalRowCount,
    fullMergedRows.length,
    mergedApproximateTotal
  ]);

  const schemaEmptyText = useMemo(() => {
    if (schemaCache?.hasError) {
      return "Failed to load schema";
    }
    return "No Records found!";
  }, [schemaCache?.hasError]);

  useEffect(() => {
    mergedRowCountRef.current = tableData.length;
  }, [tableData]);

  /** Match Classic: if only historical columns exist, force the switch on. */
  useEffect(() => {
    if (!schemaCache?.initialLoadDone) {
      return;
    }
    const activeCount = fullMergedRows.filter((r) => !isSchemaRowHistorical(r))
      .length;
    if (!checked && activeCount === 0 && historicalRowCount > 0) {
      setChecked(true);
    }
  }, [
    schemaCache?.initialLoadDone,
    fullMergedRows,
    checked,
    historicalRowCount
  ]);

  const countVisibleSchemaRows = useCallback(
    (cache: SchemaTabCacheState | null) => {
      if (!cache?.initialLoadDone) {
        return 0;
      }
      const full = buildFullMergedResolvedRows(
        cache.rowsByRelation,
        schemaRelationNames,
        resolveEntity
      );
      return getVisibleSchemaRowsFromFullMerged(full, checkedRef.current).length;
    },
    [schemaRelationNames, resolveEntity]
  );

  const handleSwitchChange = useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      event.stopPropagation();
      setChecked(event.target.checked);
    },
    []
  );

  const hasMoreAnywhere = useCallback(() => {
    if (!schemaCache?.initialLoadDone) {
      return false;
    }
    for (const name of schemaRelationNames) {
      const rawTotal = schemaCache.totals[name];
      const total =
        typeof rawTotal === "number" &&
        Number.isFinite(rawTotal) &&
        rawTotal >= 0
          ? rawTotal
          : 0;
      const rows = schemaCache.rowsByRelation[name] || [];
      if (rows.length < total) {
        return true;
      }
    }
    return false;
  }, [schemaCache, schemaRelationNames]);

  useEffect(() => {
    hasMoreRef.current = hasMoreAnywhere();
  }, [hasMoreAnywhere]);

  const loadNextChunk = useCallback(async (): Promise<number> => {
    const snap = schemaCacheRef.current;
    if (!snap?.initialLoadDone || !guid || loadLockRef.current) {
      return countVisibleSchemaRows(snap);
    }
    const target = schemaRelationNames.find((name) => {
      const rawTotal = snap.totals[name];
      const total =
        typeof rawTotal === "number" &&
        Number.isFinite(rawTotal) &&
        rawTotal >= 0
          ? rawTotal
          : 0;
      const rows = snap.rowsByRelation[name] || [];
      return rows.length < total;
    });
    if (!target) {
      return countVisibleSchemaRows(snap);
    }
    loadLockRef.current = true;
    setLoadingMore(true);
    try {
      const existing = snap.rowsByRelation[target] || [];
      const res = await fetchRelationPage(target, existing.length, snap);
      const list = Array.isArray(res.entities) ? res.entities : [];
      const mergedRef = mergeReferredFromRelationshipResponse(snap.referredEntities, {
        referredEntities: res.referredEntities
      });
      const nextRows = sortRowsInRelation([...existing, ...list]);
      const nextCache: SchemaTabCacheState = {
        ...snap,
        rowsByRelation: {
          ...snap.rowsByRelation,
          [target]: nextRows
        },
        referredEntities: mergedRef,
        totals:
          res.approximateCount !== undefined
            ? {
                ...snap.totals,
                [target]: res.approximateCount
              }
            : snap.totals
      };
      schemaCacheRef.current = nextCache;
      setSchemaCache(nextCache);
      return countVisibleSchemaRows(nextCache);
    } catch (e: any) {
      console.error("Schema load more failed", e);
      serverError(e, toastId);
      return countVisibleSchemaRows(schemaCacheRef.current);
    } finally {
      setLoadingMore(false);
      loadLockRef.current = false;
    }
  }, [
    guid,
    schemaRelationNames,
    fetchRelationPage,
    setSchemaCache,
    countVisibleSchemaRows
  ]);

  const handleClientPageSizeChange = useCallback(
    (size: number) => {
      if (!Number.isFinite(size) || size < 1) {
        return;
      }
      paginationRef.current = {
        pageIndex: 0,
        pageSize: size
      };
      setSchemaCache((prev) => {
        if (!prev) {
          return prev;
        }
        const nextLimits: Record<string, number> = {};
        schemaRelationNames.forEach((n) => {
          nextLimits[n] = size;
        });
        return {
          ...prev,
          pageLimitByRelation: {
            ...prev.pageLimitByRelation,
            ...nextLimits
          }
        };
      });
    },
    [schemaRelationNames, setSchemaCache]
  );

  /**
   * After classification add/remove, GET each affected column (child) entity with
   * ignoreRelationships=true and merge into Redux + schema tab cache (no relationship replay).
   */
  const refreshSchemaChildEntities = useCallback(
    async (childGuids: string[]) => {
      const unique = [
        ...new Set(childGuids.filter((g) => typeof g === "string" && g.length > 0))
      ];
      if (unique.length === 0) {
        return;
      }
      const mergedFromGet: Record<string, any> = {};
      for (const g of unique) {
        try {
          const res = await getEntity(g, "GET", {});
          const ent = (res as any)?.data?.entity;
          if (ent) {
            mergedFromGet[g] = ent;
          }
        } catch (err) {
          console.error("Schema child entity refresh failed", g, err);
        }
      }
      if (Object.keys(mergedFromGet).length === 0) {
        return;
      }
      dispatch(mergeReferredEntities(mergedFromGet));
      setSchemaCache((prev) =>
        prev
          ? {
              ...prev,
              referredEntities: {
                ...prev.referredEntities,
                ...mergedFromGet
              }
            }
          : prev
      );
    },
    [dispatch, setSchemaCache]
  );

  const handleFetchForPagination = useCallback(
    async ({
      pagination
    }: {
      pagination: PaginationState;
      sorting: SortingState;
    }) => {
      paginationRef.current = pagination;
      const needRows = (pagination.pageIndex + 1) * pagination.pageSize;
      let len = countVisibleSchemaRows(schemaCacheRef.current);
      let guard = 0;
      while (guard++ < 120) {
        if (len >= needRows) {
          mergedRowCountRef.current = len;
          return;
        }
        if (!hasMoreRef.current) {
          return;
        }
        const before = len;
        const after = await loadNextChunk();
        mergedRowCountRef.current = after;
        if (after <= before) {
          return;
        }
        len = after;
      }
    },
    [loadNextChunk, countVisibleSchemaRows]
  );

  const defaultColumns: ColumnDef<any>[] = useMemo(() => {
    const cols: ColumnDef<any>[] = [];
    if (!isEmpty(schemaTableAttribute)) {
      Object.keys(schemaTableAttribute).forEach((key) => {
        if (key === undefined || key === "position" || key === "description") {
          return;
        }
        if (key === "name") {
          cols.push({
            accessorFn: (row: any) => row.attributes?.name,
            accessorKey: "name",
            cell: (info: any) => {
              const entityDef: any = info.row.original;
              const href = `/detailPage/${entityDef.guid}`;
              return (
                <div className="searchTableName">
                  <LightTooltip title={entityDef.attributes?.name}>
                    {entityDef.guid != "-1" ? (
                      <Link
                        className={`entity-name text-decoration-none ${
                          entityDef.status &&
                          entityStateReadOnly[entityDef.status]
                            ? "text-red"
                            : "text-blue"
                        }`}
                        style={{
                          width: "unset !important",
                          whiteSpace: "nowrap"
                        }}
                        to={{ pathname: href }}
                      >
                        {entityDef.attributes?.name}
                      </Link>
                    ) : (
                      <span>{entityDef.attributes?.name} </span>
                    )}
                  </LightTooltip>
                  {entityDef.status &&
                    entityStateReadOnly[entityDef.status] && (
                      <LightTooltip title="Deleted">
                        <IconButton
                          aria-label="deleted-entity"
                          sx={{
                            display: "inline-flex",
                            position: "relative",
                            padding: "4px",
                            marginLeft: "4px",
                            color: (theme) => theme.palette.grey[500]
                          }}
                        >
                          <DeleteOutlineOutlinedIcon
                            sx={{ fontSize: "1.25rem" }}
                          />
                        </IconButton>
                      </LightTooltip>
                    )}
                </div>
              );
            },
            header: "Name",
            enableSorting: true,
            id: "name"
          });
          return;
        }
        cols.push({
          accessorFn: (row: any) => row.attributes?.[key],
          accessorKey: key,
          cell: (info: any) => {
            const values = info.row.original.attributes;
            return (
              <Typography>
                {!isEmpty(values?.[key]) ? values[key] : "N/A"}
              </Typography>
            );
          },
          header: key,
          enableSorting: true,
          id: key
        });
      });
    }
    cols.push({
      accessorFn: (row: any) => row.classificationNames?.[0],
      accessorKey: "classificationNames",
      cell: (info: any) => {
        const data = info.row.original;
        if (data.guid === "-1") {
          return;
        }
        return (
          <DialogShowMoreLess
            value={data}
            readOnly={
              data.status && entityStateReadOnly[data.status] ? true : false
            }
            columnVal="classifications"
            colName="Classification"
            displayText="typeName"
            removeApiMethod={removeClassification}
            isShowMoreLess={true}
            onSchemaChildEntityRefresh={refreshSchemaChildEntities}
          />
        );
      },
      header: "Classifications",
      enableSorting: false,
      id: "classifications"
    });
    return cols;
  }, [schemaTableAttribute, refreshSchemaChildEntities]);

  /** Avoid indefinite skeleton when schemaRelationNames is not ready yet (only !schemaCache mattered). */
  const needsSchemaFetch = !isEmpty(schemaRelationNames);
  const isFetching =
    detailLoading ||
    initialLoading ||
    loadingMore ||
    (needsSchemaFetch && !schemaCache);

  return (
    <>
      <Grid container marginTop={0} className="properties-container">
        <Grid item md={12} p={2}>
          <Stack position="relative">
            <Stack
              direction="row"
              justifyContent="flex-end"
              alignItems="center"
              marginBottom="0.75rem"
              flexWrap="wrap"
              gap={1}
              data-id="checkDeletedEntity"
            >
              <FormGroup>
                <FormControlLabel
                  control={
                    <AntSwitch
                      size="small"
                      checked={checked}
                      onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                        handleSwitchChange(e);
                      }}
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                      sx={{ marginRight: "4px" }}
                      inputProps={{ "aria-label": "show-historical-entities" }}
                    />
                  }
                  label="Show historical entities"
                />
              </FormGroup>
            </Stack>

            <TableLayout
              key={`${guid ?? ""}-schema`}
              data={tableData}
              columns={defaultColumns.filter(Boolean)}
              emptyText={schemaEmptyText}
              isFetching={isFetching}
              showRowSelection={true}
              columnVisibility={false}
              clientSideSorting={true}
              columnSort={true}
              showPagination={true}
              isClientSidePagination={true}
              totalCount={
                schemaCache?.initialLoadDone
                  ? schemaPaginationTotalCount
                  : undefined
              }
              fetchData={handleFetchForPagination}
              tableFilters={false}
              assignFilters={{ classifications: true, term: false }}
              defaultPageSize={DEFAULT_RELATIONSHIP_PAGE_LIMIT}
              onClientPageSizeChange={handleClientPageSizeChange}
            />
          </Stack>
        </Grid>
      </Grid>
    </>
  );
};

export default SchemaTab;
