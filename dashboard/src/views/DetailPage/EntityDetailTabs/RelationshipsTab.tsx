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
  FormControlLabel,
  FormGroup,
  Grid,
  Stack,
  ToggleButton,
  ToggleButtonGroup
} from "@mui/material";
import { useState, useEffect, useRef, useMemo } from "react";
import { isArray, isEmpty } from "@utils/Utils";
import { EntityDetailTabProps } from "@models/entityDetailType";
import RelationshipLineage from "./RelationshipLineage";
import { AntSwitch } from "@utils/Muiutils";
import RelationshipCard from "./RelationshipCard";
import RelationshipCardSkeleton from "./RelationshipCardSkeleton";
import { getRelationShipV2 } from "@api/apiMethods/searchApiMethod";
import { useParams } from "react-router-dom";
import { serverError } from "@utils/Utils";
import { useSelector } from "react-redux";
import { EntityState } from "@models/relationshipSearchType";

const RelationshipsTab: React.FC<EntityDetailTabProps> = ({
  entity,
  referredEntities
}) => {
  const { guid } = useParams<{ guid: string }>();
  const toastId = useRef<any>(null);
  const fetchStartedRef = useRef<boolean>(false);
  const prevMeaningsSigRef = useRef<string | null>(null);
  const meaningsCardControlsRef = useRef<{
    isSorted: boolean;
    showDeleted: boolean;
  }>({ isSorted: false, showDeleted: true });
  const prevCardTotalCountsRef = useRef<Record<string, number>>({});
  const { entityData } = useSelector((state: EntityState) => state.entity);
  const entityTypeName = entity?.typeName;

  const [alignment, setAlignment] = useState<string>("table");
  const [checked, setChecked] = useState<boolean>(false);
  
  // Card view state
  const [cardData, setCardData] = useState<Record<string, any[]>>({});
  const [cardTotalCounts, setCardTotalCounts] = useState<Record<string, number>>({});
  const [cardLoading, setCardLoading] = useState<boolean>(false);
  const [cardLoadingByName, setCardLoadingByName] = useState<
    Record<string, boolean>
  >({});
  const [initialLoadDone, setInitialLoadDone] = useState<boolean>(false);
  const [sortByNameByAttr, setSortByNameByAttr] = useState<
    Record<string, boolean>
  >({});
  const [showDeletedByAttr, setShowDeletedByAttr] = useState<
    Record<string, boolean>
  >({});
  const [pageLimitByAttr, setPageLimitByAttr] = useState<
    Record<string, number>
  >({});
  const [cardResettingByName, setCardResettingByName] = useState<
    Record<string, boolean>
  >({});
  const [showInitialSkeletons, setShowInitialSkeletons] = useState<boolean>(true);
  const [hasRelationshipApiError, setHasRelationshipApiError] =
    useState<boolean>(false);
  const initialSkeletonTimerRef = useRef<ReturnType<typeof setTimeout> | null>(
    null
  );

  useEffect(() => {
    setInitialLoadDone(false);
    setCardData({});
    setCardTotalCounts({});
    setShowInitialSkeletons(true);
    setHasRelationshipApiError(false);
    fetchStartedRef.current = false;
    prevMeaningsSigRef.current = null;

    if (initialSkeletonTimerRef.current) {
      clearTimeout(initialSkeletonTimerRef.current);
      initialSkeletonTimerRef.current = null;
    }
    initialSkeletonTimerRef.current = setTimeout(() => {
      setShowInitialSkeletons(false);
      initialSkeletonTimerRef.current = null;
    }, 5000);

    return () => {
      if (initialSkeletonTimerRef.current) {
        clearTimeout(initialSkeletonTimerRef.current);
      }
    };
  }, [guid]);

  const relationNames = useMemo((): string[] => {
    if (!entityTypeName || !entityData?.entityDefs) {
      return [];
    }
    const entityDef = entityData.entityDefs.find(
      (def: { name: string }) => def.name === entityTypeName
    );
    const relationshipDefs = (entityDef?.relationshipAttributeDefs || []) as Array<{ name: string }>;
    const names = relationshipDefs
      .map((def) => def.name)
      .filter(Boolean) as string[];
    return [...new Set(names)];
  }, [entityData, entityTypeName]);

  /** Stable fingerprint of assigned glossary terms for meanings card refresh. */
  const meaningsSignature = useMemo((): string => {
    const raw = entity?.relationshipAttributes?.meanings;
    if (!isArray(raw) || isEmpty(raw)) {
      return '__none__';
    }
    const ids = raw
      .map((m: any) => {
        if (m && typeof m === 'object') {
          return String(
            m.guid || m.termGuid || m.qualifiedName || ''
          ).trim();
        }
        return '';
      })
      .filter(Boolean)
      .sort();
    return ids.join('|');
  }, [entity?.relationshipAttributes?.meanings]);

  const handleChange = (
    _event: React.MouseEvent<HTMLElement>,
    newAlignment: string
  ) => {
    if (newAlignment !== null) {
      setAlignment(newAlignment);
    }
  };

  const handleSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    event.stopPropagation();
    setChecked(event.target.checked);
  };

  // Fetch initial relationship attributes data
  /** Page limit may be 1 only when the card total is 0 or 1; otherwise minimum is 2. */
  const getMinPageLimitForTotal = (totalCount?: number): number => {
    if (typeof totalCount !== "number" || !Number.isFinite(totalCount)) {
      return 1;
    }
    if (totalCount <= 0) {
      return 1;
    }
    return totalCount <= 1 ? 1 : 2;
  };

  const DEFAULT_RELATIONSHIP_PAGE_LIMIT = 100;

  const getPageLimit = (relationName: string, totalCount?: number) => {
    const minL = getMinPageLimitForTotal(totalCount);
    const currentLimit = pageLimitByAttr[relationName];
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

  const getRelationshipParams = (
    relationName: string,
    offset: number,
    totalCount?: number,
    overrides?: {
      isSorted?: boolean;
      showDeleted?: boolean;
      limit?: number;
      /** Use API default page size instead of stored limit (e.g. after term count changes). */
      ignoreStoredLimit?: boolean;
    }
  ) => {
    const isSorted =
      typeof overrides?.isSorted === "boolean"
        ? overrides.isSorted
        : !!sortByNameByAttr[relationName];
    const showDeleted =
      typeof overrides?.showDeleted === "boolean"
        ? overrides.showDeleted
        : !!showDeletedByAttr[relationName];
    const limit =
      typeof overrides?.limit === "number"
        ? overrides.limit
        : overrides?.ignoreStoredLimit
          ? Math.max(
              getMinPageLimitForTotal(totalCount),
              DEFAULT_RELATIONSHIP_PAGE_LIMIT
            )
          : getPageLimit(relationName, totalCount);
    return {
      limit,
      offset,
      guid,
      sortBy: isSorted ? "name" : undefined,
      sortOrder: isSorted ? "ASCENDING" : undefined,
      disableDefaultSorting: !isSorted,
      excludeDeletedEntities: !showDeleted,
      includeSubClassifications: true,
      includeSubTypes: true,
      includeClassificationAttributes: true,
      relation: relationName,
      getApproximateCount: offset === 0
    };
  };

  const fetchRelationshipData = async (
    relationName: string,
    offset: number,
    totalCount?: number,
    overrides?: {
      isSorted?: boolean;
      showDeleted?: boolean;
      limit?: number;
      ignoreStoredLimit?: boolean;
    }
  ) => {
    const params = getRelationshipParams(
      relationName,
      offset,
      totalCount,
      overrides
    );
    const response = await getRelationShipV2({ params });
    return {
      entities: response?.data?.entities || [],
      approximateCount:
        response?.data?.approximateCount ?? response?.data?.totalCount
    };
  };

  const fetchInitialRelationshipData = async () => {
    if (!guid || initialLoadDone || isEmpty(relationNames)) {
      return;
    }

    setCardLoading(true);
    let completedCount = 0;
    const totalCount = relationNames.length;

    relationNames.forEach((relationName) => {
      fetchRelationshipData(relationName, 0, undefined, {
        showDeleted: true
      })
        .then((response) => {
          const entities = response.entities;
          const entityList = isArray(entities)
            ? entities
            : !isEmpty(entities)
            ? [entities]
            : [];
          setCardData((prev) => ({ ...prev, [relationName]: entityList }));
          if (response.approximateCount !== undefined) {
            setCardTotalCounts((prev) => ({
              ...prev,
              [relationName]: response.approximateCount
            }));
          }
          setSortByNameByAttr((prev) => ({
            ...prev,
            [relationName]: prev[relationName] ?? false
          }));
          setShowDeletedByAttr((prev) => ({
            ...prev,
            [relationName]: prev[relationName] ?? true
          }));
        })
        .catch((err) => {
          console.error(`Error fetching relationship ${relationName}:`, err);
          setHasRelationshipApiError(true);
          setCardData((prev) => ({ ...prev, [relationName]: [] }));
          setSortByNameByAttr((prev) => ({
            ...prev,
            [relationName]: prev[relationName] ?? false
          }));
          setShowDeletedByAttr((prev) => ({
            ...prev,
            [relationName]: prev[relationName] ?? true
          }));
        })
        .finally(() => {
          completedCount += 1;
          if (completedCount >= totalCount) {
            setInitialLoadDone(true);
            setCardLoading(false);
            setShowInitialSkeletons(false);
            if (initialSkeletonTimerRef.current) {
              clearTimeout(initialSkeletonTimerRef.current);
              initialSkeletonTimerRef.current = null;
            }
          }
        });
    });
  };

  // Load initial data when tab is switched to card/graph view
  useEffect(() => {
    if (
      (alignment === "table" || alignment === "graph") &&
      guid &&
      !initialLoadDone &&
      !cardLoading &&
      !isEmpty(relationNames)
    ) {
      if (fetchStartedRef.current) {
        return;
      }
      fetchStartedRef.current = true;
      fetchInitialRelationshipData().finally(() => {
        fetchStartedRef.current = false;
      });
    }
  }, [alignment, guid, relationNames, initialLoadDone, cardLoading]);

  useEffect(() => {
    if (!isEmpty(relationNames)) {
      setShowInitialSkeletons(false);
      if (initialSkeletonTimerRef.current) {
        clearTimeout(initialSkeletonTimerRef.current);
        initialSkeletonTimerRef.current = null;
      }
    }
  }, [relationNames]);

  // Handle load more for a specific card
  const handleCardLoadMore = async (attributeName: string) => {
    if (!guid || cardLoadingByName[attributeName]) {
      return;
    }
    const existingData = cardData[attributeName] || [];
    setCardLoadingByName((prev) => ({ ...prev, [attributeName]: true }));
    try {
      const totalCount = cardTotalCounts[attributeName];
      const response = await fetchRelationshipData(
        attributeName,
        existingData.length,
        totalCount
      );
      const updated = [...existingData, ...(response.entities || [])];
      setCardData((prev) => ({
        ...prev,
        [attributeName]: updated
      }));
    } catch (error: any) {
      console.error("Error loading more relationship data:", error);
      serverError(error, toastId);
    } finally {
      setCardLoadingByName((prev) => ({ ...prev, [attributeName]: false }));
    }
  };

  useEffect(() => {
    if (isEmpty(cardTotalCounts)) {
      return;
    }
    const snapshot = { ...prevCardTotalCountsRef.current };
    setPageLimitByAttr((prev) => {
      const next = { ...prev };
      Object.keys(cardTotalCounts).forEach((relationName) => {
        const totalCount = cardTotalCounts[relationName];
        const minL = getMinPageLimitForTotal(totalCount);
        const current = next[relationName];
        const prevTotal = snapshot[relationName];

        if (typeof current !== "number") {
          return;
        }
        if (totalCount > 0 && current > totalCount) {
          next[relationName] = totalCount;
          return;
        }
        if (current < minL) {
          next[relationName] = minL;
          return;
        }
        if (
          typeof prevTotal === "number" &&
          totalCount > prevTotal &&
          current === prevTotal &&
          prevTotal > 0
        ) {
          next[relationName] = Math.max(minL, DEFAULT_RELATIONSHIP_PAGE_LIMIT);
        }
      });
      return next;
    });
    Object.keys(cardTotalCounts).forEach((key) => {
      const v = cardTotalCounts[key];
      if (typeof v === "number") {
        prevCardTotalCountsRef.current[key] = v;
      }
    });
  }, [cardTotalCounts]);

  const resetRelationshipData = async (
    relationName: string,
    overrides?: {
      isSorted?: boolean;
      showDeleted?: boolean;
      limit?: number;
      ignoreStoredLimit?: boolean;
    },
    options?: {
      /** Use when term assignments changed so stale totalCount (e.g. 0) does not cap the page. */
      refreshTotalFromApi?: boolean;
    }
  ) => {
    if (!guid || cardLoadingByName[relationName]) {
      return;
    }
    setCardResettingByName((prev) => ({ ...prev, [relationName]: true }));
    try {
      const totalCount = options?.refreshTotalFromApi
        ? undefined
        : cardTotalCounts[relationName];
      const response = await fetchRelationshipData(
        relationName,
        0,
        totalCount,
        {
          ...(overrides || {}),
          ignoreStoredLimit:
            !!options?.refreshTotalFromApi || !!overrides?.ignoreStoredLimit,
        }
      );
      const entities = response.entities;
      const updated = isArray(entities)
        ? entities
        : !isEmpty(entities)
        ? [entities]
        : [];
      setCardData((prev) => ({
        ...prev,
        [relationName]: updated
      }));
      if (response.approximateCount !== undefined) {
        setCardTotalCounts((prev) => ({
          ...prev,
          [relationName]: response.approximateCount
        }));
      }
    } catch (error: any) {
      console.error("Error resetting relationship data:", error);
      serverError(error, toastId);
    } finally {
      setCardResettingByName((prev) => ({ ...prev, [relationName]: false }));
    }
  };

  const resetRelationshipDataRef = useRef(resetRelationshipData);
  resetRelationshipDataRef.current = resetRelationshipData;

  meaningsCardControlsRef.current = {
    isSorted: !!sortByNameByAttr['meanings'],
    showDeleted: !!showDeletedByAttr['meanings']
  };

  useEffect(() => {
    if (
      !guid ||
      !initialLoadDone ||
      !relationNames.includes('meanings')
    ) {
      return;
    }
    if (prevMeaningsSigRef.current === null) {
      prevMeaningsSigRef.current = meaningsSignature;
      return;
    }
    if (prevMeaningsSigRef.current === meaningsSignature) {
      return;
    }
    prevMeaningsSigRef.current = meaningsSignature;
    const c = meaningsCardControlsRef.current;
    void resetRelationshipDataRef.current(
      'meanings',
      {
        isSorted: c.isSorted,
        showDeleted: c.showDeleted
      },
      { refreshTotalFromApi: true }
    );
  }, [meaningsSignature, initialLoadDone, guid, relationNames]);

  const handleToggleSort = (relationName: string) => {
    const nextSorted = !sortByNameByAttr[relationName];
    setSortByNameByAttr((prev) => ({
      ...prev,
      [relationName]: nextSorted
    }));
    resetRelationshipData(relationName, { isSorted: nextSorted });
  };

  const handleToggleShowDeleted = (relationName: string) => {
    const nextShowDeleted = !showDeletedByAttr[relationName];
    setShowDeletedByAttr((prev) => ({
      ...prev,
      [relationName]: nextShowDeleted
    }));
    resetRelationshipData(relationName, { showDeleted: nextShowDeleted });
  };

  const handlePageLimitChange = (relationName: string, value: string) => {
    const parsed = parseInt(value, 10);
    if (!Number.isFinite(parsed)) {
      setPageLimitByAttr((prev) => ({ ...prev, [relationName]: 0 }));
      return;
    }
    setPageLimitByAttr((prev) => ({ ...prev, [relationName]: parsed }));
  };

  const handlePageLimitSubmit = (relationName: string, rawValue: string) => {
    const totalCount = cardTotalCounts[relationName] ?? 0;
    const minL = getMinPageLimitForTotal(totalCount);
    const parsed = parseInt(String(rawValue).trim(), 10);
    let nextLimit = Number.isFinite(parsed) && parsed > 0
      ? parsed
      : DEFAULT_RELATIONSHIP_PAGE_LIMIT;
    if (totalCount > 0) {
      nextLimit = Math.max(minL, nextLimit);
      if (nextLimit > totalCount) {
        nextLimit = totalCount;
      }
    } else {
      nextLimit = Math.max(1, nextLimit);
    }
    setPageLimitByAttr((prev) => ({ ...prev, [relationName]: nextLimit }));
    resetRelationshipData(relationName, { limit: nextLimit });
  };

  /**
   * Computes column layout based on record count:
   * - >5 records: 1 card per column
   * - 1-5 records: 2 cards per column
   * - 0 records: 2 cards per column (when showEmptyValues is true)
   * During loading, uses relationNames in pairs for stable layout.
   */
  const relationshipColumns = useMemo((): string[][] => {
    const loadedNames = Object.keys(cardData);
    const allLoaded = relationNames.length > 0 && relationNames.every((r) => r in cardData);
    const names = allLoaded ? loadedNames : relationNames;

    if (isEmpty(names)) {
      return [];
    }

    const getCount = (name: string) =>
      cardTotalCounts[name] ?? cardData[name]?.length ?? 0;

    const largeCards: string[] = [];
    const smallCards: string[] = [];
    const emptyCards: string[] = [];

    names.forEach((name) => {
      const count = getCount(name);
      if (count > 5) {
        largeCards.push(name);
      } else if (count >= 1 && count <= 5) {
        smallCards.push(name);
      } else {
        emptyCards.push(name);
      }
    });

    const sortNames = (arr: string[]) =>
      arr.sort((a, b) => a.localeCompare(b));

    sortNames(largeCards);
    sortNames(smallCards);
    sortNames(emptyCards);

    const columns: string[][] = [];

    largeCards.forEach((name) => columns.push([name]));

    for (let i = 0; i < smallCards.length; i += 2) {
      const chunk = smallCards.slice(i, i + 2);
      columns.push(chunk);
    }

    if (checked || !allLoaded) {
      for (let i = 0; i < emptyCards.length; i += 2) {
        const chunk = emptyCards.slice(i, i + 2);
        columns.push(chunk);
      }
    }

    return columns;
  }, [cardData, cardTotalCounts, checked, relationNames]);

  return (
    <Grid container marginTop={0} className="properties-container">
      <Grid item md={12} p={2}>
        <Stack>
          <Stack
            direction="row"
            justifyContent="space-between"
            alignItems="center"
            marginBottom="0.75rem"
            gap="1rem"
          >
            <ToggleButtonGroup
              size="small"
              color="primary"
              value={alignment}
              exclusive
              onChange={handleChange}
              aria-label="Platform"
            >
              <ToggleButton size="small" value="graph">
                Graph
              </ToggleButton>
              <ToggleButton size="small" value="table">
                Table
              </ToggleButton>
            </ToggleButtonGroup>
            {alignment == "table" && (
              <FormGroup>
                <FormControlLabel
                  sx={{ marginRight: "0" }}
                  control={
                    <AntSwitch
                      size="small"
                      sx={{ marginRight: "8px" }}
                      checked={checked}
                      onChange={(e) => {
                        handleSwitchChange(e);
                      }}
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                      inputProps={{ "aria-label": "controlled" }}
                    />
                  }
                  label="Show Empty Values"
                />
              </FormGroup>
            )}
          </Stack>

          {alignment == "graph" && (
            <RelationshipLineage
              entity={entity}
              relationshipAttributes={cardData}
              isLoading={cardLoading && Object.keys(cardData).length === 0}
            />
          )}

          {alignment == "table" && (
            <>
              {showInitialSkeletons ? (
                <div className="relationship-cards-grid relationship-cards-grid--custom">
                  <div className="relationship-cards-column">
                    <RelationshipCardSkeleton />
                    <RelationshipCardSkeleton />
                  </div>
                </div>
              ) : isEmpty(relationNames) ||
                (relationNames.every((r) => r in cardData) &&
                  Object.values(cardData).every((arr) => isEmpty(arr)) &&
                  !checked) ? (
                <div className="relationship-cards-empty">
                  {hasRelationshipApiError
                    ? "Failed to load relationship data"
                    : "No relationship data available"}
                </div>
              ) : (
                <div className="relationship-cards-grid relationship-cards-grid--custom">
                  {relationshipColumns.map((columnNames, colIndex) => (
                    <div
                      key={`col-${colIndex}`}
                      className="relationship-cards-column"
                    >
                      {columnNames.map((attributeName) => {
                        const data = cardData[attributeName];
                        const hasData = attributeName in cardData;
                        if (hasData) {
                          if (isEmpty(data) && !checked) {
                            return null;
                          }
                          return (
                            <RelationshipCard
                              key={attributeName}
                              attributeName={attributeName}
                              data={data ?? []}
                              referredEntities={referredEntities}
                              showEmptyValues={checked}
                              showTypeNameInDisplay={relationNames.length > 1}
                              onLoadMore={handleCardLoadMore}
                              totalCount={cardTotalCounts[attributeName]}
                              isLoading={cardLoadingByName[attributeName]}
                              isResetting={cardResettingByName[attributeName]}
                              isSorted={!!sortByNameByAttr[attributeName]}
                              showDeleted={!!showDeletedByAttr[attributeName]}
                              pageLimit={getPageLimit(
                                attributeName,
                                cardTotalCounts[attributeName]
                              )}
                              onToggleSort={handleToggleSort}
                              onToggleShowDeleted={handleToggleShowDeleted}
                              onPageLimitChange={handlePageLimitChange}
                              onPageLimitSubmit={handlePageLimitSubmit}
                            />
                          );
                        }
                        return (
                          <RelationshipCardSkeleton key={attributeName} />
                        );
                      })}
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </Stack>
      </Grid>
    </Grid>
  );
};

export default RelationshipsTab;
