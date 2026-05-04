//@t

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

import { Chip, Stack, Typography } from "@mui/material";
import { attributeFilter } from "@utils/CommonViewFunction";
import {
  queryBuilderDateRangeUIValueToAPI,
  systemAttributes,
  filterQueryValue,
  getDisplayOperator
} from "@utils/Enum";
import { removeCriterionFromFilterUrl } from "@utils/filterUrlCriterionRemoval";
import type { ParsedApiFilter } from "@utils/filterUrlCriterionRemoval";
import { dateTimeFormat } from "@utils/Global";
import moment from "moment";
import { useLocation, useNavigate } from "react-router-dom";
import { globalSearchFilterInitialQuery } from "@utils/Utils";

export const FilterQuery = ({ value }: any) => {
  const location = useLocation();
  const navigate = useNavigate();
  function objToString(
    filterObj: { rules: { [x: string]: any } },
    type: string
  ) {
    return Object.keys(filterObj?.rules || {})?.map((key) => {
      let obj = { ...filterObj.rules[key] };

      if (Object.prototype.hasOwnProperty.call(obj, "condition")) {
        return (
          <Typography className="operator" fontWeight="600">
            {obj.condition}
          </Typography>
        );
      } else {
        const isDateAttr =
          obj.type === "date" ||
          obj.id === "createTime" ||
          obj.id === "__timestamp" ||
          obj.id === "__modificationTimestamp";
        if (isDateAttr) {
          if (queryBuilderDateRangeUIValueToAPI[obj.value]) {
            obj.value = queryBuilderDateRangeUIValueToAPI[obj.value];
          } else if (/^\d+$/.test(String(obj.value))) {
            obj.value = `${moment(Number(obj.value)).format(dateTimeFormat)} (${moment.tz(moment.tz.guess()).zoneAbbr()})`;
          } else {
            obj.value = `${obj.value} (${moment.tz(moment.tz.guess()).zoneAbbr()})`;
          }
        }

        return (
          <Chip
            color="primary"
            className="chip-items"
            data-type={type}
            data-rule-key={key}
            data-id={`${obj.id}${key}`}
            data-cy={`${obj.id}${key}`}
            label={
              <>
                <Stack direction="row" gap="0.125em">
                  <Typography className="searchKey">
                    {systemAttributes[obj.id]
                      ? systemAttributes[obj.id]
                      : obj.id}
                  </Typography>
                  <Typography className="operator" fontWeight="600">
                    {getDisplayOperator(obj.operator)}{" "}
                  </Typography>
                  <Typography className="searchValue">
                    {filterQueryValue[obj.id]
                      ? filterQueryValue[obj.id][obj.value]
                      : obj.value}
                  </Typography>
                </Stack>
              </>
            }
            onDelete={(e) => {
              clearQueryAttr(e);
            }}
            size="small"
            variant="outlined"
            clickable
          />
        );
      }
    });
  }

  let queryArray = [];

  const clearQueryAttr = (e: any) => {
    const searchParams = new URLSearchParams(location.search);
    const chipEl = (e?.target as HTMLElement)?.closest?.(
      "[data-type]"
    ) as HTMLElement | null;
    const currentType = chipEl?.getAttribute("data-type");
    const ruleKeyRaw = chipEl?.getAttribute("data-rule-key");

    const navigateAfterChange = (sp: URLSearchParams) => {
      const meaningfulFilterParams = [
        "type",
        "tag",
        "query",
        "term",
        "relationshipName",
        "entityFilters",
        "tagFilters",
        "relationshipFilters",
        "excludeST",
        "excludeSC",
        "includeDE"
      ];
      const hasMeaningfulFilters = meaningfulFilterParams.some((param) =>
        sp.has(param)
      );
      if (!hasMeaningfulFilters) {
        navigate({
          pathname: "/search"
        });
      } else if ([...sp]?.length <= 1) {
        navigate({
          pathname: "/search"
        });
      } else {
        navigate({
          pathname: "/search/searchResult",
          search: sp.toString()
        });
      }
    };

    const syncFilterParamGlobalState = (
      paramKey: "entityFilters" | "tagFilters" | "relationshipFilters",
      urlValue: string | null
    ) => {
      if (!urlValue) {
        globalSearchFilterInitialQuery.setQuery({ [paramKey]: [] });
        return;
      }
      const api = attributeFilter.extractUrl({
        value: urlValue,
        apiObj: true
      }) as ParsedApiFilter | null;
      if (!api?.criterion || !Array.isArray(api.criterion)) {
        globalSearchFilterInitialQuery.setQuery({ [paramKey]: [] });
        return;
      }
      globalSearchFilterInitialQuery.setQuery({
        [paramKey]: {
          combinator: String(api.condition || "AND").toLowerCase(),
          rules: api.criterion.map((rule: any, i: number) => ({
            id: `url-rule-${i}`,
            field: rule.attributeName,
            operator: getDisplayOperator(rule.operator) || rule.operator,
            value: rule.attributeValue,
            ...(rule.type ? { type: rule.type } : {})
          }))
        }
      });
    };

    const isFilterCriterionChip =
      ruleKeyRaw !== null &&
      ruleKeyRaw !== "" &&
      (currentType === "entityFilters" ||
        currentType === "tagFilters" ||
        currentType === "relationshipFilters");

    if (isFilterCriterionChip) {
      const idx = Number.parseInt(ruleKeyRaw as string, 10);
      if (Number.isNaN(idx)) {
        return;
      }
      const paramKey = currentType as
        | "entityFilters"
        | "tagFilters"
        | "relationshipFilters";
      const raw = searchParams.get(paramKey);
      if (!raw) {
        return;
      }
      const allowSimplify = paramKey === "entityFilters";
      const result = removeCriterionFromFilterUrl(raw, idx, allowSimplify);
      if (!result) {
        return;
      }
      if (result.kind === "empty") {
        searchParams.delete(paramKey);
        globalSearchFilterInitialQuery.setQuery({ [paramKey]: [] });
        navigateAfterChange(searchParams);
        return;
      }
      if (result.kind === "singleTypeName") {
        searchParams.set("type", result.typeName);
        searchParams.delete("entityFilters");
        if (searchParams.get("includeDE") === "true") {
          const delUrl = attributeFilter.generateUrl({
            value: {
              condition: "AND",
              criterion: [
                {
                  attributeName: "__state",
                  operator: "eq",
                  attributeValue: "DELETED"
                }
              ]
            }
          });
          if (delUrl) {
            searchParams.set("entityFilters", delUrl);
          }
        }
        syncFilterParamGlobalState(
          "entityFilters",
          searchParams.get("entityFilters")
        );
        navigateAfterChange(searchParams);
        return;
      }
      searchParams.set(paramKey, result.url);
      syncFilterParamGlobalState(paramKey, result.url);
      navigateAfterChange(searchParams);
      return;
    }

    if (currentType == "term") {
      searchParams.delete("gtype");
      searchParams.delete("viewType");
      searchParams.delete("guid");
    }

    if (currentType === "type") {
      searchParams.delete("entityFilters");
      globalSearchFilterInitialQuery.setQuery({ entityFilters: [] });
    }
    if (currentType === "tag") {
      searchParams.delete("tagFilters");
      globalSearchFilterInitialQuery.setQuery({ tagFilters: [] });
    }
    if (currentType === "relationshipName") {
      searchParams.delete("relationshipFilters");
      globalSearchFilterInitialQuery.setQuery({ relationshipFilters: [] });
    }
    if (
      currentType === "entityFilters" ||
      currentType === "tagFilters" ||
      currentType === "relationshipFilters"
    ) {
      globalSearchFilterInitialQuery.setQuery({ [currentType]: [] });
    }

    if (currentType) {
      searchParams.delete(currentType);
    }

    navigateAfterChange(searchParams);
  };

  if (value.type) {
    let typeKeyValue = (
      <Chip
        color="primary"
        className="chip-items"
        data-type="type"
        label={
          <>
            <Stack direction="row">
              <Typography className="searchKey" gap="0.125em">
                Type:
              </Typography>
              <Typography className="searchValue">{value.type}</Typography>
            </Stack>
          </>
        }
        onDelete={(e) => {
          clearQueryAttr(e);
        }}
        size="small"
        variant="outlined"
        clickable
      />
    );
    let entityFilters: any = attributeFilter.extractUrl({
      value: value.entityFilters,
      formatDate: true
    });
    if (entityFilters) {
      const conditionForEntity =
        entityFilters?.rules?.length === 1 ? "" : "AND";
      typeKeyValue = (
        <>
          {typeKeyValue}

          <Typography className="operator" fontWeight="600">
            {conditionForEntity}
          </Typography>
          <>
            <Typography fontWeight="600">{"("}</Typography>
            <Typography className="operator" fontWeight="600">
              {entityFilters.condition}
            </Typography>
            <Typography fontWeight="600">{"("}</Typography>
            {objToString(entityFilters, "entityFilters")}
            <Typography fontWeight="600">{")"}</Typography>
            <Typography fontWeight="600">{")"}</Typography>
          </>
        </>
      );
    }

    queryArray.push(
      <Stack direction="row" flexWrap="wrap" gap="0.25rem" alignItems="center">
        {typeKeyValue}
      </Stack>
    );
  }

  // Handle Tag filters
  if (value.tag) {
    let tagKeyValue = (
      <Chip
        color="primary"
        className="chip-items"
        data-type="tag"
        label={
          <>
            <Stack direction="row" gap="0.125em" alignItems="center">
              <Typography className="searchKey">Classification: </Typography>
              <Typography className="searchValue">{value.tag}</Typography>
            </Stack>
          </>
        }
        onDelete={(e) => {
          clearQueryAttr(e);
        }}
        size="small"
        variant="outlined"
        clickable
      />
    );
    let tagFilters: any = attributeFilter.extractUrl({
      value: value.tagFilters,
      formatDate: true
    });
    if (tagFilters) {
      const conditionForTag = tagFilters?.rules?.length === 1 ? "" : "AND";

      tagKeyValue = (
        <>
          {tagKeyValue}
          <Typography className="operator" fontWeight="600">
            {conditionForTag}
          </Typography>
          <>
            <Typography fontWeight="600">{"("}</Typography>
            <Typography className="operator" fontWeight="600">
              {tagFilters.condition}
            </Typography>
            <Typography fontWeight="600">{"("}</Typography>
            {objToString(tagFilters, "tagFilters")}
            <Typography fontWeight="600">{")"}</Typography>
            <Typography fontWeight="600">{")"}</Typography>
          </>
        </>
      );
    }
    queryArray.push(
      <Stack direction="row" flexWrap="wrap" gap="0.25rem" alignItems="center">
        {tagKeyValue}
      </Stack>
    );
  }

  // Handle Relationship filters
  if (value.relationshipName) {
    let relationshipKeyValue: any = (
      <Chip
        color="primary"
        className="chip-items"
        data-type="relationshipName"
        label={
          <>
            <Stack direction="row" gap="0.125em">
              <Typography className="searchKey">Relationship: </Typography>
              <Typography className="searchValue">
                {value.relationshipName}
              </Typography>
            </Stack>
          </>
        }
        onDelete={(e) => {
          clearQueryAttr(e);
        }}
        size="small"
        variant="outlined"
        clickable
      />
    );
    let relationshipFilters: any = attributeFilter.extractUrl({
      value: value.relationshipFilters,
      formatDate: true
    });

    if (value.relationshipFilters) {
      const conditionForRelationship: any =
        value.relationshipFilters.rules &&
        value?.relationshipFilters?.rules?.length === 1
          ? ""
          : "AND";
      relationshipKeyValue = (
        <>
          {relationshipKeyValue}

          <Typography className="operator" fontWeight="600">
            {conditionForRelationship}
          </Typography>
          <>
            <Typography fontWeight="600">{"("}</Typography>
            <Typography className="operator" fontWeight="600">
              {relationshipFilters.condition}
            </Typography>
            <Typography fontWeight="600">{"("}</Typography>
            {objToString(relationshipFilters, "relationshipFilters")}
            <Typography fontWeight="600">{")"}</Typography>
            <Typography fontWeight="600">{")"}</Typography>
          </>
        </>
      );
    }

    queryArray.push(
      <Stack direction="row" flexWrap="wrap" gap="0.25em" alignItems="center">
        {relationshipKeyValue}
      </Stack>
    );
  }

  // Handle Term filter
  if (value.term) {
    let termKeyValue = (
      <Chip
        color="primary"
        className="chip-items"
        data-type="term"
        label={
          <>
            <Stack direction="row" gap="0.125em">
              <Typography className="searchKey">Term: </Typography>
              <Typography className="searchValue">{value.term}</Typography>
            </Stack>
          </>
        }
        onDelete={(e) => {
          clearQueryAttr(e);
        }}
        size="small"
        variant="outlined"
        clickable
      />
    );

    queryArray.push(<div className="group">{termKeyValue}</div>);
  }

  // Handle Query filter
  if (value.query) {
    let queryKeyValue = (
      <Chip
        color="primary"
        className="chip-items"
        data-type="query"
        label={
          <>
            <Stack direction="row" gap="0.125em">
              <Typography className="searchKey">Query: </Typography>
              <Typography className="searchValue">
                {value.query.trim()}
              </Typography>
            </Stack>
          </>
        }
        onDelete={(e) => {
          clearQueryAttr(e);
        }}
        size="small"
        variant="outlined"
        clickable
      />
    );

    queryArray.push(<div className="group">{queryKeyValue}</div>);
  }

  // Handle Exclude ST filter
  if (value.excludeST) {
    let queryExcludeST = (
      <Chip
        color="primary"
        className="chip-items"
        data-type="excludeST"
        label={
          <>
            <Stack direction="row" gap="0.125em">
              <Typography className="searchKey">Exclude sub-types: </Typography>
              <Typography className="searchValue">{value.excludeST}</Typography>
            </Stack>
          </>
        }
        onDelete={(e) => {
          clearQueryAttr(e);
        }}
        size="small"
        variant="outlined"
        clickable
      />
    );

    queryArray.push(<div className="group">{queryExcludeST}</div>);
  }

  // Handle Exclude SC filter
  if (value.excludeSC) {
    let queryExcludeSC = (
      <Chip
        color="primary"
        className="chip-items"
        data-type="excludeSC"
        label={
          <>
            <Stack direction="row" gap="0.125em">
              <Typography className="searchKey">
                Exclude sub-classifications:{" "}
              </Typography>
              <Typography className="searchValue">{value.excludeSC}</Typography>
            </Stack>
          </>
        }
        onDelete={(e) => {
          clearQueryAttr(e);
        }}
        size="small"
        variant="outlined"
        clickable
      />
    );

    queryArray.push(<div className="group">{queryExcludeSC}</div>);
  }

  // Handle Include DE filter
  if (value.includeDE) {
    let queryIncludeDE = (
      <Chip
        color="primary"
        className="chip-items"
        data-type="includeDE"
        label={
          <>
            <Stack direction="row" gap="0.125em">
              <Typography className="searchKey">
                Show historical entities:{" "}
              </Typography>
              <Typography className="searchValue">{value.includeDE}</Typography>
            </Stack>
          </>
        }
        onDelete={(e) => {
          clearQueryAttr(e);
        }}
        size="small"
        variant="outlined"
        clickable
      />
    );

    queryArray.push(<div className="group">{queryIncludeDE}</div>);
  }

  // If no filters to display, return null (don't show empty parentheses)
  if (queryArray.length === 0) {
    return null;
  }

  return (
    <>
      {queryArray.length === 1 ? (
        <>{queryArray}</>
      ) : (
        <>
          <Typography fontWeight="600">(</Typography>
          {queryArray.map((item, index) => (
            <>
              {index > 0 && (
                <>
                  <Typography
                    className="operator"
                    whiteSpace="nowrap"
                    fontWeight="600"
                  >
                    {")"} AND {"("}
                  </Typography>
                </>
              )}
              {item}
            </>
          ))}
          <Typography fontWeight="600">)</Typography>
        </>
      )}
    </>
  );
};

export default FilterQuery;
