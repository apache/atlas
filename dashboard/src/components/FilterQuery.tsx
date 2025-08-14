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
  filterQueryValue
} from "@utils/Enum";
import moment from "moment";
import { useLocation, useNavigate } from "react-router-dom";

export const FilterQuery = ({ value }: any) => {
  const location = useLocation();
  const navigate = useNavigate();
  function objToString(
    filterObj: { rules: { [x: string]: any } },
    type: string
  ) {
    return Object.keys(filterObj?.rules || {})?.map((key) => {
      let obj = { ...filterObj.rules[key] };

      if (obj.hasOwnProperty("condition")) {
        return (
          <Typography className="operator" fontWeight="600">
            {obj.condition}
          </Typography>
        );
      } else {
        if (obj.type === "date" || obj.id == "createTime") {
          if (queryBuilderDateRangeUIValueToAPI[obj.value]) {
            obj.value = queryBuilderDateRangeUIValueToAPI[obj.value];
          } else {
            obj.value = `${obj.value} (${moment
              .tz(moment.tz.guess())
              .zoneAbbr()})`;
          }
        }

        return (
          <Chip
            color="primary"
            className="chip-items"
            data-type={type}
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
                    {obj.operator}{" "}
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
    const currentType: any = e?.target
      ?.closest("[data-type]")
      ?.getAttribute("data-type");

    if (currentType == "term") {
      searchParams.delete("gtype");
      searchParams.delete("viewType");
      searchParams.delete("guid");
    }

    searchParams.delete(currentType);

    if ([...searchParams]?.length == 1) {
      navigate({
        pathname: "/search"
      });
    } else {
      navigate({
        pathname: "/search/searchResult",
        search: searchParams.toString()
      });
    }
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
