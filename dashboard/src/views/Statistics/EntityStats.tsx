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
  Button,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography
} from "@mui/material";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  LightTooltip
} from "@components/muiComponents";
import { useAppSelector } from "@hooks/reducerHook";
import { numberFormatWithComma } from "@utils/Helper";
import { isEmpty, serverError } from "@utils/Utils";
import { useLocation, useNavigate } from "react-router-dom";
import { getMetricsGraph } from "@api/apiMethods/metricsApiMethods";
import { useState, useEffect, useRef, SetStateAction } from "react";
import { getStatsValue } from "./Statistics";
import moment from "moment";

const dateRangesMap: Record<string, any> = {
  Today: moment(),
  "1d": [moment().subtract(1, "days"), moment()],
  "7d": [moment().subtract(6, "days"), moment()],
  "14d": [moment().subtract(13, "days"), moment()],
  "30d": [moment().subtract(29, "days"), moment()],
  "This Month": [moment().startOf("month"), moment().endOf("month")],
  "Last Month": [
    moment().subtract(1, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "Last 3 Months": [
    moment().subtract(3, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "Last 6 Months": [
    moment().subtract(6, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "Last 12 Months": [
    moment().subtract(12, "month").startOf("month"),
    moment().subtract(1, "month").endOf("month")
  ],
  "This Quarter": [moment().startOf("quarter"), moment().endOf("quarter")],
  "Last Quarter": [
    moment().subtract(1, "quarter").startOf("quarter"),
    moment().subtract(1, "quarter").endOf("quarter")
  ],
  "This Year": [moment().startOf("year"), moment().endOf("year")],
  "Last Year": [
    moment().subtract(1, "year").startOf("year"),
    moment().subtract(1, "year").endOf("year")
  ]
};

const EntityStats = ({
  currentMetricsData,
  handleClose,
  selectedValue
}: any) => {
  const location = useLocation();
  const navigate = useNavigate();
  const toastId = useRef(null);
  const { metricsData }: any = useAppSelector((state: any) => state.metrics);
  const [entityType, setEntityType] = useState("hive_table");
  const [_metricsGraphData, setMetricsGraphData] = useState({});
  const [_alignment] = useState<string | null>("left");
  const { label } = selectedValue;
  const { metrics } = currentMetricsData;
  let metricsChartDateRange = "7d";
  const selectedMetricsData = label == "Current" ? metricsData : metrics;
  const { entityCount = 0 } = selectedMetricsData?.data?.general || {};
  const { entity } = selectedMetricsData?.data || {};
  let entityData = entity;
  let activeEntities = entityData?.entityActive || {};
  let deletedEntities = entityData?.entityDeleted || {};
  let shellEntities = entityData?.entityShell || {};
  let stats: any = {};
  let activeEntityCount: any = 0;
  let deletedEntityCount: any = 0;
  let shellEntityCount: any = 0;
  // let dateRangeText: Record<string, string> = {
  //   "1d": "last 1 day",
  //   "7d": "last 7 days",
  //   "14d": "last 14 days",
  //   "30d": "last 30 days"
  // };

  useEffect(() => {
    fetchMetricsGraphDetails();
  }, [entityType]);

  const fetchMetricsGraphDetails = async () => {
    let selectedDateRange = dateRangesMap[metricsChartDateRange];
    let startTime = Date.parse(
      getStatsValue({ value: selectedDateRange[0], type: "day" })
    );
    let endTime = Date.parse(
      getStatsValue({ value: selectedDateRange[1], type: "day" })
    );
    let params = {
      typeName: entityType,
      startTime: startTime,
      endTime: endTime
    };
    try {
      const metricsGraphResp = await getMetricsGraph(params);
      const { data } = metricsGraphResp;
      setMetricsGraphData(data);
      // renderStackAreaGraps(data);
    } catch (error) {
      console.error(`Error occur while fetching metrics graph details`, error);
      serverError(error, toastId);
    }
  };

  const createEntityData = (opt: any) => {
    var entityData = opt.entityData,
      type = opt.type;
    Object.entries(entityData).forEach(([key, val]) => {
      let intVal: any = val === undefined ? 0 : val;

      if (type === "active") {
        activeEntityCount += intVal;
      }
      if (type === "deleted") {
        deletedEntityCount += intVal;
      }
      if (type === "shell") {
        shellEntityCount += intVal;
      }

      intVal = numberFormatWithComma(intVal);

      if (stats[key]) {
        stats[key][type] = intVal;
      } else {
        stats[key] = {};
        stats[key][type] = intVal;
      }
    });
  };

  createEntityData({
    entityData: activeEntities,
    type: "active"
  });
  createEntityData({
    entityData: deletedEntities,
    type: "deleted"
  });
  createEntityData({
    entityData: shellEntities,
    type: "shell"
  });

  let statsKeys = Object.keys(stats);

  let sortedKeys = statsKeys.sort(function (a, b) {
    return a.toLowerCase().localeCompare(b.toLowerCase());
  });

  let sortedStats: any = {};
  sortedKeys.forEach(function (key) {
    if (stats.hasOwnProperty(key)) {
      sortedStats[key] = stats[key];
    }
  });

  const handleActiveClick = (key: string) => {
    const searchParams = new URLSearchParams(location.search);

    searchParams.set("searchType", "basic");
    searchParams.set("type", key);

    navigate({
      pathname: "/search/searchResult",
      search: searchParams.toString()
    });
    handleClose();
  };

  const handleDeleteClick = (key: string) => {
    const searchParams: any = new URLSearchParams(location.search);
    let attributes: string[] = [
      "__state",
      "description",
      "name",
      "owner",
      "select",
      "tag",
      "typeName"
    ];
    searchParams.set("type", key);
    searchParams.set("attributes", attributes);
    searchParams.set("includeDE", true);
    // searchParams.set(
    //   "entityFilters",
    //   "AND(__state%3A%3A%3D%3A%3ADELETED%3A%3Astring)"
    // );
    searchParams.set("searchType", "basic");

    navigate({
      pathname: "/search/searchResult",
      search: searchParams.toString()
    });
    handleClose();
  };

  // const handleAlignment = (
  //   _event: React.MouseEvent<HTMLElement>,
  //   newAlignment: string | null
  // ) => {
  //   setAlignment(newAlignment);
  // };

  const handleRowClick = (key: SetStateAction<string>) => {
    setEntityType(key);
  };

  return (
    <Accordion
      sx={{
        width: "100%",
        borderBottom: "1px solid rgba(0, 0, 0, 0.12) !important",
        boxShadow: "none"
      }}
      defaultExpanded
    >
      <AccordionSummary
        aria-controls="technical-properties-content"
        id="technical-properties-header"
      >
        <Stack direction="row" alignItems="center" flex="1">
          <div className="properties-panel-name">
            <Typography fontWeight="600" className="text-color-green">
              {`Entities (${numberFormatWithComma(entityCount)})`}
            </Typography>
          </div>
        </Stack>
      </AccordionSummary>
      <AccordionDetails>
        <Stack gap="1rem">
          <TableContainer component={Paper}>
            <Table className="classificationTable" size="small">
              <TableHead>
                <TableRow>
                  <TableCell>
                    <Typography fontWeight="600">Name</Typography>
                  </TableCell>
                  <TableCell>
                    <Typography fontWeight="600">
                      {" "}
                      Active
                      <span className="count">{`(${numberFormatWithComma(
                        activeEntityCount
                      )})`}</span>
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography fontWeight="600">
                      {" "}
                      Deleted
                      <span className="count">{`(${numberFormatWithComma(
                        deletedEntityCount
                      )})`}</span>
                    </Typography>
                  </TableCell>{" "}
                  <TableCell>
                    <Typography fontWeight="600">
                      {" "}
                      Shell
                      <span className="count">{`(${numberFormatWithComma(
                        shellEntityCount
                      )})`}</span>
                    </Typography>
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {isEmpty(Object.entries(sortedStats)) ? (
                  <TableRow className="empty text-center">
                    <TableCell colSpan={4} align="center">
                      <span>No records found!</span>
                    </TableCell>
                  </TableRow>
                ) : (
                  Object.entries(sortedStats).map(([key, value]: any) => (
                    <TableRow
                      key={key}
                      onClick={() => handleRowClick(key)}
                      sx={{
                        cursor: "pointer",
                        backgroundColor:
                          entityType === key ? "#e0e0e0" : "transparent",
                        "&:hover": {
                          backgroundColor: "#f5f5f5"
                        }
                      }}
                    >
                      <TableCell>{`${key} (${
                        Number(!isEmpty(value.active) ? value.active : 0) +
                        Number(!isEmpty(value.deleted) ? value.deleted : 0) +
                        Number(!isEmpty(value.shell) ? value.shell : 0)
                      })`}</TableCell>
                      <TableCell>
                        <LightTooltip
                          title={`Search for active entities of type '${key}'`}
                        >
                          <Button
                            className="entity-name text-blue text-decoration-none"
                            onClick={() => {
                              handleActiveClick(key);
                            }}
                            color="primary"
                          >
                            {!isEmpty(value.active) ? value.active : 0}
                          </Button>
                        </LightTooltip>
                      </TableCell>
                      <TableCell>
                        {!isEmpty(value.deleted) ? (
                          <LightTooltip
                            title={`Search for deleted entities  of type '${key}'`}
                          >
                            <Button
                              className="entity-name text-red text-decoration-none"
                              onClick={() => {
                                handleDeleteClick(key);
                              }}
                              color="error"
                            >
                              {value.deleted}
                            </Button>
                          </LightTooltip>
                        ) : (
                          0
                        )}
                      </TableCell>
                      <TableCell>
                        {!isEmpty(value.shell) ? (
                          <LightTooltip
                            title={`Search for deleted entities  of type '${key}'`}
                          >
                            <Button
                              className="entity-name text-red text-decoration-none"
                              // onClick={() => {
                              //   handleClick(key);
                              // }}
                              color="error"
                            >
                              {value.shell}
                            </Button>
                          </LightTooltip>
                        ) : (
                          0
                        )}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </TableContainer>
          {/* <Stack
            direction="row"
            justifyContent="space-between"
            alignItems="center"
            marginBottom="0.75rem"
          >
            <Typography fontWeight="600">{`${entityType} chart for ${dateRangeText[metricsChartDateRange]}`}</Typography>
            <ToggleButtonGroup
              value={alignment}
              exclusive
              onChange={handleAlignment}
              aria-label="text alignment"
              size="small"
            >
              <ToggleButton value="left" aria-label="left aligned">
                1d
              </ToggleButton>
              <ToggleButton value="center" aria-label="centered">
                7d
              </ToggleButton>
              <ToggleButton value="right" aria-label="right aligned">
                14d{" "}
              </ToggleButton>
              <ToggleButton value="justify" aria-label="justified">
                30d{" "}
              </ToggleButton>
            </ToggleButtonGroup>
          </Stack>
          {!isEmpty(metricsGraphData) && (
            <StackedAreaCharts
              metricsGraphData={Object.values(metricsGraphData)[0]}
            />
          )} */}
        </Stack>
      </AccordionDetails>
    </Accordion>
  );
};

export default EntityStats;
