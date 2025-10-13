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
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
  ToggleButton,
  ToggleButtonGroup,
  FormControlLabel,
  Radio,
  RadioGroup
} from "@mui/material";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  CustomButton,
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
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from "recharts";
import GraphCustomTooltip from "./StatsGraphs/GraphCustomTooltip";
import { statsDateRangesMap } from "@utils/Enum";

const dateRangeText = {
  "1d": "last 1 day",
  "7d": "last 7 days",
  "14d": "last 14 days",
  "30d": "last 30 days"
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
  const [metricsGraphData, setMetricsGraphData] = useState<
    Record<string, { values: [number, number][] }>
  >({});
  const [chartTimeLineList, setChartTimeLineList] =
    useState<keyof typeof dateRangeText>("7d");
  const { label } = selectedValue;
  const { metrics } = currentMetricsData;
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
  const [chartMode, setChartMode] = useState("stacked");
  const [activeKeys, setActiveKeys] = useState({
    Active: true,
    Deleted: true,
    Shell: true
  });

  const handleChartModeChange = (
    _event: React.MouseEvent<HTMLElement>,
    newMode: string | null
  ) => {
    if (newMode) {
      setChartMode(newMode);
    }
  };

  const getTransformedData = () => {
    if (chartMode === "expanded") {
      return transformedData.map((entry) => {
        const total = entry.Active + entry.Deleted + entry.Shell;
        return {
          ...entry,
          Active: (entry.Active / total) * 100 || 0,
          Deleted: (entry.Deleted / total) * 100 || 0,
          Shell: (entry.Shell / total) * 100 || 0
        };
      });
    }
    return transformedData;
  };

  useEffect(() => {
    fetchMetricsGraphDetails();
  }, [entityType, chartTimeLineList]);

  const fetchMetricsGraphDetails = async () => {
    let selectedDateRange = statsDateRangesMap[chartTimeLineList];
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
    searchParams.set("searchType", "basic");

    navigate({
      pathname: "/search/searchResult",
      search: searchParams.toString()
    });
    handleClose();
  };

  const handleAlignment = (
    _event: React.MouseEvent<HTMLElement>,
    newAlignment: string | null
  ) => {
    if (newAlignment != null) {
      setChartTimeLineList(newAlignment as keyof typeof dateRangeText);
    }
  };

  const handleRowClick = (key: SetStateAction<string>) => {
    setEntityType(key);
  };

  const handleLegendClick = (dataKey: string) => {
    setActiveKeys((prevState) => ({
      ...prevState,
      [dataKey as keyof typeof activeKeys]:
        !prevState[dataKey as keyof typeof activeKeys]
    }));
  };

  const transformedData = Array.isArray(
    Object.values(metricsGraphData || {})?.flat()?.[0]?.values
  )
    ? Object.values(metricsGraphData || {})
        ?.flat()?.[0]
        ?.values.map((_, index) => {
          const data = Object.values(metricsGraphData).flat();
          return {
            timestamp: data[0]?.values[index]?.[0] || 0,
            Active: data[0]?.values[index]?.[1] || 0,
            Deleted: data[1]?.values[index]?.[1] || 0,
            Shell: data[2]?.values[index]?.[1] || 0
          };
        })
    : [];

  const getColorForKey = (key: string) => {
    const colors = {
      Active: "rgb(31, 119, 180)",
      Deleted: "rgb(174, 199, 232)",
      Shell: "rgb(255, 127, 14)"
    };
    return colors[key as keyof typeof colors] || "#000";
  };

  return (
    <Accordion className="entity-stats-accordion" defaultExpanded>
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
                <TableRow hover className="table-header-row">
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
                      <TableCell>{`${key} (${numberFormatWithComma(
                        Number(value?.active ?? 0) +
                          Number(value?.deleted ?? 0)
                      )})`}</TableCell>
                      <TableCell>
                        <LightTooltip
                          title={`Search for active entities of type '${key}'`}
                        >
                          <CustomButton
                            className="entity-name text-blue text-decoration-none"
                            onClick={() => {
                              handleActiveClick(key);
                            }}
                            size="small"
                            color="primary"
                          >
                              {numberFormatWithComma(
                                Number(!isEmpty(value.active) ? value.active : 0)
                              )}
                          </CustomButton>
                        </LightTooltip>
                      </TableCell>
                      <TableCell>
                        {!isEmpty(value.deleted) ? (
                          <LightTooltip
                            title={`Search for deleted entities  of type '${key}'`}
                          >
                            <CustomButton
                              sx={{ padding: "0" }}
                              className="entity-name  text-red text-decoration-none"
                              onClick={() => {
                                handleDeleteClick(key);
                              }}
                              color="error"
                              size="small"
                            >
                              {numberFormatWithComma(
                                Number(!isEmpty(value.deleted) ? value.deleted : 0)
                              )}
                            </CustomButton>
                          </LightTooltip>
                        ) : (
                          numberFormatWithComma(0)
                        )}
                      </TableCell>
                      <TableCell>
                        {!isEmpty(value.shell) ? (
                          <LightTooltip
                            title={`Search for deleted entities  of type '${key}'`}
                          >
                            <CustomButton
                              sx={{ padding: "0" }}
                              className="entity-name text-red text-decoration-none"
                              // onClick={() => {
                              //   handleClick(key);
                              // }}
                              size="small"
                              color="error"
                            >
                              {numberFormatWithComma(
                                Number(!isEmpty(value.shell) ? value.shell : 0)
                              )}
                            </CustomButton>
                          </LightTooltip>
                        ) : (
                          numberFormatWithComma(0)
                        )}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </TableContainer>
          <Stack
            direction="row"
            justifyContent="space-between"
            alignItems="center"
          >
            <Typography
              fontSize="16px"
              fontWeight="600"
            >{`${entityType} chart for ${dateRangeText[chartTimeLineList]}`}</Typography>
            <ToggleButtonGroup
              value={chartTimeLineList}
              exclusive
              defaultValue={"7d"}
              onChange={handleAlignment}
              size="small"
              sx={{
                "& .MuiToggleButtonGroup-grouped": {
                  fontWeight: "600"
                }
              }}
            >
              <ToggleButton size="small" value="1d">
                1d
              </ToggleButton>
              <ToggleButton size="small" value="7d">
                7d
              </ToggleButton>
              <ToggleButton size="small" value="14d">
                14d{" "}
              </ToggleButton>
              <ToggleButton size="small" value="30d">
                30d{" "}
              </ToggleButton>
            </ToggleButtonGroup>
          </Stack>

          <Stack
            direction="row"
            justifyContent="flex-start"
            alignItems="center"
          >
            <RadioGroup
              row
              value={chartMode}
              onChange={(event) =>
                handleChartModeChange(
                  event as unknown as React.MouseEvent<HTMLElement>,
                  event.target.value
                )
              }
            >
              {["stacked", "stream", "expanded"].map((mode) => (
                <FormControlLabel
                  key={mode}
                  value={mode}
                  control={<Radio size="small" />}
                  label={mode.charAt(0).toUpperCase() + mode.slice(1)}
                />
              ))}
            </RadioGroup>
          </Stack>

          {!isEmpty(metricsGraphData) && (
            <ResponsiveContainer width="100%" height={400}>
              <AreaChart
                data={getTransformedData()}
                margin={{ top: 10, right: 60, left: 0, bottom: 0 }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="timestamp"
                  tickFormatter={(tick) => moment(tick).format("MM/DD/YYYY")}
                />

                <YAxis
                  domain={
                    chartMode === "stream"
                      ? [
                          (dataMin: number) => dataMin - 10,
                          (dataMax: number) => dataMax + 10
                        ]
                      : ["auto", "auto"]
                  }
                  tickFormatter={(tick) =>
                    chartMode === "expanded"
                      ? `${tick.toFixed(2)}%`
                      : tick.toFixed(2)
                  }
                />

                <Tooltip
                  content={<GraphCustomTooltip />}
                  cursor={{ stroke: "rgba(0, 0, 0, 0.1)", strokeWidth: 2 }}
                />
                <Legend
                  onClick={(e) => {
                    if (e && e.id) {
                      handleLegendClick(e.id);
                    }
                  }}
                  payload={Object.keys(activeKeys).map((key) => ({
                    id: key,
                    type: "square",
                    value: key,
                    color:
                      activeKeys[key as keyof typeof activeKeys] === true
                        ? getColorForKey(key)
                        : "#d3d3d3",
                    inactive: !activeKeys[key as keyof typeof activeKeys]
                  }))}
                />
                {activeKeys.Active && (
                  <Area
                    type={chartMode === "stream" ? "basis" : "monotone"}
                    dataKey="Active"
                    stackId={chartMode === "expanded" ? undefined : "1"}
                    stroke="rgb(31, 119, 180)"
                    fill="rgb(31, 119, 180)"
                  />
                )}
                {activeKeys.Deleted && (
                  <Area
                    type={chartMode === "stream" ? "basis" : "monotone"}
                    dataKey="Deleted"
                    stackId={chartMode === "expanded" ? undefined : "1"}
                    stroke="rgb(174, 199, 232)"
                    fill="rgb(174, 199, 232)"
                  />
                )}
                {activeKeys.Shell && (
                  <Area
                    type={chartMode === "stream" ? "basis" : "monotone"}
                    dataKey="Shell"
                    stackId={chartMode === "expanded" ? undefined : "1"}
                    stroke="rgb(255, 127, 14)"
                    fill="rgb(255, 127, 14)"
                  />
                )}
              </AreaChart>
            </ResponsiveContainer>
          )}
        </Stack>
      </AccordionDetails>
    </Accordion>
  );
};

export default EntityStats;
