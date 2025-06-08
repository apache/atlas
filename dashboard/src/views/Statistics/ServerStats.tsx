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
  AccordionSummary
} from "@components/muiComponents";
import { numberFormatWithComma } from "@utils/Helper";
import { useAppSelector } from "@hooks/reducerHook";
import { isEmpty } from "@utils/Utils";
import { stats } from "@utils/Enum";
import { getStatsValue } from "./Statistics";

const ServerStats = ({ selectedValue, currentMetricsData }: any) => {
  const { metricsData }: any = useAppSelector((state: any) => state.metrics);
  const { label } = selectedValue;
  const { metrics } = currentMetricsData;
  const selectedMetricsData = label == "Current" ? metricsData : metrics;
  const { general } = selectedMetricsData?.data || {};
  let statsData = general?.stats || {};

  const generateStatusData = (stateObject: any) => {
    let stats: any = {};

    for (let key in stateObject) {
      let keys: string[] = key.split(":");
      key = keys[0];
      let subKey = keys[1];
      if (stats[key]) {
        stats[key][subKey] = stateObject[`${key}:${subKey}`];
      } else {
        stats[key] = {};
        stats[key][subKey] = stateObject[`${key}:${subKey}`];
      }
    }

    return stats;
  };

  let serverData = generateStatusData(statsData);

  const offsetTableColumn = (obj: {
    [x: string]: any;
    hasOwnProperty: (arg0: string) => any;
  }) => {
    var returnObj = [];
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        returnObj.push({ label: key, dataValue: obj[key] });
      }
    }
    return returnObj;
  };

  let offSetTabelData = offsetTableColumn(
    serverData?.Notification?.topicDetails
  );

  let notificationTableHeader = [
    "Count",
    "Avg",
    "Time (ms)",
    "Creates",
    "Updates",
    "Deletes"
    // "Failed"
  ];

  let topicOffsetTableCol = [
    "Kafka Topic-Partition",
    "Start Offset",
    "Current Offset",
    "Processed",
    "Failed",
    "Last Message Processed Time"
  ];
  let topciOffsetTableHeader = [
    "offsetStart",
    "offsetCurrent",
    "processedMessageCount",
    "failedMessageCount",
    "lastMessageProcessedTime"
  ];

  let tableCol = [
    {
      label:
        "Total from " +
        getStatsValue({
          value: serverData?.Server?.["startTimeStamp"],
          type: stats?.Server?.["startTimeStamp"]
        }),
      key: "total"
    },
    {
      label:
        "Current Hour from " +
        getStatsValue({
          value: serverData?.Notification?.["currentHourStartTime"],
          type: stats.Notification?.["currentHourStartTime"]
        }),
      key: "currentHour"
    },
    { label: "Previous Hour", key: "previousHour" },
    {
      label:
        "Current Day from " +
        getStatsValue({
          value: serverData?.Notification?.["currentDayStartTime"],
          type: stats?.Notification?.["currentDayStartTime"]
        }),
      key: "currentDay"
    },
    { label: "Previous Day", key: "previousDay" }
  ];

  let tableHeader = [
    "count",
    "AvgTime",
    "EntityCreates",
    "EntityUpdates",
    "EntityDeletes",
    "Failed"
  ];

  const getTmplValue = (
    argument: { label?: string; key: any },
    args: string
  ) => {
    let pickValueFrom = argument.key.concat(args);
    if (argument.key == "total" && args == "EntityCreates") {
      pickValueFrom = "totalCreates";
    } else if (argument.key == "total" && args == "EntityUpdates") {
      pickValueFrom = "totalUpdates";
    } else if (argument.key == "total" && args == "EntityDeletes") {
      pickValueFrom = "totalDeletes";
    } else if (args == "count") {
      pickValueFrom = argument.key;
    }
    let returnVal = serverData?.Notification?.[pickValueFrom];
    return returnVal ? numberFormatWithComma(returnVal) : 0;
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
              Server Statistics
            </Typography>
          </div>
        </Stack>
      </AccordionSummary>
      <AccordionDetails>
        <Stack gap={"2rem"}>
          <TableContainer>
            <Table className="classificationTable" size="small">
              <TableHead>
                <TableRow>
                  <TableCell>
                    <Typography fontWeight="600">Server Details</Typography>
                  </TableCell>
                  <TableCell align="right">
                    <Typography fontWeight="600"></Typography>
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {isEmpty(serverData?.Server) ? (
                  <TableRow className="empty text-center">
                    <TableCell colSpan={2} align="center">
                      <span>No records found!</span>
                    </TableCell>
                  </TableRow>
                ) : (
                  Object.entries(serverData?.Server)?.map(
                    ([key, value]: any) => (
                      <TableRow key={key}>
                        <TableCell>{key}</TableCell>
                        <TableCell align="right">
                          {getStatsValue({
                            value: value,
                            type: {
                              ...stats.Server,
                              ...stats.ConnectionStatus,
                              ...stats.generalData
                            }[key]
                          })}
                        </TableCell>
                      </TableRow>
                    )
                  )
                )}
              </TableBody>
            </Table>
          </TableContainer>
          <Stack gap={"2rem"}>
            <Typography fontWeight="600">Notification Table</Typography>
            <TableContainer>
              <Table className="classificationTable" size="small">
                <TableHead>
                  <TableRow>
                    {topicOffsetTableCol.map((col) => {
                      return (
                        <>
                          <TableCell align="left">
                            <Typography fontWeight="600">{col}</Typography>
                          </TableCell>
                        </>
                      );
                    })}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {isEmpty(offSetTabelData) ? (
                    <TableRow className="empty text-center">
                      <TableCell colSpan={2} align="center">
                        <span>No records found!</span>
                      </TableCell>
                    </TableRow>
                  ) : (
                    offSetTabelData?.map((obj, index) => (
                      <TableRow key={index}>
                        <TableCell>{obj.label}</TableCell>
                        {topciOffsetTableHeader.map((header) => {
                          let returnVal =
                            serverData?.Notification?.topicDetails?.[
                              obj.label
                            ]?.[header];
                          return (
                            <TableCell align="left">
                              {returnVal
                                ? getStatsValue({
                                    value: returnVal,
                                    type: stats.Notification[header]
                                  })
                                : 0}
                            </TableCell>
                          );
                        })}
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </TableContainer>
            <TableContainer>
              <Table className="classificationTable" size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>
                      <Typography fontWeight="600">Period</Typography>
                    </TableCell>
                    {notificationTableHeader.map((header) => {
                      return (
                        <>
                          <TableCell align="right">
                            <Typography fontWeight="600">{header}</Typography>
                          </TableCell>
                        </>
                      );
                    })}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {isEmpty(tableCol) ? (
                    <TableRow className="empty text-center">
                      <TableCell colSpan={2} align="center">
                        <span>No records found!</span>
                      </TableCell>
                    </TableRow>
                  ) : (
                    tableCol?.map((obj, index) => (
                      <TableRow key={index}>
                        <TableCell>{obj.label}</TableCell>
                        {tableHeader.map((header) => (
                          <TableCell align="right">
                            {getTmplValue(obj, header)}
                          </TableCell>
                        ))}{" "}
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          </Stack>
        </Stack>
      </AccordionDetails>
    </Accordion>
  );
};

export default ServerStats;
