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

import { useEffect, useMemo, useRef, useState } from "react";
import { getDebugMetrics } from "@api/apiMethods/metricsApiMethods";
import {
  AutorenewIcon,
  CustomButton,
  LightTooltip,
  LinkTab
} from "@components/muiComponents";
import { TableLayout } from "@components/Table/TableLayout";
import {
  Divider,
  Grid,
  List,
  ListItem,
  ListItemText,
  Stack,
  styled,
  Tabs,
  Tooltip,
  tooltipClasses,
  TooltipProps,
  Typography
} from "@mui/material";
import { Item } from "@utils/Muiutils";
import { customSortBy, isEmpty, serverError } from "@utils/Utils";
import moment from "moment";
import HelpOutlinedIcon from "@mui/icons-material/HelpOutlined";

const HtmlTooltip = styled(({ className, ...props }: TooltipProps) => (
  <Tooltip
    {...props}
    classes={{ popper: className }}
    arrow
    placement="bottom-start"
  />
))(({ theme }) => ({
  [`& .${tooltipClasses.tooltip}`]: {
    backgroundColor: "#f5f5f9",
    color: "rgba(0, 0, 0, 0.87)",
    maxWidth: 500,
    fontSize: theme.typography.pxToRem(12),
    border: "1px solid #dadde9"
  }
}));

const millisecondsToSeconds = (rawValue: any): any => {
  return parseFloat(((rawValue % 60000) / 1000) as any).toFixed(3);
};

const DebugMetrics = () => {
  const [debugMetricsData, setDebugMetricsData] = useState<any>({});
  const [updateTable, setUpdateTable] = useState(moment.now());
  const [loader, setLoader] = useState(false);
  const toastId = useRef(null);

  useEffect(() => {
    fetchDebugMetrics();
  }, []);

  const fetchDebugMetrics = async () => {
    setLoader(true);
    try {
      const debugMetricsResp = await getDebugMetrics();
      const { data = {} } = debugMetricsResp || {};
      setDebugMetricsData(data);
      setLoader(false);
    } catch (error) {
      setLoader(false);
      console.error(`Error occur while fetching debug metrics details`, error);
      serverError(error, toastId);
    }
  };

  const defaultColumns: any = useMemo(
    () => [
      {
        accessorKey: "name",
        cell: (info: any) => {
          let values = info.row.original;
          const { name } = values;
          return !isEmpty(name) ? (
            <Typography color="#686868">{name}</Typography>
          ) : (
            <span>N/A</span>
          );
        },
        header: "Name",
        enableSorting: true,
        show: true,
        width: "30%"
      },
      {
        accessorKey: "numops",
        cell: (info: any) => {
          let values = info.row.original;
          const { numops } = values;
          return !isEmpty(numops) ? (
            <Typography color="#686868">{numops}</Typography>
          ) : (
            <span>N/A</span>
          );
        },
        header: (
          <LightTooltip title="Numebr of times the API has been hit since Atlas started">
            Count
          </LightTooltip>
        ),
        enableSorting: true
      },
      {
        accessorKey: "minTime",
        cell: (info: any) => {
          let values = info.row.original;
          const { minTime } = values;
          return !isEmpty(minTime) ? (
            <Typography color="#686868">
              {millisecondsToSeconds(minTime)}
            </Typography>
          ) : (
            <span>N/A</span>
          );
        },
        header: "Min Time(secs)",
        enableSorting: true
      },
      {
        accessorKey: "maxTIme",
        cell: (info: any) => {
          let values = info.row.original;
          const { maxTime } = values;
          return !isEmpty(maxTime) ? (
            <Typography color="#686868">
              {millisecondsToSeconds(maxTime)}
            </Typography>
          ) : (
            <span>N/A</span>
          );
        },
        header: "Max Time(secs)",
        enableSorting: true
      },
      {
        accessorKey: "avgTime",
        cell: (info: any) => {
          let values = info.row.original;
          const { avgTime } = values;
          return !isEmpty(avgTime) ? (
            <Typography color="#686868">
              {millisecondsToSeconds(avgTime)}
            </Typography>
          ) : (
            <span>N/A</span>
          );
        },
        header: "Average Time(secs)",
        enableSorting: true
      }
    ],
    [updateTable]
  );

  return (
    <>
      <Item variant="outlined" className="administration-items">
        <Stack width="100%">
          <Tabs
            value={0}
            className="detail-page-tabs administration-tabs"
            data-cy="tab-list"
          >
            <LinkTab label="REST API Metrics" />
          </Tabs>
        </Stack>
        <Stack gap="0.5rem" padding="1rem">
          <Stack
            direction="row"
            justifyContent="flex-end"
            spacing={1}
            alignItems="right"
          >
            <HtmlTooltip
              arrow
              title={
                <>
                  <Typography className="advanced-search-title">
                    Debug Metrics
                  </Typography>
                  <Divider />
                  <br />
                  <Typography color="inherit">
                    Debug Metrics Information
                  </Typography>
                  <Grid container>
                    <Grid>
                      <List className="advanced-search-queries-list">
                        <ListItem className="advanced-search-queries-listitem">
                          <ListItemText
                            primary={"Count"}
                            secondary={
                              "Number of times the API has been hit since Atlas started"
                            }
                          />
                        </ListItem>
                        <ListItem className="advanced-search-queries-listitem">
                          <ListItemText
                            primary={"Min Time (secs)"}
                            secondary={
                              "Minimum API execution time since Atlas started"
                            }
                          />
                        </ListItem>
                        <ListItem className="advanced-search-queries-listitem">
                          <ListItemText
                            primary={"Max Time (secs)"}
                            secondary={
                              "Maximum API execution time since Atlas started"
                            }
                          />
                        </ListItem>
                        <ListItem className="advanced-search-queries-listitem">
                          <ListItemText
                            primary={"Average Time (secs)"}
                            secondary={
                              "Average time taken to execute by an API within an interval of time"
                            }
                          />
                        </ListItem>
                      </List>
                      <Divider />
                    </Grid>
                  </Grid>
                </>
              }
            >
              <HelpOutlinedIcon />
            </HtmlTooltip>
            <CustomButton
              variant="outlined"
              size="small"
              onClick={(e: any) => {
                e.stopPropagation();
                fetchDebugMetrics();
                setUpdateTable(moment.now());
              }}
              data-cy="refreshSearchResult"
            >
              <AutorenewIcon className="table-filter-refresh" />
            </CustomButton>
          </Stack>
          <TableLayout
            data={
              !isEmpty(debugMetricsData)
                ? customSortBy(Object.values(debugMetricsData || {}), ["name"])
                : []
            }
            columns={defaultColumns}
            emptyText="No Records found!"
            isFetching={loader}
            columnVisibility={false}
            clientSideSorting={true}
            columnSort={true}
            showPagination={true}
            showRowSelection={false}
            tableFilters={false}
          />
        </Stack>
      </Item>
    </>
  );
};

export default DebugMetrics;
