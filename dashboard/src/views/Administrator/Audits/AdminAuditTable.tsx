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

import { useSearchParams } from "react-router-dom";
import { getAuditData } from "@api/apiMethods/detailpageApiMethod";
import { useCallback, useMemo, useRef, useState } from "react";
import { toast } from "react-toastify";
import {
  isEmpty,
  isNumber,
  millisecondsToTime,
  serverError
} from "@utils/Utils";
import { TableLayout } from "@components/Table/TableLayout";
import { dateFormat } from "@utils/Utils";
import { Grid, Stack, Typography } from "@mui/material";
import { ColumnDef } from "@tanstack/react-table";
import { AuditTableType } from "@models/entityDetailType";
import moment from "moment";
import AuditResults from "./AuditResults";
import { CustomButton } from "@components/muiComponents";
import AuditFilters from "./AuditsFilter/AuditFilters";
import KeyboardArrowRightOutlinedIcon from "@mui/icons-material/KeyboardArrowRightOutlined";
import KeyboardArrowDownOutlinedIcon from "@mui/icons-material/KeyboardArrowDownOutlined";

const AdminAuditTable = () => {
  const [searchParams] = useSearchParams();
  const toastId: any = useRef(null);
  const [loader, setLoader] = useState<boolean>(true);
  const [auditData, setAuditData] = useState([]);
  const [updateTable, setupdateTable] = useState(moment.now());
  const [queryApiObj, setQueryApiObj] = useState({});
  const [filtersPopover, setFiltersPopover] =
    useState<HTMLButtonElement | null>(null);
  const filtersOpen = Boolean(filtersPopover);
  const popoverId = filtersOpen ? "simple-popover" : undefined;

  const fetchAuditResult = useCallback(
    async ({ pagination }: { pagination?: any }) => {
      const { pageSize, pageIndex } = pagination || {};
      if (pageIndex > 1) {
        searchParams.set("pageOffset", `${pageSize * pageIndex}`);
      }
      let params: any = {
        auditFilters: !isEmpty(queryApiObj) ? queryApiObj : null,
        limit: pageSize,
        sortOrder: "DESCENDING",
        offset: pageIndex * pageSize,
        sortBy: "startTime"
      };

      try {
        setLoader(true);
        let searchResp = await getAuditData(params);
        setAuditData(searchResp.data);
        setLoader(false);
      } catch (error: any) {
        console.error("Error fetching data:", error.response.data.errorMessage);
        toast.dismiss(toastId.current);
        serverError(error, toastId);
        setLoader(false);
      }
    },
    [updateTable]
  );

  const defaultColumns = useMemo<ColumnDef<AuditTableType>[]>(
    () => [
      {
        accessorKey: "userName",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Users",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "operation",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Operations",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "clientId",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography noWrap title={info.getValue()}>
              {info.getValue()}
            </Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Client ID",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "resultCount",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography noWrap>{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Result Count",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "startTime",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography noWrap>{dateFormat(info.getValue())}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Start Time",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "endTime",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography noWrap>{dateFormat(info.getValue())}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "End Time",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "duration",
        cell: (info: any) => {
          const { startTime, endTime } = info.row.original;
          if (isNumber(parseInt(startTime)) && isNumber(parseInt(endTime))) {
            let duration = moment.duration(
              moment(endTime).diff(moment(startTime))
            );

            return millisecondsToTime(duration);
          } else {
            return "N/A";
          }
        },
        header: "Duration",
        enableSorting: true,
        show: false
      }
    ],
    []
  );

  const handleClickFilterPopover = (
    event: React.MouseEvent<HTMLButtonElement>
  ) => {
    setFiltersPopover(event.currentTarget);
  };

  const handleCloseFilterPopover = () => {
    setFiltersPopover(null);
  };

  const defaultColumnVisibility: any = (columns: any) => {
    let hideColumns: any = {};

    for (let col of columns) {
      if (col.show == false) {
        hideColumns[col.accessorKey] = false;
      }
    }

    return hideColumns;
  };

  return (
    <>
      <Grid container marginTop={0}>
        <Grid item md={12} p={2}>
          <Stack alignItems="flex-start">
            <div
              style={{
                height: !isEmpty(auditData) ? 0 : "32px"
              }}
            >
              {!loader && (
                <CustomButton
                  variant="outlined"
                  size="small"
                  onClick={handleClickFilterPopover}
                  startIcon={
                    !filtersPopover ? (
                      <KeyboardArrowRightOutlinedIcon />
                    ) : (
                      <KeyboardArrowDownOutlinedIcon />
                    )
                  }
                  sx={{
                    zIndex: "99999",
                    marginTop: "13px !important",
                    marginLeft: "13px !important"
                  }}
                >
                  Filters
                </CustomButton>
              )}
            </div>
          </Stack>
          <Stack>
            <TableLayout
              fetchData={fetchAuditResult}
              data={auditData || []}
              columns={defaultColumns}
              defaultColumnVisibility={defaultColumnVisibility(defaultColumns)}
              emptyText="No Records found!"
              isFetching={loader}
              columnVisibility={true}
              clientSideSorting={true}
              columnSort={true}
              showPagination={true}
              showRowSelection={false}
              tableFilters={true}
              expandRow={true}
              auditTableDetails={{
                Component: AuditResults,
                componentProps: {
                  auditData: auditData
                }
              }}
              queryBuilder={false}
            />
          </Stack>
        </Grid>
      </Grid>
      {filtersOpen && (
        <AuditFilters
          popoverId={popoverId}
          filtersOpen={filtersOpen}
          filtersPopover={filtersPopover}
          handleCloseFilterPopover={handleCloseFilterPopover}
          setupdateTable={setupdateTable}
          queryApiObj={queryApiObj}
          setQueryApiObj={setQueryApiObj}
        />
      )}
    </>
  );
};

export default AdminAuditTable;
