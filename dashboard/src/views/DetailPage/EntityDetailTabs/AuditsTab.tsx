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

import { useParams, useSearchParams } from "react-router-dom";
import { getDetailPageAuditData } from "@api/apiMethods/detailpageApiMethod";
import { useCallback, useMemo, useRef, useState } from "react";
import { toast } from "react-toastify";
import { isEmpty, serverError } from "@utils/Utils";
import { TableLayout } from "@components/Table/TableLayout";
import { dateFormat } from "@utils/Utils";
import { Grid, Stack, Typography } from "@mui/material";
import { auditAction } from "@utils/Enum";
import { ColumnDef } from "@tanstack/react-table";
import { AuditTableType } from "@models/entityDetailType";
import AuditTableDetails from "./AuditTableDetails";

const AuditsTab = ({
  entity,
  referredEntities,
  loading,
  auditResultGuid
}: any) => {
  const [searchParams] = useSearchParams();
  const { guid } = useParams();
  const toastId: any = useRef(null);
  const [loader, setLoader] = useState<boolean>(true);
  const [auditData, setAuditData] = useState([]);

  const fetchAuditResult = useCallback(
    async ({
      pagination,
      sorting
    }: {
      pagination?: any;
      sorting: [{ id: string; desc: boolean }];
    }) => {
      const { pageSize, pageIndex } = pagination || {};
      if (pageIndex > 1) {
        searchParams.set("pageOffset", `${pageSize * pageIndex}`);
      }
      let params: any = {
        sortOrder: sorting[0]?.desc == false ? "asc" : "desc",
        offset:
          searchParams.get("pageOffset") != null
            ? Number(searchParams.get("pageOffset"))
            : pageIndex * pageSize,
        count: pageSize,
        sortBy: sorting[0]?.id || "timestamp"
      };

      setLoader(true);
      try {
        let searchResp = await getDetailPageAuditData(
          guid || auditResultGuid,
          params
        );
        setAuditData(searchResp.data);
        setLoader(false);
      } catch (error: any) {
        console.error("Error fetching data:", error.response.data.errorMessage);
        toast.dismiss(toastId.current);
        serverError(error, toastId);
        setLoader(false);
      }
    },
    [guid]
  );

  const defaultColumns = useMemo<ColumnDef<AuditTableType>[]>(
    () => [
      {
        accessorKey: "user",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography color="#686868">{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Users",
        width: "20%",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "timestamp",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography color="#686868">
              {dateFormat(info.getValue())}
            </Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Timestamp",
        enableSorting: true,
        width: "30%"
      },
      {
        accessorKey: "action",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography color="#686868">
              {auditAction[info.getValue()]}
            </Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Action",
        width: "40%",
        enableSorting: true
      }
    ],
    []
  );
  const getDefaultSort = useMemo(() => [{ id: "timestamp", desc: true }], []);
  return (
    <Grid container marginTop={0}>
      <Grid item md={12} p={2}>
        <Stack>
          <TableLayout
            fetchData={fetchAuditResult}
            data={auditData}
            columns={defaultColumns}
            emptyText="No Records found!"
            isFetching={loader}
            columnVisibility={false}
            clientSideSorting={false}
            defaultSortCol={getDefaultSort}
            columnSort={true}
            showPagination={true}
            showRowSelection={false}
            tableFilters={false}
            expandRow={true}
            auditTableDetails={{
              Component: AuditTableDetails,
              componentProps: {
                entity: entity,
                referredEntities: referredEntities,
                loading: loading
              }
            }}
          />
        </Stack>
      </Grid>
    </Grid>
  );
};

export default AuditsTab;
