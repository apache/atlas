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

import { Grid, Stack, Typography } from "@mui/material";
import { TableLayout } from "@components/Table/TableLayout";
import { useParams, useSearchParams } from "react-router-dom";
import { useCallback, useMemo, useRef, useState } from "react";
import { getDetailPageRauditData } from "@api/apiMethods/detailpageApiMethod";
import {
  dateFormat,
  extractKeyValueFromEntity,
  isEmpty,
  serverError
} from "@utils/Utils";
import { toast } from "react-toastify";
import RauditsTableResults from "./RauditsTableResults";

const ReplicationAuditTable = (props: any) => {
  const { entity = {}, referredEntities = {}, loading } = props;
  const [searchParams] = useSearchParams();
  const { guid } = useParams();
  const toastId: any = useRef(null);
  const [loader, setLoader] = useState<boolean>(false);
  const [rauditData, setRauditData] = useState([]);
  let { name } = extractKeyValueFromEntity(entity);

  const fetchAuditResult = useCallback(
    async ({
      pagination
    }: {
      pagination?: any;
      sorting: [{ id: string; desc: boolean }];
    }) => {
      const { pageSize, pageIndex } = pagination || {};
      if (pageIndex > 1) {
        searchParams.set("pageOffset", `${pageSize * pageIndex}`);
      }

      let params: any = {
        serverName: name,
        limit: pageSize
      };

      setLoader(true);
      try {
        let searchResp = await getDetailPageRauditData(params);
        setRauditData(searchResp.data);
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

  const defaultColumns = useMemo(
    () => [
      {
        accessorKey: "operation",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Operations",
        width: "20%",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "sourceServerName",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Source Server",
        width: "10%",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "targetServerName",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Target Server",
        width: "10%",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "startTime",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{dateFormat(info.getValue())}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Operation StartTime",
        enableSorting: true,
        width: "30%"
      },
      {
        accessorKey: "endTime",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{dateFormat(info.getValue())}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Operation EndTime",
        width: "30%",
        enableSorting: true
      }
    ],
    []
  );

  return (
    <Grid container marginTop={0}>
      <Grid item md={12} p={2}>
        <Stack>
          {
            <TableLayout
              fetchData={fetchAuditResult}
              data={rauditData}
              columns={defaultColumns}
              emptyText="No Records found!"
              isFetching={loader}
              columnVisibility={false}
              columnSort={false}
              showPagination={true}
              showRowSelection={false}
              tableFilters={false}
              expandRow={true}
              auditTableDetails={{
                Component: RauditsTableResults,
                componentProps: {
                  entity: entity,
                  referredEntities: referredEntities,
                  loading: loading
                }
              }}
            />
          }
        </Stack>
      </Grid>
    </Grid>
  );
};

export default ReplicationAuditTable;
