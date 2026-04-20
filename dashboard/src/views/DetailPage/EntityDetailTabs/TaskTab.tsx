/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ColumnDef } from "@tanstack/react-table";
import { Grid, IconButton, Stack, Typography, Collapse } from "@mui/material";
import RefreshIcon from "@mui/icons-material/Refresh";
import { TableLayout } from "@components/Table/TableLayout";
import { getPendingTasks } from "@api/apiMethods/adminTasksApiMethod";
import { serverError, dateFormat } from "@utils/Utils";
import moment from "moment-timezone";
import { auditAction } from "@utils/Enum";
import { isEmpty } from "@utils/Utils";

const formatTaskDate = (v: unknown): string => {
  if (v == null || v === "") {
    return "N/A";
  }
  const t =
    typeof v === "number"
      ? v
      : moment(v as string | Date).valueOf();
  if (!Number.isFinite(t) || t <= 0) {
    return "N/A";
  }
  return dateFormat(t);
};

export interface AtlasTaskRow {
  guid?: string;
  type?: string;
  status?: string;
  createdTime?: string | number | Date;
  updatedTime?: string | number | Date;
  parameters?: Record<string, unknown>;
  attemptCount?: number;
  createdBy?: string;
}

const TaskTab = () => {
  const toastId = useRef<any>(null);
  const [tasks, setTasks] = useState<AtlasTaskRow[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [expandedByGuid, setExpandedByGuid] = useState<Record<string, boolean>>(
    {}
  );

  const loadTasks = useCallback(async () => {
    setLoading(true);
    try {
      const resp = await getPendingTasks({});
      const data = resp?.data;
      const list = Array.isArray(data) ? data : [];
      setTasks(list);
    } catch (e: any) {
      console.error(e);
      serverError(e, toastId);
      setTasks([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadTasks();
  }, [loadTasks]);

  const columns = useMemo<ColumnDef<AtlasTaskRow>[]>(
    () => [
      {
        id: "expand",
        header: "",
        cell: ({ row }) => {
          const guid = String(row.original.guid ?? row.id);
          const open = !!expandedByGuid[guid];
          const params = row.original.parameters || {};
          const displayParams: Record<string, unknown> = {
            ...params,
            attemptCount: row.original.attemptCount,
            createdBy: row.original.createdBy
          };
          delete displayParams.entityGuid;
          return (
            <>
              <IconButton
                size="small"
                aria-expanded={open}
                aria-label="toggle task parameters"
                onClick={() => {
                  setExpandedByGuid((prev) => ({
                    ...prev,
                    [guid]: !prev[guid]
                  }));
                }}
              >
                {open ? "−" : "+"}
              </IconButton>
              <Collapse in={open} timeout="auto" unmountOnExit>
                <Typography
                  variant="caption"
                  component="div"
                  sx={{ p: 1, bgcolor: "action.hover" }}
                >
                  <strong>Parameters</strong>
                  <pre
                    style={{
                      margin: 0,
                      whiteSpace: "pre-wrap",
                      fontSize: 12
                    }}
                  >
                    {JSON.stringify(displayParams, null, 2)}
                  </pre>
                </Typography>
              </Collapse>
            </>
          );
        },
        enableSorting: false
      },
      {
        accessorKey: "type",
        header: "Type",
        cell: (info) => {
          const v = info.getValue() as string;
          return (
            <Typography>{!isEmpty(v) ? auditAction[v] || v : "N/A"}</Typography>
          );
        },
        enableSorting: false
      },
      {
        accessorKey: "guid",
        header: "Guid",
        cell: (info) => (
          <Typography sx={{ wordBreak: "break-all" }}>
            {!isEmpty(info.getValue()) ? String(info.getValue()) : "N/A"}
          </Typography>
        ),
        enableSorting: false
      },
      {
        accessorKey: "status",
        header: "Status",
        enableSorting: false
      },
      {
        accessorKey: "createdTime",
        header: "Created Time",
        cell: (info) => {
          const v = info.getValue();
          return <Typography>{formatTaskDate(v)}</Typography>;
        },
        enableSorting: false
      },
      {
        accessorKey: "updatedTime",
        header: "Updated Time",
        cell: (info) => {
          const v = info.getValue();
          return <Typography>{formatTaskDate(v)}</Typography>;
        },
        enableSorting: false
      }
    ],
    [expandedByGuid]
  );

  return (
    <Grid container marginTop={0} className="properties-container">
      <Grid item md={12} p={2}>
        <Stack direction="row" justifyContent="flex-end" sx={{ mb: 1 }}>
          <IconButton
            aria-label="refresh tasks"
            onClick={() => void loadTasks()}
            size="small"
          >
            <RefreshIcon />
          </IconButton>
        </Stack>
        <TableLayout
          data={tasks}
          columns={columns}
          emptyText="No records found!"
          isFetching={loading}
          columnVisibility={false}
          clientSideSorting={true}
          columnSort={false}
          showPagination={true}
          isClientSidePagination={true}
          showRowSelection={false}
          tableFilters={false}
        />
      </Grid>
    </Grid>
  );
};

export default TaskTab;
