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

import { CustomButton, LightTooltip } from "@components/muiComponents";
import { Stack, Typography } from "@mui/material";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";
import { TableLayout } from "@components/Table/TableLayout";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { useMemo } from "react";
import { dateFormat, isEmpty, sanitizeHtmlContent } from "@utils/Utils";
import { Link, useLocation } from "react-router-dom";
import BusinessMetadataAtrribute from "@views/DetailPage/BusinessMetadataDetails/BusinessMetadataAtrribute";
import AddIcon from "@mui/icons-material/Add";
import { setEditBMAttribute } from "@redux/slice/createBMSlice";

const BusinessMetadataTab = ({ setForm, setBMAttribute }: any) => {
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const dispatchState = useAppDispatch();
  const { businessMetaData, loading }: any = useAppSelector(
    (state: any) => state.businessMetaData
  );
  const { businessMetadataDefs } = businessMetaData || {};

  const defaultColumns = useMemo(
    () => [
      {
        accessorKey: "name",
        cell: (info: any) => {
          let guid = info.row.original.guid;
          let keys = Array.from(searchParams.keys());
          for (let i = 0; i < keys.length; i++) {
            if (keys[i] != "searchType") {
              searchParams.delete(keys[i]);
            }
          }
          searchParams.set("from", "bm");

          return (
            <LightTooltip title={info.getValue()}>
              <Link
                className="text-blue text-decoration-none"
                to={{
                  pathname: `/administrator/businessMetadata/${guid}`,
                  search: `?${searchParams.toString()}`
                }}
                color={"primary"}
              >
                {info.getValue()}
              </Link>
            </LightTooltip>
          );
        },
        header: "Name",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "description",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <LightTooltip
              title={
                <Typography>
                  {" "}
                  <div
                    dangerouslySetInnerHTML={{
                      __html: sanitizeHtmlContent(info.getValue())
                    }}
                  />
                </Typography>
              }
            >
              <Typography>
                {" "}
                <div
                  dangerouslySetInnerHTML={{
                    __html: sanitizeHtmlContent(
                      info.getValue().length > 40
                        ? info.getValue().substr(0, 40) + "..."
                        : info.getValue()
                    )
                  }}
                />
              </Typography>
            </LightTooltip>
          ) : (
            <span>N/A</span>
          ),
        header: "Description",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "createdBy",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Created by",
        width: "10%",
        enableSorting: true,
        show: false
      },
      {
        accessorKey: "createTime",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{dateFormat(info.getValue())}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Created on",
        width: "10%",
        enableSorting: true,
        show: false
      },
      {
        accessorKey: "updatedBy",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Updated by",
        width: "10%",
        enableSorting: true,
        show: false
      },
      {
        accessorKey: "updateTime",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{dateFormat(info.getValue())}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Updated on",
        enableSorting: true,
        show: false
      },
      {
        accessorKey: "action",
        cell: (info) => {
          return (
            <LightTooltip title={"Attributes"}>
              <CustomButton
                variant="outlined"
                color="success"
                className="table-filter-btn bm-table-action"
                size="small"
                onClick={(_e: any) => {
                  setForm(true);
                  setBMAttribute(info.row.original);
                  dispatchState(setEditBMAttribute({}));
                }}
                startIcon={<AddOutlinedIcon />}
                data-cy="addAttribute"
              >
                Attributes
              </CustomButton>
            </LightTooltip>
          );
        },
        header: "Action",
        width: "5%",
        enableSorting: false,
        show: true
      }
    ],
    []
  );

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
    <Stack direction="column">
      <Stack padding={2} position="relative">
        <div style={{ height: "0" }}>
          <CustomButton
            variant="contained"
            sx={{
              position: "absolute",
              marginTop: "13px",
              marginLeft: "13px",
              left: "16px"
            }}
            size="small"
            onClick={(_e: any) => {
              setForm(true);
              setBMAttribute({});
              dispatchState(setEditBMAttribute({}));
            }}
            startIcon={<AddIcon fontSize="small" />}
            data-cy="createBusinessMetadata"
          >
            Create Business Metadata
          </CustomButton>
        </div>

        <TableLayout
          data={businessMetadataDefs || []}
          columns={defaultColumns}
          defaultColumnVisibility={defaultColumnVisibility(defaultColumns)}
          emptyText="No Records found!"
          isFetching={loading}
          columnVisibility={true}
          clientSideSorting={true}
          columnSort={true}
          showPagination={true}
          showRowSelection={false}
          tableFilters={true}
          expandRow={true}
          auditTableDetails={{
            Component: BusinessMetadataAtrribute,
            componentProps: {
              attributeDefs: businessMetadataDefs,
              loading: loading,
              setForm: setForm,
              setBMAttribute: setBMAttribute
            }
          }}
        />
      </Stack>
    </Stack>
  );
};

export default BusinessMetadataTab;
