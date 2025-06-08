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

import { useCallback, useMemo, useReducer, useRef, useState } from "react";
import {
  FormControlLabel,
  FormGroup,
  Grid,
  IconButton,
  Stack,
  Typography
} from "@mui/material";
import { TableLayout } from "@components/Table/TableLayout";
import {
  Action,
  EntityDetailTabProps,
  ProfileTableType,
  State
} from "@models/entityDetailType";
import {
  dateFormat,
  extractKeyValueFromEntity,
  isEmpty,
  serverError
} from "@utils/Utils";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { LightTooltip } from "@components/muiComponents";
import { entityStateReadOnly, serviceTypeMap } from "@utils/Enum";
import { getRelationShip } from "@api/apiMethods/searchApiMethod";
import { toast } from "react-toastify";
import DisplayImage from "@components/EntityDisplayImage";
import { useSelector } from "react-redux";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import { ColumnDef } from "@tanstack/react-table";
import { EntityState } from "@models/relationshipSearchType";
import { AntSwitch } from "@utils/Muiutils";

const initialState = {
  isLoading: false,
  respData: []
};
function reducer(_state: State, action: Action): State {
  switch (action.type) {
    case "request":
      return { isLoading: true };
    case "success":
      return { isLoading: false, respData: action.respData };
    case "failure":
      return { isLoading: false, error: action.error };
  }
}

const ProfileTab: React.FC<EntityDetailTabProps> = ({ entity }) => {
  const { guid } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const [{ respData, isLoading }, dispatch] = useReducer(reducer, initialState);
  const [checkedEntities, setCheckedEntities] = useState<any>(
    !isEmpty(searchParams.get("includeDE"))
      ? searchParams.get("includeDE")
      : false
  );
  const { entityData } = useSelector((state: EntityState) => state.entity);
  const toastId: any = useRef(null);

  const fetchRelationShipResult = useCallback(
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
        order: sorting[0]?.desc == false ? "asc" : "desc",
        offset:
          searchParams.get("pageOffset") !== undefined &&
          searchParams.get("pageOffset") !== null
            ? Number(searchParams.get("pageOffset"))
            : pageIndex * pageSize,
        limit: pageSize,
        sort_by: sorting[0]?.id || "timestamp",
        guid: guid,
        relation:
          entity?.typeName === "hive_db"
            ? "__hive_table.db"
            : "__hbase_table.namespace",
        sortBy: sorting[0]?.id || "name",
        sortOrder: sorting[0]?.desc == false ? "ASCENDING" : "DESCENDING",
        excludeDeletedEntities: !isEmpty(searchParams.get("includeDE"))
          ? !searchParams.get("includeDE")
          : true,
        includeSubClassifications: true,
        includeSubTypes: true,
        includeClassificationAttributes: true
      };
      dispatch({ type: "request" });

      try {
        let searchResp = await getRelationShip({ params: params });
        dispatch({
          type: "success",
          respData: searchResp.data.entities
        });
      } catch (error: any) {
        console.error("Error fetching data:", error.response.data.errorMessage);
        toast.dismiss(toastId.current);
        serverError(error, toastId);
        dispatch({ type: "failure", error });
      }
    },
    [searchParams]
  );

  const handleSwitchChangeEntities = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    event.stopPropagation();

    setCheckedEntities(event.target.checked);
    if (event.target.checked) {
      searchParams.set("includeDE", event.target.checked ? "true" : "false");
      setSearchParams(searchParams);
    } else {
      searchParams.delete("includeDE");
      setSearchParams(searchParams);
    }
  };

  const defaultColumns = useMemo<ColumnDef<ProfileTableType>[]>(
    () => [
      {
        accessorFn: (row: any) => row.attributes.name,
        accessorKey: "name",
        cell: (info: any) => {
          let entityDef: any = info.row.original;
          const { name }: { name: string; found: boolean; key: any } =
            extractKeyValueFromEntity(entityDef);

          const getName = (entity: { guid: string }) => {
            const href = `/detailPage/${entity.guid}`;

            return (
              <LightTooltip title={name}>
                {entity.guid != "-1" ? (
                  <Link
                    className={`entity-name text-decoration-none ${
                      entityDef.status && entityStateReadOnly[entityDef.status]
                        ? "text-red"
                        : "text-blue"
                    }`}
                    style={{ width: "unset !important", whiteSpace: "nowrap" }}
                    to={{
                      pathname: href
                    }}
                  >
                    {name}
                  </Link>
                ) : (
                  <span>{name} </span>
                )}
              </LightTooltip>
            );
          };
          if (
            entityDef.attributes &&
            entityDef.attributes.serviceType !== undefined
          ) {
            if (
              serviceTypeMap[entityDef.typeName] === undefined &&
              entityData.entityDefs
            ) {
              let defObj = entityData.entityDefs.find(
                (obj: { typeName: string }) => ({
                  name: obj.typeName
                })
              );
              if (defObj) {
                serviceTypeMap[entityDef.typeName] = defObj.get("serviceType");
              }
            }
          } else if (serviceTypeMap[entityDef.typeName] === undefined) {
            serviceTypeMap[entityDef.typeName] = entityDef.attributes
              ? entityDef.attributes.serviceType
              : null;
          }
          entityDef.serviceType = serviceTypeMap[entityDef.typeName];
          return (
            <div className="searchTableName">
              <DisplayImage entity={entityDef} />
              {getName(entityDef)}
              {entityDef.status && entityStateReadOnly[entityDef.status] && (
                <LightTooltip title="Deleted">
                  <IconButton
                    aria-label="back"
                    sx={{
                      display: "inline-flex",
                      position: "relative",
                      padding: "4px",
                      marginLeft: "4px",
                      color: (theme) => theme.palette.grey[500]
                    }}
                  >
                    <DeleteOutlineOutlinedIcon sx={{ fontSize: "1.25rem" }} />
                  </IconButton>
                </LightTooltip>
              )}
            </div>
          );
        },
        header: "Table Name",
        enableSorting: true,
        width: "30%"
      },
      {
        accessorFn: (row: any) => row.attributes.owner,
        accessorKey: "owner",
        cell: (info: any) =>
          !isEmpty(info.row.original.attributes.owner) ? (
            <Typography>{info.row.original.attributes.owner}</Typography>
          ) : (
            <span>N/A</span>
          ),
        enableSorting: true,
        header: "Owner",
        width: "20%"
      },
      {
        accessorFn: (row: any) => row.attributes.createTime,
        accessorKey: "createTime",
        cell: (info: any) =>
          !isEmpty(info.row.original.attributes.createTime) ? (
            <Typography>
              {dateFormat(info.row.original.attributes.createTime)}
            </Typography>
          ) : (
            <span>N/A</span>
          ),
        enableSorting: true,
        header: "Date Created",
        width: "30%"
      }
    ],
    []
  );

  const getDefaultSort = useMemo(() => [{ id: "name", desc: false }], []);

  return (
    <>
      <Grid container marginTop={0} className="properties-container">
        <Grid item md={12} p={2}>
          <Stack position="relative" className="profile-tab-container">
            <Stack
              direction="row"
              justifyContent="right"
              alignItems="center"
              marginBottom="0.75rem"
              position="absolute"
              right={0}
            >
              <FormGroup>
                <FormControlLabel
                  control={
                    <AntSwitch
                      sx={{ marginRight: "4px" }}
                      size="small"
                      checked={checkedEntities}
                      onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                        handleSwitchChangeEntities(e);
                      }}
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                      inputProps={{ "aria-label": "controlled" }}
                    />
                  }
                  label="Show historical entities"
                />
              </FormGroup>
            </Stack>

            <TableLayout
              fetchData={fetchRelationShipResult}
              data={respData || []}
              columns={defaultColumns || []}
              emptyText="No Records found!"
              isFetching={isLoading}
              defaultSortCol={getDefaultSort}
              clientSideSorting={true}
              columnSort={true}
              columnVisibility={false}
              showRowSelection={true}
              showPagination={true}
              tableFilters={false}
              assignFilters={{ classifications: true, term: true }}
            />
          </Stack>
        </Grid>
      </Grid>
    </>
  );
};

export default ProfileTab;
