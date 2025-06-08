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

import { useCallback, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { getRelationShipResult } from "@api/apiMethods/searchApiMethod";
import { useSearchParams } from "react-router-dom";
import CircularProgress from "@mui/material/CircularProgress";
import IconButton from "@mui/material/IconButton";
import Stack from "@mui/material/Stack";
import { useSelector } from "react-redux";
import {
  findUniqueValues,
  extractKeyValueFromEntity,
  isEmpty,
  removeDuplicateObjects,
  serverError,
  Capitalize
} from "@utils/Utils";
import { toast } from "react-toastify";
import { TableLayout } from "@components/Table/TableLayout";
import moment from "moment-timezone";
import { LightTooltip } from "@components/muiComponents";
import { _get } from "@api/apiMethods/apiMethod";
import { entityStateReadOnly, serviceTypeMap } from "@utils/Enum";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import DisplayImage from "@components/EntityDisplayImage";
import { EntityState, Params } from "@models/relationshipSearchType";

let defaultColumnsName: Array<string> = [
  "guid",
  "typeName",
  "end1",
  "end2",
  "label"
];

const RelationShipSearch: React.FC = () => {
  const [searchParams] = useSearchParams();
  const [searchData, setSearchData] = useState<any>([]);
  const [loader, setLoader] = useState(true);
  const toastId: any = useRef(null);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const { entityData, loading } = useSelector(
    (state: EntityState) => state.entity
  );
  const [pageCount, setPageCount] = useState(0);
  let searchParamsObj = {
    attributesParams: searchParams.get("attributes"),
    limitParams: searchParams.get("pageLimit"),
    offsetParams: searchParams.get("pageOffset"),
    relationshipFiltersParams: searchParams.get("relationshipFilters"),
    relationshipNameParams: searchParams.get("relationshipName")
  };

  const {
    attributesParams,
    limitParams,
    offsetParams,
    relationshipFiltersParams,
    relationshipNameParams
  } = searchParamsObj;

  const fetchRelationshipSearchResult = useCallback(
    async ({ pagination }: { pagination?: any }) => {
      const { pageSize, pageIndex } = pagination || {};
      if (pageIndex > 1) {
        searchParams.set("pageOffset", `${pageSize * pageIndex}`);
      }
      let params: Params = {
        attributes: !isEmpty(attributesParams)
          ? findUniqueValues(
              attributesParams?.split(",") || [],
              defaultColumnsName
            )
          : [],
        limit: !isEmpty(limitParams) ? Number(limitParams) : pageSize,
        offset: !isEmpty(offsetParams)
          ? Number(searchParams.get("pageOffset"))
          : pageIndex * pageSize,
        relationshipFilters: !isEmpty(relationshipFiltersParams)
          ? relationshipFiltersParams
          : null,
        relationshipName: relationshipNameParams
      };
      try {
        setLoader(true);
        let searchResp = await getRelationShipResult({
          data: params
        });
        let totalCount = searchResp.data.approximateCount;
        setSearchData(searchResp.data);
        setPageCount(Math.ceil(totalCount / pagination.pageSize));
        setLoader(false);
      } catch (error: any) {
        console.error("Error fetching data:", error.response.data.errorMessage);
        toast.dismiss(toastId.current);
        serverError(error, toastId);
        setLoader(false);
      }
    },
    [updateTable, searchParams]
  );

  const refreshTable = () => {
    setUpdateTable(moment.now());
  };

  const defaultColumns = [
    {
      accessorFn: (row: any) => row.attributes.name,
      accessorKey: "guid",
      id: "guid",
      cell: (info: any) => {
        let entityDef: any = info.row.original;
        const { name }: { name: string; found: boolean; key: any } =
          extractKeyValueFromEntity(entityDef);

        const getName = (entity: { guid: string }) => {
          const href = `/relationshipDetailPage/${entity.guid}`;

          return (
            <LightTooltip title={name}>
              {entity.guid != "-1" ? (
                <Link
                  className="entity-name text-blue text-decoration-none"
                  to={{
                    pathname: href
                  }}
                  style={{ width: "unset !important", whiteSpace: "nowrap" }}
                  color={
                    entityDef.status && entityStateReadOnly[entityDef.status]
                      ? "error"
                      : "primary"
                  }
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
            var defObj = entityData.entityDefs.find(
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
      header: "Guid",
      enableSorting: false,
      show: true
    },
    ,
    {
      accessorFn: (row: any) => row.typeName,
      accessorKey: "typeName",
      id: "typeName",
      cell: (info: any) => <span>{info.getValue()}</span>,
      header: "Type",
      show: true
    },
    {
      accessorFn: (row: any) => row.end1.uniqueAttributes.qualifiedName,
      accessorKey: "end1",
      id: "end1",
      cell: (info: any) => {
        const end1 = info.row.original.end1;
        const { guid } = end1;
        return (
          <LightTooltip title={`Search ${info.getValue()}`}>
            <Link
              className="text-blue text-decoration-none"
              to={{
                pathname: `/detailPage/${guid}`,
                search: `?from=relationshipSearch`
              }}
            >
              {info.getValue()}
            </Link>
          </LightTooltip>
        );
      },
      header: "End1",
      show: true,
      enableSorting: false
    },
    {
      accessorFn: (row: any) => row.end2.uniqueAttributes.qualifiedName,
      accessorKey: "end2",
      id: "end2",
      cell: (info: any) => {
        const end2 = info.row.original.end2;
        const { guid } = end2;

        return (
          <LightTooltip title={`Search ${info.getValue()}`}>
            <Link
              className="text-blue text-decoration-none"
              to={{
                pathname: `/detailPage/${guid}`,
                search: `?from=relationshipSearch`
              }}
            >
              {info.getValue()}
            </Link>
          </LightTooltip>
        );
      },
      header: "End2",
      show: true,
      enableSorting: false
    },
    {
      accessorFn: (row: any) => row.label,
      accessorKey: "label",
      id: "label",
      cell: (info: any) => <span>{info.getValue()}</span>,
      header: "Label",
      show: true
    }
  ];
  let currentRelationshipData = searchData?.relations?.find(
    (obj: { typeName: string }) =>
      obj.typeName == searchParams.get("relationshipName")
  );

  let defaultHideColumnsName = Object.keys(
    currentRelationshipData?.attributes || {}
  )?.map((obj: string) => {
    return { accessorKey: obj, header: Capitalize(obj) };
  });

  let defaultHideColumns = defaultHideColumnsName?.map(
    (relationship: any, index: any) => {
      return {
        accessorFn: (row: any) => row.attributes,
        accessorKey: relationship.accessorKey,
        header: relationship.header,
        id: relationship.accessorKey,
        cell: (user: any) => {
          let rowData = user.row.original.attributes[relationship.accessorKey];

          return !isEmpty(rowData) ? (
            <span key={index}>{rowData}</span>
          ) : (
            <span>NA</span>
          );
        },
        show: false
      };
    }
  );

  let allColumns = removeDuplicateObjects([
    ...defaultColumns,
    ...defaultHideColumns
  ]);

  const defaultColumnVisibility: any = (columns: any) => {
    let columnsParams: any = searchParams.get("attributes");
    let hideColumns: any = {};

    for (let col of columns) {
      if (
        !isEmpty(columnsParams) &&
        columnsParams.split(",").includes(col.accessorKey)
      ) {
        hideColumns[col.accessorKey] = true;
      } else if (col.show == false) {
        hideColumns[col.accessorKey] = false;
      } else if (
        !isEmpty(columnsParams) &&
        columnsParams.split(",").includes(col.accessorKey) == false
      ) {
        hideColumns[col.accessorKey] = false;
      }
    }

    return hideColumns;
  };

  return loading ? (
    <CircularProgress />
  ) : (
    <Stack>
      <TableLayout
        fetchData={fetchRelationshipSearchResult}
        data={searchData.relations || []}
        columns={allColumns.filter((value) => Object.keys(value).length !== 0)}
        emptyText="No Records found!"
        isFetching={loader}
        pageCount={pageCount}
        columnVisibilityParams={true}
        defaultColumnVisibility={defaultColumnVisibility(allColumns)}
        defaultColumnParams={defaultColumnsName.join(",")}
        columnVisibility={true}
        refreshTable={refreshTable}
        defaultSortCol={[]}
        clientSideSorting={true}
        columnSort={true}
        showPagination={true}
        showRowSelection={false}
        tableFilters={true}
        queryBuilder={true}
        allTableFilters={true}
        setUpdateTable={setUpdateTable}
      />
    </Stack>
  );
};

export default RelationShipSearch;
