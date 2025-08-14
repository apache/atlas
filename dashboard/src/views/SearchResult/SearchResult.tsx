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

import { useCallback, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { getBasicSearchResult } from "../../api/apiMethods/searchApiMethod";
import { useSearchParams } from "react-router-dom";
import { FormControlLabel, FormGroup, IconButton, Stack } from "@mui/material";
import { useSelector } from "react-redux";
import {
  customSortBy,
  findUniqueValues,
  extractKeyValueFromEntity,
  isArray,
  isEmpty,
  isNull,
  removeDuplicateObjects,
  dateFormat,
  flattenArray,
  serverError,
  searchParamsAPiQuery,
  globalSearchParams
} from "@utils/Utils";
import { toast } from "react-toastify";
import { TableLayout } from "@components/Table/TableLayout";
import moment from "moment-timezone";
import { LightTooltip } from "@components/muiComponents";
import { _get } from "@api/apiMethods/apiMethod";
import { getValues } from "@components/commonComponents";
import DialogShowMoreLess from "@components/DialogShowMoreLess";
import { entityStateReadOnly, serviceTypeMap } from "@utils/Enum";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import { startsWith } from "@utils/Helper";
import { removeClassification } from "@api/apiMethods/classificationApiMethod";
import { removeTerm } from "@api/apiMethods/glossaryApiMethod";
import { TextShowMoreLess } from "@components/TextShowMoreLess";
import DisplayImage from "@components/EntityDisplayImage";
import { AntSwitch } from "@utils/Muiutils";

interface EntityState {
  entity: {
    loading: boolean;
    entityData: any;
  };
}
interface Params {
  excludeDeletedEntities: boolean;
  includeSubClassifications: boolean;
  includeSubTypes: boolean;
  includeClassificationAttributes: boolean;
  entityFilters?: any;
  tagFilters?: any;
  attributes?: string[];
  limit: number;
  offset: number;
  typeName: string | null;
  classification: any;
  termName: string | null;
  relationshipFilters?: string | null;
}

let defaultColumnsName: Array<string> = [
  "select",
  "name",
  "owner",
  "description",
  "typeName",
  "classificationNames",
  "term"
];

const SearchResult = ({ classificationParams, glossaryTypeParams }: any) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchData, setSearchData] = useState<any>([]);
  const [loader, setLoader] = useState(true);
  const toastId: any = useRef(null);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const { entityData } = useSelector((state: EntityState) => state.entity);
  const [pageCount, setPageCount] = useState<number>(0);
  const [checkedEntities, setCheckedEntities] = useState<any>(
    !isEmpty(searchParams.get("includeDE"))
      ? searchParams.get("includeDE")
      : false
  );
  const [checkedSubClassifications, setCheckedSubClassifications] =
    useState<any>(
      !isEmpty(searchParams.get("excludeSC"))
        ? searchParams.get("excludeSC")
        : false
    );
  const entityFilterParams = searchParams.get("entityFilters");
  const tagFilterParams = searchParams.get("tagFilters");
  const relatonshipParams = searchParams.get("relationshipFilters");

  const handleSwitchChangeSubClassification = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    event.stopPropagation();

    setCheckedSubClassifications(event.target.checked);
    if (event.target.checked) {
      searchParams.set("excludeSC", event.target.checked ? "true" : "false");
      setSearchParams(searchParams);
    } else {
      searchParams.delete("excludeSC");
      setSearchParams(searchParams);
    }
  };

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

  const fetchSearchResult = useCallback(
    async ({ pagination }: { pagination?: any }) => {
      setLoader(true);
      const { pageSize, pageIndex } = pagination || {};
      if (pageIndex > 1) {
        searchParams.set("pageOffset", `${pageSize * pageIndex}`);
      }
      let params: Params | any = {
        excludeDeletedEntities: !isEmpty(searchParams.get("includeDE"))
          ? !searchParams.get("includeDE")
          : true,
        includeSubClassifications: !isEmpty(searchParams.get("excludeSC"))
          ? !searchParams.get("excludeSC")
          : true,
        includeSubTypes: !isEmpty(searchParams.get("excludeST"))
          ? !searchParams.get("excludeST")
          : true,
        includeClassificationAttributes: true,
        ...(isEmpty(classificationParams || glossaryTypeParams) && {
          entityFilters: !isEmpty(entityFilterParams)
            ? searchParamsAPiQuery(entityFilterParams)
            : null
        }),
        ...(isEmpty(classificationParams || glossaryTypeParams) && {
          tagFilters: !isEmpty(tagFilterParams)
            ? searchParamsAPiQuery(tagFilterParams)
            : null
        }),
        ...(isEmpty(classificationParams || glossaryTypeParams) && {
          attributes:
            searchParams.get("attributes")?.split(",") != null
              ? findUniqueValues(
                  searchParams.get("attributes")?.split(",") || [],
                  defaultColumnsName
                )
              : []
        }),
        limit:
          searchParams.get("pageLimit") != null
            ? Number(searchParams.get("pageLimit"))
            : pageSize,
        offset:
          searchParams.get("pageOffset") != null
            ? Number(searchParams.get("pageOffset"))
            : pageIndex * pageSize,
        ...(isEmpty(classificationParams || glossaryTypeParams) && {
          relationshipFilters: !isEmpty(relatonshipParams)
            ? searchParamsAPiQuery(relatonshipParams)
            : null,
          ...(searchParams.get("query") != null && {
            query: searchParams.get("query")
          })
        }),
        typeName: searchParams.get("type") || null,
        classification: searchParams.get("tag") || classificationParams || null,
        termName: searchParams.get("term") || glossaryTypeParams || null
      };
      let dslParams = {
        limit: pageSize,
        offset:
          searchParams.get("pageOffset") != null
            ? searchParams.get("pageOffset")
            : pageIndex * pageSize || 0,
        typeName: searchParams.get("type") || null,
        ...(searchParams.get("query") != null && {
          query: searchParams.get("query")
        })
      };

      globalSearchParams.basicParams = params;
      globalSearchParams.dslParams = dslParams;

      let searchTypeParams =
        searchParams.get("searchType") == "dsl" ? dslParams : params;
      try {
        let searchResp = await getBasicSearchResult(
          {
            data:
              (searchParams.get("searchType") == "basic" ||
                !isEmpty(classificationParams || glossaryTypeParams)) &&
              searchTypeParams,
            params: searchParams.get("searchType") == "dsl" && searchTypeParams
          },
          searchParams.get("searchType") || "basic"
        );
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
  let typeDefEntityData = !isNull(entityData)
    ? entityData.entityDefs.find((entity: { name: string }) => {
        if (entity.name == searchParams.get("type")) {
          return entity;
        }
      })
    : {};

  const defaultColumns: any = [
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
                  className={`entity-name nav-link text-decoration-none ${
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
      header: "Name",
      sortingFn: "alphanumeric",
      show: true
    },
    ,
    {
      accessorFn: (row: any) => row.attributes.owner,
      accessorKey: "owner",
      cell: (info: any) => <span>{info.getValue()}</span>,
      header: "Owner",
      show: true
    },
    {
      accessorFn: (row: any) => row.attributes.description,
      accessorKey: "description",
      cell: (info: any) => <span>{info.getValue()}</span>,
      header: "Description",
      show: true
    },
    {
      accessorFn: (row: any) => row.typeName,
      accessorKey: "typeName",
      cell: (info: any) => (
        <LightTooltip title={`Search ${info.getValue()}`}>
          <Link
            className="text-blue text-decoration-none"
            to={{
              pathname: "/"
            }}
          >
            {info.getValue()}
          </Link>
        </LightTooltip>
      ),
      header: "Type",
      show: true
    },
    {
      accessorFn: (row: any) => row.classificationNames[0],
      accessorKey: "classificationNames",
      cell: (info: any) => {
        let data: { status: string; guid: string; classifications: any } =
          info.row.original;
        if (data.guid == "-1") {
          return;
        }

        return (
          <DialogShowMoreLess
            value={data}
            readOnly={
              data.status && entityStateReadOnly[data.status] ? true : false
            }
            setUpdateTable={setUpdateTable}
            columnVal="classifications"
            colName="Classification"
            displayText="typeName"
            removeApiMethod={removeClassification}
            isShowMoreLess={true}
          />
        );
      },
      header: "Classifications",
      show: true,
      enableSorting: false
    },
    isEmpty(glossaryTypeParams) && {
      accessorFn: (row: { meanings: { qualifiedName?: string }[] }) =>
        row.meanings[0]?.qualifiedName,
      accessorKey: "term",
      cell: (info: any) => {
        let data: {
          status: string;
          guid: string;
          typeName: string;
          meanings: any;
        } = info.row.original;
        if (data.guid == "-1") {
          return;
        }
        if (data.typeName && !startsWith(data.typeName, "AtlasGlossary")) {
          return (
            <DialogShowMoreLess
              value={data}
              readOnly={
                data.status && entityStateReadOnly[data.status] ? true : false
              }
              columnVal="meanings"
              colName="Term"
              setUpdateTable={setUpdateTable}
              displayText={"qualifiedName"}
              removeApiMethod={removeTerm}
              isShowMoreLess={true}
            />
          );
        }
      },
      header: "Term",
      show: true,
      enableSorting: false
    }
  ];
  let superTypeAttr: any = [];
  const getSuperTypeAttributeDefsCol = (
    superTypes: any,
    entityAttr: string
  ) => {
    superTypes?.map((superTypesEntity: any) => {
      let superTypeAttributeData = !isNull(entityData)
        ? entityData.entityDefs.find((entity: any) => {
            if (entity.name == superTypesEntity) {
              return entity;
            }
          })
        : {};

      superTypeAttributeData?.attributeDefs?.map(
        (superTypesEntityData: any) => {
          let superTypesObj = {};
          if (!isEmpty(superTypesEntityData)) {
            let referredEntities: number =
              superTypeAttributeData.relationshipAttributeDefs.findIndex(
                (obj: { name: string }) =>
                  obj.name === superTypesEntityData.name
              );
            if (referredEntities == -1) {
              // let referredEntities = searchData.referredEntities[obj?.guid];
              superTypesObj = {
                accessorFn: (row: any) =>
                  row.attributes[superTypesEntityData.name],
                accessorKey: superTypesEntityData.name,
                header:
                  superTypesEntityData.name.charAt(0).toUpperCase() +
                  superTypesEntityData.name.slice(1),
                cell: (info: any) => {
                  let rowAttrVal =
                    info.row.original.attributes[superTypesEntityData.name];
                  if (searchData.referredEntities !== undefined) {
                    if (isArray(rowAttrVal)) {
                      let refAtrrVal = rowAttrVal.map((obj: any) => {
                        let referredEntities =
                          searchData.referredEntities[obj?.guid];
                        return getValues(
                          info,
                          referredEntities,
                          superTypesEntityData,
                          "relationShipAttr"
                        );
                      });
                      return refAtrrVal.length > 0 ? (
                        <TextShowMoreLess data={refAtrrVal} />
                      ) : (
                        <span>NA</span>
                      );
                    } else {
                      let referredEntities =
                        searchData.referredEntities[rowAttrVal?.guid];
                      return getValues(
                        info,
                        referredEntities,
                        superTypesEntityData,
                        "relationShipAttr"
                      );
                    }
                  } else {
                    return getValues(
                      info,
                      superTypeAttributeData,
                      superTypesEntityData
                    );
                  }
                },
                show: false
              };
            }
          }
          superTypeAttr.push(superTypesObj);
        }
      );
      if (!isEmpty(superTypeAttributeData?.superTypes) && entityAttr == "") {
        return getSuperTypeAttributeDefsCol(
          superTypeAttributeData?.superTypes,
          ""
        );
      }
    });
    return superTypeAttr;
  };
  const getEntityDefsCol = (typeDefEntityData: any) => {
    let getEntityAttr: any = [];

    typeDefEntityData?.attributeDefs?.map((entity: any) => {
      let referredEntities: number =
        typeDefEntityData.relationshipAttributeDefs.findIndex(
          (obj: { name: string }) => obj.name === entity.name
        );
      if (referredEntities == -1) {
        getEntityAttr.push({
          accessorFn: (row: any) => row.attributes[entity.name],
          accessorKey: entity.name,
          header: entity.name.charAt(0).toUpperCase() + entity.name.slice(1),
          cell: (info: any) => {
            if (referredEntities == -1) {
              return getValues(info, typeDefEntityData, entity);
            }
          },

          show: false,
          enableSorting:
            entity.typeName.search(
              /(string|date|boolean|int|number|byte|float|long|double|short)/i
            ) == 0
        });
      }

      if (!isEmpty(typeDefEntityData?.superTypes)) {
        let superTypesAttr: any = getSuperTypeAttributeDefsCol(
          typeDefEntityData?.superTypes,
          "entityAttr"
        );
        getEntityAttr = [...getEntityAttr, ...superTypesAttr];
      }
    });
    return getEntityAttr;
  };

  let entitytAttributeDefsCol = flattenArray(
    removeDuplicateObjects(getEntityDefsCol(typeDefEntityData))
  );

  let superTypeAttributeDefsCol = flattenArray(
    removeDuplicateObjects(
      getSuperTypeAttributeDefsCol(typeDefEntityData?.superTypes, "")
    )
  );
  let relationshipAttributeDefsCol =
    typeDefEntityData?.relationshipAttributeDefs?.map(
      (entity: { typeName: any; name: string }) => {
        return {
          accessorFn: (row: any) => row.attributes[entity.name],
          accessorKey: entity.name,
          header: entity.name.charAt(0).toUpperCase() + entity.name.slice(1),
          cell: (user: any) => {
            let rowVal = user.row.original[entity.name];
            if (isArray(rowVal) && rowVal.length > 0) {
              return user.row.original[entity.name].length > 0 ? (
                <TextShowMoreLess data={rowVal} displayText="displayText" />
              ) : (
                <span>NA</span>
              );
            }

            if (searchData.referredEntities != undefined) {
              let rowAttrVal = user.row.original.attributes[entity.name];
              if (isArray(rowAttrVal)) {
                let refAtrrVal = rowAttrVal.map((obj: any) => {
                  let referredEntities = searchData.referredEntities[obj?.guid];
                  return getValues(
                    user,
                    referredEntities,
                    entity,
                    "relationShipAttr"
                  );
                });
                return refAtrrVal.length > 0 ? (
                  <TextShowMoreLess data={refAtrrVal} />
                ) : (
                  <span>NA</span>
                );
              } else {
                let referredEntities =
                  searchData.referredEntities[rowAttrVal?.guid];
                return getValues(
                  user,
                  referredEntities,
                  entity,
                  "relationShipAttr"
                );
              }
            }
          },
          show: false,
          enableSorting:
            entity.typeName.search(
              /(string|date|boolean|int|number|byte|float|long|double|short)/i
            ) == 0
        };
      }
    );

  let dynamicColumns: any = customSortBy(
    [
      ...(entitytAttributeDefsCol || []),
      ...(relationshipAttributeDefsCol || []),
      ...(superTypeAttributeDefsCol || [])
    ],
    ["header"]
  );

  let defaultHideColumnsName = [
    {
      accessorKey: "__timestamp",
      header: "Created Timestamp"
    },
    {
      accessorKey: "__modificationTimestamp",
      header: "Last Modified Timestamp"
    },
    { accessorKey: "__modifiedBy", header: "Last Modified User" },
    {
      accessorKey: "__createdBy",
      header: "Created By User"
    },
    {
      accessorKey: "__state",
      header: "Status"
    },
    {
      accessorKey: "__guid",
      header: "Guid"
    },
    {
      accessorKey: "__typeName",
      header: "Type Name"
    },
    {
      accessorKey: "__isIncomplete",
      header: "IsIncomplete"
    },
    {
      accessorKey: "__labels",
      header: "Label(s)"
    },
    {
      accessorKey: "__customAttributes",
      header: "User-defined Properties"
    },
    {
      accessorKey: "__pendingTasks",
      header: "__pendingTasks"
    }
  ];
  let defaultHideColumns = defaultHideColumnsName?.map(
    (entity: any, index: any) => {
      let classificationHideCol: string[] = [
        "Type Name",
        "Created Timestamp",
        "Last Modified Timestamp",
        "Last Modified User",
        "Created By User"
      ];

      if (
        !isEmpty(searchParams.get("tag")) &&
        classificationHideCol.findIndex(
          (obj: string) => obj == entity.header
        ) == -1
      ) {
        return null;
      }
      if (entity.accessorKey == "__guid") {
        return {
          accessorKey: "__guid",
          header: "Guid",
          cell: (user: any) => {
            return (
              <span>
                {" "}
                <Link
                  className="text-blue text-decoration-none"
                  to={{
                    pathname: `/detailPage/${user.row.original.attributes.__guid}`
                  }}
                >
                  {user.row.original.attributes.__guid}
                </Link>
              </span>
            );
          },
          show: false
        };
      }

      return {
        accessorKey: entity.accessorKey,
        header: entity.header,
        cell: (user: any) => {
          let rowData = user.row.original;
          if (
            entity.accessorKey == "__timestamp" ||
            entity.accessorKey == "__modificationTimestamp"
          ) {
            let date = user.row.original.attributes[entity.accessorKey];
            return date !== undefined && <span>{dateFormat(date)}</span>;
          }
          return (
            <span key={index}>{rowData.attributes[entity.accessorKey]}</span>
          );
        },
        show: false
      };
    }
  );

  let allColumns =
    isEmpty(searchParams.get("type")) &&
    isEmpty(searchParams.get("tag")) &&
    !isEmpty(searchParams.get("term"))
      ? removeDuplicateObjects([...defaultColumns])
      : removeDuplicateObjects([
          ...defaultColumns,
          ...dynamicColumns,
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
  const getDefaultSort = useMemo(() => [{ id: "name", asc: true }], []);

  return (
    <Stack
      // paddingTop={
      //   isEmpty(classificationParams || glossaryTypeParams) ? 0 : "40px"
      // }
      // marginTop={
      //   isEmpty(classificationParams || glossaryTypeParams) ? 0 : "20px"
      // }
      // padding="16px"
      position="relative"
      gap={"1rem"}
    >
      {!isEmpty(classificationParams || glossaryTypeParams) && (
        <Stack
          direction="row"
          justifyContent="right"
          alignItems="center"
          right={0}
          top={0}
        >
          <FormGroup>
            <FormControlLabel
              control={
                <AntSwitch
                  sx={{ m: 1 }}
                  size="small"
                  checked={checkedSubClassifications}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                    handleSwitchChangeSubClassification(e);
                  }}
                  onClick={(e) => {
                    e.stopPropagation();
                  }}
                  inputProps={{ "aria-label": "controlled" }}
                />
              }
              label="Exclude sub-classification"
            />
          </FormGroup>
          <FormGroup>
            <FormControlLabel
              control={
                <AntSwitch
                  sx={{ m: 1 }}
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
      )}

      {/* {loader ? (
        <CircularProgress />
      ) : ( */}
      <TableLayout
        fetchData={fetchSearchResult}
        data={searchData.entities || []}
        columns={allColumns.filter(
          (value: {}) => Object.keys(value).length !== 0
        )}
        emptyText="No Records found!"
        isFetching={loader}
        pageCount={pageCount}
        columnVisibilityParams={true}
        defaultColumnVisibility={defaultColumnVisibility(allColumns)}
        defaultColumnParams={defaultColumnsName.join(",")}
        columnVisibility={true}
        refreshTable={refreshTable}
        defaultSortCol={getDefaultSort}
        clientSideSorting={true}
        isClientSidePagination={false}
        columnSort={true}
        showPagination={true}
        showRowSelection={true}
        tableFilters={
          isEmpty(classificationParams || glossaryTypeParams) ? true : false
        }
        assignFilters={
          !isEmpty(classificationParams || glossaryTypeParams)
            ? {
                classifications: true,
                term: true
              }
            : null
        }
        queryBuilder={true}
        allTableFilters={true}
        setUpdateTable={setUpdateTable}
        isfilterQuery={true}
      />
      {/* )} */}
    </Stack>
  );
  // );
};

export default SearchResult;
