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
  FormControlLabel,
  FormGroup,
  Grid,
  IconButton,
  Stack,
  Typography
} from "@mui/material";
import { TableLayout } from "@components/Table/TableLayout";
import { EntityState } from "@models/relationshipSearchType";
import { useSelector } from "react-redux";
import { isEmpty, pick } from "@utils/Utils";
import { LightTooltip } from "@components/muiComponents";
import { Link } from "react-router-dom";
import { entityStateReadOnly } from "@utils/Enum";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import DialogShowMoreLess from "@components/DialogShowMoreLess";
import { useMemo, useState } from "react";
import { AntSwitch } from "@utils/Muiutils";

const SchemaTab = ({
  entity,
  referredEntities,
  loading,
  schemaElementsAttribute
}: any) => {
  const { entityData, loading: _loader } = useSelector(
    (state: EntityState) => state.entity
  );

  const [checked, setChecked] = useState<boolean>(
    !isEmpty(entity) && entityStateReadOnly[entity.status]
  );

  let schemaTableAttribute = null;
  let attribute: any = null;
  let firstColumn: { typeName: string } | any = null;
  let defObj = null;
  let activeObj: any = [];
  let deleteObj: any = [];

  const getRelationshipEntityObj = (entityObj: { guid: string | number }) => {
    return referredEntities?.[entityObj.guid] != undefined
      ? referredEntities?.[entityObj.guid]
      : entityObj;
  };

  if (!isEmpty(entity) && !isEmpty(entityData?.entityDefs)) {
    attribute =
      entity.relationshipAttributes[schemaElementsAttribute] ||
      entity.attributes[schemaElementsAttribute];
    firstColumn = !isEmpty(attribute)
      ? getRelationshipEntityObj(attribute[0])
      : null;
    defObj =
      !isEmpty(entityData?.entityDefs) && !isEmpty(firstColumn)
        ? entityData?.entityDefs?.find((obj: { name: any }) => {
            return obj.name == firstColumn.typeName;
          })
        : {};
  }

  if (defObj && defObj?.options?.schemaAttributes) {
    if (firstColumn) {
      try {
        let mapObj = JSON.parse(defObj?.options?.schemaAttributes);
        schemaTableAttribute = pick(firstColumn.attributes, mapObj);
      } catch (e) {}
    }
  }

  let defaultColumns: any = !isEmpty(schemaTableAttribute)
    ? Object.keys(schemaTableAttribute).map((key) => {
        if (key != undefined && key !== "position") {
          if (key == "name") {
            return {
              accessorFn: (row: any) => row.attributes.name,
              accessorKey: "name",
              cell: (info: any) => {
                let entityDef: any = info.row.original;

                const getName = (entity: { guid: string }) => {
                  const href = `/detailPage/${entity.guid}`;

                  return (
                    <LightTooltip title={entityDef.attributes.name}>
                      {entity.guid != "-1" ? (
                        <Link
                          className={`entity-name text-decoration-none ${
                            entityDef.status &&
                            entityStateReadOnly[entityDef.status]
                              ? "text-red"
                              : "text-blue"
                          }`}
                          style={{
                            width: "unset !important",
                            whiteSpace: "nowrap"
                          }}
                          to={{
                            pathname: href
                          }}
                        >
                          {entityDef.attributes.name}
                        </Link>
                      ) : (
                        <span>{entityDef.attributes.name} </span>
                      )}
                    </LightTooltip>
                  );
                };

                return (
                  <div className="searchTableName">
                    {getName(entityDef)}
                    {entityDef.status &&
                      entityStateReadOnly[entityDef.status] && (
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
                            <DeleteOutlineOutlinedIcon
                              sx={{ fontSize: "1.25rem" }}
                            />
                          </IconButton>
                        </LightTooltip>
                      )}
                  </div>
                );
              },
              header: "Name",
              enableSorting: true,
              id: "name"
            };
          }
          return {
            accessorFn: (row: any) => row.attributes[key],
            accessorKey: key,
            cell: (info: any) => {
              let values = info.row.original.attributes;
              return (
                <Typography>
                  {!isEmpty(values[key]) ? values[key] : "N/A"}
                </Typography>
              );
            },
            header: key,
            enableSorting: true,
            id: key
          };
        }
      })
    : [];
  defaultColumns.push({
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
          // setUpdateTable={setUpdateTable}
          columnVal="classifications"
          colName="Classification"
          displayText="typeName"
          // removeApiMethod={removeClassification}
          isShowMoreLess={true}
        />
      );
    },
    header: "Classifications",
    show: true,
    enableSorting: false
  });

  for (let keys in attribute) {
    if (!entityStateReadOnly[attribute[keys].entityStatus]) {
      activeObj.push(attribute[keys]);
    } else if (entityStateReadOnly[attribute[keys].entityStatus]) {
      deleteObj.push(attribute[keys]);
    }
  }

  const getAttributeObj = () => {
    if ((isEmpty(deleteObj) && !checked) || (isEmpty(deleteObj) && checked)) {
      return attribute;
    } else if (!isEmpty(deleteObj) && checked) {
      return deleteObj;
    }
  };

  let activeDeletedAttributes = getAttributeObj();

  let tableData = useMemo(
    () =>
      !isEmpty(activeDeletedAttributes)
        ? activeDeletedAttributes.map((obj: { guid: string | number }) => {
            return getRelationshipEntityObj(obj);
          })
        : [],
    [checked]
  );

  const handleSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    event.stopPropagation();
    setChecked(event.target.checked);
  };

  return (
    <>
      <Grid container marginTop={0} className="properties-container">
        <Grid item md={12} p={2}>
          <Stack position="relative">
            <Stack
              direction="row"
              justifyContent="flex-end"
              alignItems="center"
              marginBottom="0.75rem"
              data-id="checkDeletedEntity"
            >
              <FormGroup>
                <FormControlLabel
                  control={
                    <AntSwitch
                      size="small"
                      checked={checked}
                      onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                        handleSwitchChange(e);
                      }}
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                      sx={{ marginRight: "4px" }}
                      inputProps={{ "aria-label": "controlled" }}
                    />
                  }
                  label="Show historical entities"
                />
              </FormGroup>
            </Stack>

            <TableLayout
              data={tableData}
              columns={defaultColumns.filter(Boolean)}
              emptyText="No Records found!"
              isFetching={loading}
              showRowSelection={true}
              columnVisibility={false}
              clientSideSorting={true}
              columnSort={true}
              showPagination={true}
              tableFilters={false}
              assignFilters={{ classifications: true, term: false }}
            />
          </Stack>
        </Grid>
      </Grid>
    </>
  );
};

export default SchemaTab;
