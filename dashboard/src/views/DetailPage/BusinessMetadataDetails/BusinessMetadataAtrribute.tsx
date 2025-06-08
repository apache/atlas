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

import { EllipsisText } from "@components/commonComponents";
import { CustomButton, LightTooltip } from "@components/muiComponents";
import { TableLayout } from "@components/Table/TableLayout";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { Checkbox, Chip, Stack, Typography } from "@mui/material";
import { setEditBMAttribute } from "@redux/slice/createBMSlice";
import { defaultType } from "@utils/Enum";
import { isEmpty } from "@utils/Utils";
import { useMemo } from "react";

const BusinessMetadataAtrribute = ({ componentProps, row }: any) => {
  const dispatchState = useAppDispatch();
  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const { enumDefs } = enumObj.data || {};

  const { attributeDefs, loading, setForm, setBMAttribute, reset } =
    componentProps;

  const defaultColumns = useMemo(
    () => [
      {
        accessorKey: "name",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Attribute Name",
        enableSorting: true,
        show: true
      },
      {
        accessorKey: "typeName",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Type Name",
        enableSorting: true
      },
      {
        accessorKey: "searchWeight",
        cell: (info: any) =>
          !isEmpty(info.getValue()) ? (
            <Typography>{info.getValue()}</Typography>
          ) : (
            <span>N/A</span>
          ),
        header: "Search Weight",
        enableSorting: true
      },
      {
        accessorKey: "checkbox",
        cell: (info: any) => {
          return (
            <Checkbox
              defaultChecked={info.row.original.typeName.includes("array")}
              size="small"
              disabled
            />
          );
        },
        header: "Enable Multivalues",
        enableSorting: false
      },
      {
        accessorKey: "maxStrLength",
        cell: (info: any) => {
          const { maxStrLength } = info.row.original.options || {};
          return !isEmpty(maxStrLength) ? (
            <Typography>{maxStrLength}</Typography>
          ) : (
            <span>N/A</span>
          );
        },
        header: "Max Length",

        enableSorting: true
      },
      {
        accessorKey: "options",
        cell: (info: any) => {
          const { applicableEntityTypes } = info.row.original.options || {};
          const typesObj = !isEmpty(applicableEntityTypes)
            ? JSON.parse(applicableEntityTypes, (_key, value) => {
                try {
                  return JSON.parse(value);
                } catch (e) {
                  return value;
                }
              })
            : [];

          return (
            <Stack
              direction="row"
              flexWrap="wrap"
              gap="0.25rem"
              spacing={1}
              justifyContent="flex-start"
            >
              {!isEmpty(typesObj) ? (
                typesObj.map((label: string) => {
                  return (
                    <LightTooltip title={label}>
                      <Chip
                        label={<EllipsisText>{label}</EllipsisText>}
                        size="small"
                        sx={{
                          "& .MuiChip-label": {
                            display: "block",
                            overflow: "ellipsis",
                            maxWidth: "108px"
                          }
                        }}
                        className=" chip-items properties-labels-chip"
                        data-cy="tagClick"
                        clickable
                      />
                    </LightTooltip>
                  );
                })
              ) : (
                <span>N/A</span>
              )}
            </Stack>
          );
        },
        width: "20%",
        header: `${isEmpty(row) ? "Entity" : "Applicable"} Type(s)`,
        enableSorting: true
      },
      {
        accessorKey: "action",
        cell: (info) => {
          const { original = {} } = info?.row || {};
          return (
            <CustomButton
              variant="outlined"
              color="success"
              className="table-filter-btn assignTag"
              size="small"
              onClick={(_e: React.MouseEvent<HTMLElement>) => {
                const { typeName, options }: any = original;
                const str = !isEmpty(typeName) ? typeName : "";
                const start = !isEmpty(str) ? str.indexOf("<") + 1 : "";
                const end = !isEmpty(str) ? str.indexOf(">") : "";
                const extracted = !isEmpty(str) ? str.slice(start, end) : "";
                let currentTypeName =
                  str.indexOf("<") != -1
                    ? defaultType.includes(typeName)
                      ? extracted
                      : "enumeration"
                    : defaultType.includes(typeName)
                    ? typeName
                    : "enumeration";
                let selectedEnumObj = !isEmpty(enumDefs)
                  ? enumDefs.find((obj: { name: any }) => {
                      return obj.name == typeName;
                    })
                  : {};
                let selectedEnumValues = !isEmpty(selectedEnumObj)
                  ? selectedEnumObj?.elementDefs
                  : [];

                let enumTypeOptions = [...selectedEnumValues];
                const editObj = [
                  {
                    ...(original || {}),
                    options: {
                      ...(options || {}),
                      applicableEntityTypes: !isEmpty(options)
                        ? (function () {
                            try {
                              return JSON.parse(options?.applicableEntityTypes);
                            } catch (e) {
                              return options?.applicableEntityTypes;
                            }
                          })()
                        : []
                    },
                    typeName:
                      str.indexOf("<") != -1
                        ? defaultType.includes(typeName)
                          ? extracted
                          : "enumeration"
                        : defaultType.includes(typeName)
                        ? typeName
                        : "enumeration",
                    ...(currentTypeName == "enumeration" && {
                      enumType: typeName
                    }),
                    ...(currentTypeName == "enumeration" && {
                      enumValues: enumTypeOptions
                    }),
                    multiValueSelect: str.indexOf("<") != -1 ? true : false
                  }
                ];
                setForm(true);
                if (!isEmpty(reset)) {
                  reset({ attributeDefs: editObj || [] });
                }
                setBMAttribute(row?.original);
                dispatchState(setEditBMAttribute(original));
              }}
              data-cy="attributeEdit"
            >
              Edit
            </CustomButton>
          );
        },
        header: "Action",

        enableSorting: false
      }
    ],
    []
  );

  return (
    <TableLayout
      data={
        isEmpty(row) ? attributeDefs || [] : row?.original?.attributeDefs || []
      }
      columns={defaultColumns}
      emptyText="No Records found!"
      isFetching={loading}
      columnVisibility={false}
      clientSideSorting={isEmpty(row) ? true : false}
      columnSort={isEmpty(row) ? true : false}
      showPagination={isEmpty(row) ? true : false}
      showRowSelection={false}
      tableFilters={false}
    />
  );
};
export default BusinessMetadataAtrribute;
