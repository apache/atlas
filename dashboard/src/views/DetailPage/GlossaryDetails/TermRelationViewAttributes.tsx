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

import { TableLayout } from "@components/Table/TableLayout";
import { Stack, TextField, Typography } from "@mui/material";
import { attributeObj } from "@utils/Enum";
import { isEmpty } from "@utils/Utils";
import { useMemo } from "react";
import { Controller } from "react-hook-form";

const TermRelationViewAttributes = ({
  attrObj,
  editModal,
  control,
  currentType
}: any) => {
  const defaultColumns = useMemo(
    () => [
      {
        accessorKey: "name",
        cell: (info: any) => {
          let values = info.row.original;

          return <Typography fontWeight="400"> {values} </Typography>;
        },
        header: "Name",
        wdith: "10%",
        enableSorting: true
      },
      {
        accessorKey: "value",
        cell: (info: any) => {
          let values: string = info.row.original;
          const { displayText } = attrObj;
          return editModal ? (
            <Stack direction="row" gap="2rem">
              <Controller
                control={control}
                name={`${currentType}.${displayText}.${values}`}
                defaultValue={attrObj[values]}
                render={({ field: { onChange, value } }) => (
                  <>
                    <TextField
                      margin="normal"
                      fullWidth
                      value={value}
                      onChange={(e) => {
                        const value = e.target.value;
                        onChange(value);
                      }}
                      variant="outlined"
                      size="small"
                      placeholder={values}
                      className="form-textfield"
                    />
                  </>
                )}
              />
            </Stack>
          ) : (
            <Typography>
              {!isEmpty(attrObj[values]) ? attrObj[values] : "--"}
            </Typography>
          );
        },
        header: "Value",
        enableSorting: false
      }
    ],
    []
  );
  return (
    <>
      {" "}
      <TableLayout
        data={Object.keys(attributeObj)}
        columns={defaultColumns}
        emptyText="No Records found!"
        columnVisibility={false}
        clientSideSorting={false}
        columnSort={false}
        showPagination={false}
        showRowSelection={false}
        tableFilters={false}
      />
    </>
  );
};

export default TermRelationViewAttributes;
