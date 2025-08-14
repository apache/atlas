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
  Stack,
  ToggleButton,
  ToggleButtonGroup,
  Typography
} from "@mui/material";
import { useMemo, useState } from "react";
import { TableLayout } from "@components/Table/TableLayout";
import { getValues } from "@components/commonComponents";
import { customSortByObjectKeys, isArray, isEmpty } from "@utils/Utils";
import { EntityDetailTabProps } from "@models/entityDetailType";
import RelationshipLineage from "./RelationshipLineage";
import { AntSwitch } from "@utils/Muiutils";

const RelationshipsTab: React.FC<EntityDetailTabProps> = ({
  entity,
  referredEntities,
  loading
}) => {
  const { relationshipAttributes = {} } = entity || {};
  let columnData = { ...relationshipAttributes };

  let rowData = [];
  for (let key in columnData) {
    rowData.push({ [key]: columnData[key] });
  }

  let nonEmptyRowData = rowData.filter((obj) => {
    const entries = Object.entries(obj);
    return entries.every(([_key, value]) => !isEmpty(value));
  });

  const [alignment, setAlignment] = useState<string>("table");
  const [checked, setChecked] = useState<boolean>(false);

  const handleChange = (
    _event: React.MouseEvent<HTMLElement>,
    newAlignment: string
  ) => {
    if (newAlignment !== null) {
      setAlignment(newAlignment);
    }
  };

  const handleSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    event.stopPropagation();
    setChecked(event.target.checked);
  };

  const defaultColumns = useMemo(
    () => [
      {
        accessorKey: "key",
        cell: (info: any) => {
          let keys: string[] = Object.keys(info.row.original);
          let values = info.row.original;
          return (
            <Typography fontWeight="600">{`${keys[0]} ${
              isArray(values[keys[0]]) && !isEmpty(values[keys[0]])
                ? `(${values[keys[0]].length})`
                : ""
            }`}</Typography>
          );
        },
        header: "Key",
        width: "30%"
      },
      {
        accessorKey: "value",
        cell: (info: any) => {
          let keys: string[] = Object.keys(info.row.original);
          let values = info.row.original;
          return (
            <span className="value-text">
              {getValues(
                values[keys[0]],
                columnData,
                entity,
                undefined,
                "properties",
                referredEntities,
                undefined,
                Object.keys(info.row.original)[0]
              )}
            </span>
          );
        },
        header: "Value"
      }
    ],
    []
  );

  return (
    <Grid container marginTop={0} className="properties-container">
      <Grid item md={12} p={2}>
        <Stack>
          <Stack
            direction="row"
            justifyContent="space-between"
            alignItems="center"
            marginBottom="0.75rem"
            gap="1rem"
          >
            <ToggleButtonGroup
              size="small"
              color="primary"
              value={alignment}
              exclusive
              onChange={handleChange}
              aria-label="Platform"
            >
              <ToggleButton size="small" value="graph">
                Graph
              </ToggleButton>
              <ToggleButton size="small" value="table">
                Table
              </ToggleButton>
            </ToggleButtonGroup>
            {alignment == "table" && (
              <FormGroup>
                <FormControlLabel
                  sx={{ marginRight: "0" }}
                  control={
                    <AntSwitch
                      size="small"
                      sx={{ marginRight: "8px" }}
                      checked={checked}
                      onChange={(e) => {
                        handleSwitchChange(e);
                      }}
                      onClick={(e) => {
                        e.stopPropagation();
                      }}
                      inputProps={{ "aria-label": "controlled" }}
                    />
                  }
                  label="Show Empty Values"
                />
              </FormGroup>
            )}
          </Stack>

          {alignment == "graph" && <RelationshipLineage entity={entity} />}

          {alignment == "table" && (
            <TableLayout
              data={
                !isEmpty(checked ? rowData : nonEmptyRowData)
                  ? customSortByObjectKeys(checked ? rowData : nonEmptyRowData)
                  : []
              }
              columns={defaultColumns.filter(
                (value) => Object.keys(value).length !== 0
              )}
              emptyText="No Records found!"
              isFetching={loading}
              columnVisibility={false}
              columnSort={false}
              showPagination={false}
              showRowSelection={false}
              tableFilters={false}
            />
          )}
        </Stack>
      </Grid>
    </Grid>
  );
};

export default RelationshipsTab;
