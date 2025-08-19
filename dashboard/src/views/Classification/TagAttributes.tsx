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
 * limitations under the License.useForm
 */

import { LightTooltip, CustomButton } from "@components/muiComponents";
import {
  Stack,
  TextField,
  Select,
  SelectChangeEvent,
  MenuItem,
  IconButton
} from "@mui/material";
import { isEmpty } from "@utils/Utils";
import { Controller, useFieldArray } from "react-hook-form";
import ClearOutlinedIcon from "@mui/icons-material/ClearOutlined";
import { useAppSelector } from "@hooks/reducerHook";
import { defaultDataType } from "@utils/Enum";
import AddIcon from "@mui/icons-material/Add";

const TagAtrributes = ({ control }: any) => {
  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const { enumDefs } = enumObj.data;
  const enumType = !isEmpty(enumDefs)
    ? enumDefs.map((obj: { name: string; guid: string }) => obj.name)
    : [];

  const dataTypeSelector = [...defaultDataType, ...enumType];
  const { fields, append, remove } = useFieldArray({
    control,
    name: "attribute"
  });

  return (
    <>
      <LightTooltip title={"Add New Attribute"}>
        <CustomButton
          sx={{
            alignSelf: "flex-start",
            marginBottom: "1rem",
            height: "32px"
          }}
          variant="outlined"
          size="small"
          onClick={(e: any) => {
            e.stopPropagation();
            append({ attributeName: "", typeName: "" });
          }}
          startIcon={<AddIcon />}
        >
          Add New Attributes
        </CustomButton>
      </LightTooltip>
      {fields.map((field, index) => {
        return (
          <Stack gap="1rem" key={field.id} direction="row">
            <Controller
              control={control}
              name={`atrribute.${index}.atrributeName` as const}
              defaultValue={null}
              render={({ field: { onChange, value } }) => (
                <>
                  <TextField
                    margin="normal"
                    fullWidth
                    onChange={onChange}
                    value={value}
                    variant="outlined"
                    size="small"
                    placeholder={"Attribute Name"}
                    className="form-textfield"
                    sx={{
                      marginTop: "8px !important",
                      marginBottom: "8px !important"
                    }}
                  />
                </>
              )}
            />
            <Controller
              control={control}
              name={`atrribute.${index}.typeName` as const}
              defaultValue={null}
              render={({ field: { onChange, value } }) => (
                <>
                  <div style={{ width: "100%" }}>
                    <Select
                      fullWidth
                      size="small"
                      defaultValue="string"
                      id="demo-select-small"
                      value={value}
                      onChange={(e: SelectChangeEvent) => {
                        onChange(e.target.value);
                      }}
                      className="form-textfield"
                      sx={{
                        marginTop: "8px !important",
                        marginBottom: "8px !important"
                      }}
                    >
                      {dataTypeSelector.map((type) => (
                        <MenuItem key={type} value={type}>
                          {type}
                        </MenuItem>
                      ))}
                    </Select>
                  </div>
                </>
              )}
            />
            <IconButton
              aria-label="back"
              color="error"
              sx={{
                display: "inline-flex",
                position: "relative",
                padding: "4px",
                marginLeft: "4px"
              }}
              onClick={(e) => {
                e.stopPropagation();
                remove(index);
              }}
            >
              <ClearOutlinedIcon sx={{ fontSize: "1.25rem" }} />
            </IconButton>
          </Stack>
        );
      })}
    </>
  );
};

export default TagAtrributes;
