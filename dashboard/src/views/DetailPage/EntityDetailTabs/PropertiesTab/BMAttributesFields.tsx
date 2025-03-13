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

import { useAppSelector } from "@hooks/reducerHook";
import {
  Select,
  SelectChangeEvent,
  MenuItem,
  Stack,
  TextField,
  Autocomplete,
  createFilterOptions
} from "@mui/material";
import { AdapterMoment } from "@mui/x-date-pickers/AdapterMoment";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import { DemoContainer } from "@mui/x-date-pickers/internals/demo";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { isEmpty } from "@utils/Utils";
import moment from "moment";
import { Controller } from "react-hook-form";
import ReactQuill from "react-quill-new";

const BMAttributesFields = ({ obj, control, index }: any) => {
  const { name, isOptional, typeName } = obj || {};
  // const tomorrow = moment();

  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const filter = createFilterOptions<any>();
  const { enumDefs } = enumObj?.data || {};

  let isMultiValued: string = typeName && typeName?.indexOf("array<") === 0;
  let multipleType: string = typeName?.match("array<(.*)>");
  let options: any,
    foundEnumType = {};
  if (isMultiValued && isMultiValued) {
    foundEnumType = !isEmpty(enumDefs)
      ? enumDefs.find((obj: { name: any }) => {
          return obj.name == multipleType[1];
        })
      : {};
    const { elementDefs }: any = foundEnumType || {};

    typeName == "array<boolean>"
      ? (options = [
          { label: "true", value: "true" },
          { label: "false", value: "false" }
        ])
      : (options = !isEmpty(elementDefs)
          ? elementDefs.map((obj: { value: any }) => {
              return { label: obj.value, value: obj.value };
            })
          : []);
  }
  switch (typeName) {
    case "date":
    case "array<date>":
      return (
        <Controller
          name={`businessMetadata.${index}.value` as const}
          control={control}
          key={`businessMetadata.${index}.value` as const}
          render={({ field: { onChange, value, ref } }) => (
            <>
              <div className="w-100" style={{ padding: 0 }}>
                <LocalizationProvider dateAdapter={AdapterMoment}>
                  <DemoContainer
                    sx={{ padding: 0, overflow: "unset" }}
                    components={["DatePicker"]}
                  >
                    <DatePicker
                      // minDate={tomorrow}
                      slotProps={{ textField: { size: "small" } }}
                      value={value ? moment(value) : null}
                      defaultValue={moment()}
                      inputRef={ref}
                      onChange={(date) => {
                        onChange(date ? date.toISOString() : null);
                      }}
                    />
                  </DemoContainer>
                </LocalizationProvider>
              </div>
            </>
          )}
        />
      );

    case "string":
    case "int":
    case "short":
    case "float":
    case "double":
    case "long":
      return (
        <Controller
          control={control}
          name={`businessMetadata.${index}.value` as const}
          rules={{
            required: true
          }}
          defaultValue={null}
          render={({ field }) => (
            <Stack gap="0.5rem">
              <div style={{ position: "relative", flexBasis: "60%" }}>
                {typeName == "string" ? (
                  <ReactQuill
                    {...field}
                    theme="snow"
                    placeholder={"Enter String"}
                    onChange={(text) => {
                      field.onChange(text);
                    }}
                    className="classification-form-editor"
                  />
                ) : (
                  <TextField
                    margin="none"
                    fullWidth
                    onChange={(text) => {
                      field.onChange(text);
                    }}
                    variant="outlined"
                    size="small"
                    type={typeName == "string" ? "text" : "number"}
                    placeholder={name}
                  />
                )}
              </div>
            </Stack>
          )}
        />
      );
    case "boolean":
      return (
        <Controller
          name={`businessMetadata.${index}.value` as const}
          control={control}
          rules={{
            required: isOptional ? false : true
          }}
          key={name}
          defaultValue={null}
          render={({ field: { onChange, value } }) => (
            <>
              <div style={{ width: "100%" }}>
                <Select
                  fullWidth
                  size="small"
                  id="demo-select-small"
                  value={value}
                  onChange={(e: SelectChangeEvent) => {
                    onChange(e.target.value);
                  }}
                  renderValue={(selected) => {
                    if (selected.length === 0) {
                      return <em>--Select true or false--</em>;
                    }

                    return selected;
                  }}
                >
                  <MenuItem value="">
                    <em>--Select true or false--</em>
                  </MenuItem>
                  <MenuItem value="true">true</MenuItem>
                  <MenuItem value="false">false</MenuItem>
                </Select>
              </div>
            </>
          )}
        />
      );
    case "array<string>":
    case "array<int>":
    case "array<short>":
    case "array<float>":
    case "array<double":
    case "array<long>":
      return (
        <Controller
          name={`businessMetadata.${index}.value` as const}
          control={control}
          key={`autocomplete-${name}`}
          rules={{
            required: isOptional ? false : true
          }}
          defaultValue={[]}
          render={({ field: { onChange, value }, fieldState: { error } }) => {
            return (
              <>
                <Autocomplete
                  size="small"
                  freeSolo
                  multiple
                  onChange={(_event, newValue) => {
                    onChange(newValue);
                  }}
                  sx={{ flexBasis: "60%" }}
                  filterOptions={(options, params) => {
                    const filtered = filter(options, params);

                    const { inputValue } = params;

                    const isExisting = options.some(
                      (option) => inputValue === option
                    );
                    if (inputValue !== "" && !isExisting) {
                      filtered.push({
                        inputValue
                      });
                    }

                    return filtered;
                  }}
                  value={isEmpty(value) ? [] : value}
                  getOptionLabel={(option) => {
                    return !isEmpty(option?.inputValue)
                      ? option.inputValue
                      : option;
                  }}
                  options={[]}
                  className="form-autocomplete-field"
                  renderOption={(props, option) => {
                    const { ...optionProps } = props;
                    const { inputValue } = option;
                    return <li {...optionProps}>{inputValue}</li>;
                  }}
                  isOptionEqualToValue={(option, value) =>
                    option.inputValue === value.inputValue
                  }
                  filterSelectedOptions
                  renderInput={(params) => (
                    <TextField
                      {...params}
                      error={!!error}
                      size="small"
                      InputProps={{
                        ...params.InputProps
                      }}
                      type={typeName == "array<string>" ? "string" : "number"}
                      placeholder={`Select a ${typeName} from the dropdown list`}
                      // helperText={error ? "This field is required" : ""}
                    />
                  )}
                />
              </>
            );
          }}
        />
      );

    case typeName || (typeName?.indexOf("array") > -1 && typeName):
      return (
        <Controller
          control={control}
          name={`businessMetadata.${index}.value` as const}
          key={`autocomplete-${name}`}
          rules={{
            required: isOptional ? false : true
          }}
          defaultValue={[]}
          render={({ field: { onChange, value }, fieldState: { error } }) => {
            return (
              <>
                <Autocomplete
                  freeSolo
                  size="small"
                  multiple
                  onChange={(_event, item) => {
                    onChange(item);
                  }}
                  sx={{ flexBasis: "60%" }}
                  value={isEmpty(value) ? [] : value}
                  filterSelectedOptions
                  getOptionLabel={(option: any) => option.label}
                  isOptionEqualToValue={(option, value) =>
                    option.label === value.label
                  }
                  options={options}
                  className="form-autocomplete-field"
                  renderInput={(params) => (
                    <TextField
                      {...params}
                      error={!!error}
                      className="form-textfield"
                      size="small"
                      InputProps={{
                        ...params.InputProps
                      }}
                      placeholder={`Select a ${typeName} from the dropdown list`}
                      // helperText={error ? "This field is required" : ""}
                    />
                  )}
                />
              </>
            );
          }}
        />
      );
    default:
      return (
        <Controller
          name={`businessMetadata.${index}.value` as const}
          control={control}
          rules={{
            required: !isOptional
          }}
          key={name}
          render={({ field: { onChange, value }, fieldState: { error } }) => (
            <>
              <TextField
                margin="none"
                error={!!error}
                fullWidth
                onChange={onChange}
                value={value}
                variant="outlined"
                size="small"
                type={typeName == "string" ? "text" : "number"}
                placeholder={name}
                // helperText={error ? "This field is required" : ""}
              />
            </>
          )}
        />
      );
  }
};

export default BMAttributesFields;
