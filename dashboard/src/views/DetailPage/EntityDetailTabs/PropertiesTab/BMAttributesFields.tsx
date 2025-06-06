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

import CustomDatepicker from "@components/DatePicker/CustomDatePicker";
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
import { isEmpty } from "@utils/Utils";
import moment from "moment";
import { Controller } from "react-hook-form";
import ReactQuill from "react-quill-new";

const BMAttributesFields = ({ obj, control, index }: any) => {
  const { name, typeName } = obj || {};

  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const filter = createFilterOptions<any>();
  const { enumDefs } = enumObj?.data || {};

  let isMultiValued: string = typeName && typeName?.indexOf("array<") === 0;
  let multipleType: string = typeName?.match("array<(.*)>");
  let modeTypeName = typeName;
  let options: any,
    foundEnumType = {};
  if (!isMultiValued || typeName === "array<boolean>") {
    if (multipleType && multipleType[1]) {
      modeTypeName = multipleType[1];
    }
    foundEnumType = !isEmpty(enumDefs)
      ? enumDefs.find((obj: { name: any }) => {
          return obj.name == modeTypeName;
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
  if (typeName === "date" || typeName === "array<date>") {
    return (
      <Controller
        name={`businessMetadata.${index}.value` as const}
        control={control}
        key={`businessMetadata.${index}.value` as const}
        defaultValue={new Date().toISOString()}
        render={({ field: { onChange, value, ref } }) => {
          if (!value) {
            const defaultDate = new Date();
            onChange(defaultDate.toISOString());
          }

          return typeName === "date" ? (
            <CustomDatepicker
              showPopperArrow={false}
              popperProps={{ strategy: "fixed" }}
              selected={
                value && moment(value).isValid()
                  ? moment(value).toDate()
                  : moment().toDate()
              }
              onChange={(date: { getTime: () => any }) => {
                onChange(date ? date.getTime() : null);
              }}
              ref={ref}
              dateFormat="MM/dd/yyyy"
            />
          ) : (
            <CustomDatepicker
              showPopperArrow={false}
              popperProps={{ strategy: "fixed" }}
              selectsMultiple
              selectedDates={
                Array.isArray(value)
                  ? value.map((d: string | number | Date) => new Date(d))
                  : []
              }
              onChange={(dates: any[]) => {
                onChange(
                  Array.isArray(dates)
                    ? dates.map((date) => date.getTime())
                    : []
                );
              }}
              shouldCloseOnSelect={false}
              disabledKeyboardNavigation
              ref={ref}
              dateFormat="MM/dd/yyyy"
            />
          );
        }}
      />
    );
  } else if (
    (typeName === "string" ||
      typeName === "int" ||
      typeName === "short" ||
      typeName === "float" ||
      typeName === "double" ||
      typeName === "long") &&
    typeName !== "array<boolean>" &&
    typeName !== "array<string>" &&
    typeName !== "array<date>"
  ) {
    return (
      <Controller
        control={control}
        name={`businessMetadata.${index}.value` as const}
        rules={{
          required: true
        }}
        defaultValue={""}
        render={({ field }) => (
          <Stack gap="0.5rem">
            <div style={{ position: "relative", flexBasis: "100%" }}>
              {typeName == "string" ? (
                <ReactQuill
                  {...field}
                  theme="snow"
                  placeholder={"Enter String"}
                  onChange={(text) => {
                    field.onChange(text);
                  }}
                  className="classification-form-editor"
                  value={typeof field.value === "string" ? field.value : ""}
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
                  value={
                    typeName == "string"
                      ? typeof field.value === "string"
                        ? field.value
                        : ""
                      : typeof field.value === "number"
                      ? field.value
                      : ""
                  }
                />
              )}
            </div>
          </Stack>
        )}
      />
    );
  } else if (typeName === "boolean") {
    return (
      <Controller
        name={`businessMetadata.${index}.value` as const}
        control={control}
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
  } else if (
    typeName === "array<string>" ||
    typeName === "array<int>" ||
    typeName === "array<short>" ||
    typeName === "array<float>" ||
    typeName === "array<double" ||
    typeName === "array<boolean>" ||
    typeName === "array<long>"
  ) {
    return (
      <Controller
        name={`businessMetadata.${index}.value` as const}
        control={control}
        key={`autocomplete-${name}`}
        // rules={{
        //   required: isOptional ? false : true
        // }}
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
                sx={{
                  flexBasis: "100%",
                  paddingTop: "4px",
                  paddingBottom: "4px",
                  paddingLeft: "6px",
                  height: "34px",
                  gap: "4px"
                }}
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

                  return typeName != "array<boolean>" ? filtered : options;
                }}
                value={!isEmpty(value) && Array.isArray(value) ? value : []}
                getOptionLabel={(option) => {
                  if (typeof option === "string") {
                    return option;
                  }

                  return !isEmpty(option?.inputValue)
                    ? option.inputValue
                    : option.label;
                }}
                options={!isEmpty(options) ? options : []}
                renderOption={(props, option) => {
                  const { ...optionProps } = props;
                  const { inputValue, label } = option;
                  return <li {...optionProps}>{inputValue || label}</li>;
                }}
                isOptionEqualToValue={(option, value) => {
                  if (!isEmpty(options?.label)) {
                    return option.label === value.label;
                  }
                  return option.inputValue === value.inputValue;
                }}
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
                  />
                )}
              />
            </>
          );
        }}
      />
    );
  } else if (
    typeName?.indexOf("array") > -1 &&
    typeName &&
    typeName !== "array<boolean>"
  ) {
    return (
      <Controller
        control={control}
        name={`businessMetadata.${index}.value` as const}
        key={`autocomplete-${name}`}
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
                sx={{ flexBasis: "100%" }}
                value={!isEmpty(value) && Array.isArray(value) ? value : []}
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
                filterSelectedOptions
                getOptionLabel={(option) => {
                  if (typeof option === "string") {
                    return option;
                  }

                  return !isEmpty(option?.inputValue)
                    ? option.inputValue
                    : option.label;
                }}
                isOptionEqualToValue={(option, value) => {
                  if (!isEmpty(options?.label)) {
                    return option.label === value.label;
                  }
                  return option.inputValue === value.inputValue;
                }}
                options={!isEmpty(options) ? options : []}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    error={!!error}
                    size="small"
                    InputProps={{
                      ...params.InputProps
                    }}
                    placeholder={`Select a ${typeName} from the dropdown list`}
                  />
                )}
              />
            </>
          );
        }}
      />
    );
  } else if (
    (typeName && typeName.indexOf("array") === -1) ||
    typeName !== "array<boolean>"
  ) {
    return (
      <Controller
        name={`businessMetadata.${index}.value` as const}
        control={control}
        key={name}
        defaultValue={""}
        render={({ field: { onChange, value } }) => (
          <>
            <div style={{ width: "100%" }}>
              <Select
                fullWidth
                size="small"
                id="demo-select-small"
                value={!isEmpty(value) ? value : ""}
                onChange={(e: SelectChangeEvent) => {
                  onChange(e.target.value);
                }}
              >
                {options.map((option: any) => (
                  <MenuItem key={option.value} value={option.value}>
                    {option.label}
                  </MenuItem>
                ))}
              </Select>
            </div>
          </>
        )}
      />
    );
  } else {
    return (
      <Controller
        name={`businessMetadata.${index}.value` as const}
        control={control}
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
            />
          </>
        )}
      />
    );
  }
};

export default BMAttributesFields;
