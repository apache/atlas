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
  createFilterOptions,
  CircularProgress,
  Typography
} from "@mui/material";
import type { RootState } from "@redux/store/store";
import {
  areEnumOptionsEqual,
  buildEnumOptionsForTypeName,
  EnumOption,
  getEnumOptionLabel,
  isArrayTypeName,
  isEnumTypeName,
  isPotentialEnumTypeName,
  normalizeMultiEnumValue
} from "@utils/enumTypeUtils";
import { isEmpty } from "@utils/Utils";
import moment from "moment";
import { useEffect, type Ref } from "react";
import { Control, Controller, FieldValues } from "react-hook-form";
import ReactQuill from "react-quill-new";

interface BMAttributeFieldObj {
  name: string;
  typeName: string;
}

interface BMAttributesFieldsProps {
  obj: BMAttributeFieldObj;
  control: Control<FieldValues>;
  index: number;
}

type AutocompleteFreeSoloOption = string | EnumOption | { inputValue: string; label?: string };

interface DateValueFieldProps {
  typeName: "date" | "array<date>";
  value: unknown;
  onChange: (value: unknown) => void;
  fieldRef: Ref<unknown>;
}

const DateValueField = ({
  typeName,
  value,
  onChange,
  fieldRef
}: DateValueFieldProps) => {
  useEffect(() => {
    if (typeName === "date" && !value) {
      onChange(new Date().toISOString());
    }
  }, [typeName, value, onChange]);

  if (typeName === "date") {
    return (
      <CustomDatepicker
        showPopperArrow={false}
        popperProps={{ strategy: "fixed" }}
        selected={
          value && moment(value).isValid()
            ? moment(value).toDate()
            : moment().toDate()
        }
        onChange={(date: { getTime: () => number }) => {
          onChange(date ? date.getTime() : null);
        }}
        ref={fieldRef}
        dateFormat="MM/dd/yyyy"
      />
    );
  }

  return (
    <CustomDatepicker
      showPopperArrow={false}
      popperProps={{ strategy: "fixed" }}
      selectsMultiple
      selectedDates={
        Array.isArray(value)
          ? value.map((d: string | number | Date) => new Date(d))
          : []
      }
      onChange={(dates: Date[]) => {
        onChange(
          Array.isArray(dates) ? dates.map((date) => date.getTime()) : []
        );
      }}
      shouldCloseOnSelect={false}
      disabledKeyboardNavigation
      ref={fieldRef}
      dateFormat="MM/dd/yyyy"
    />
  );
};

const BMAttributesFields = ({ obj, control, index }: BMAttributesFieldsProps) => {
  const { name, typeName } = obj || { name: "", typeName: "" };

  const enumObj = useAppSelector((state: RootState) => state.enum.enumObj);
  const filter = createFilterOptions<AutocompleteFreeSoloOption>();
  const enumDefs = enumObj?.data?.enumDefs ?? [];
  const enumLoading = enumObj?.loading === true;
  const enumLoaded = enumObj?.data != null;

  const isMultiValued = isArrayTypeName(typeName);
  const isPotentialEnumType = isPotentialEnumTypeName(typeName);
  const isEnumType = isEnumTypeName(typeName, enumDefs);
  const options = buildEnumOptionsForTypeName(typeName, enumDefs);
  const fieldWidthSx = { width: "100%" };
  const booleanSelectId = `bm-boolean-select-${index}`;
  const enumSelectId = `bm-enum-select-${index}`;

  if (isPotentialEnumType && enumLoading) {
    return (
      <Stack
        direction="row"
        alignItems="center"
        gap="0.5rem"
        className="bm-enum-loading"
        aria-label="Loading enum definitions"
      >
        <CircularProgress size={18} />
        <Typography variant="body2" color="text.secondary">
          Loading enum definitions...
        </Typography>
      </Stack>
    );
  }

  if (isPotentialEnumType && enumLoaded && isEmpty(enumDefs)) {
    return (
      <Typography
        variant="body2"
        color="text.secondary"
        className="bm-enum-unavailable-text"
      >
        No enum definitions present.
      </Typography>
    );
  }

  if (typeName === "date" || typeName === "array<date>") {
    return (
      <Controller
        name={`businessMetadata.${index}.value` as const}
        control={control}
        key={`businessMetadata.${index}.value` as const}
        defaultValue={new Date().toISOString()}
        render={({ field: { onChange, value, ref } }) => (
          <DateValueField
            typeName={typeName as "date" | "array<date>"}
            value={value}
            onChange={onChange}
            fieldRef={ref}
          />
        )}
      />
    );
  } else if (
    typeName === "string" ||
    typeName === "int" ||
    typeName === "short" ||
    typeName === "float" ||
    typeName === "double" ||
    typeName === "long"
  ) {
    return (
      <Controller
        control={control}
        name={`businessMetadata.${index}.value` as const}
        rules={{
          required: true
        }}
        defaultValue={""}
        render={({ field }) => {
          return (
          <Stack gap="0.5rem">
            <div style={{ position: "relative", flexBasis: "100%" }}>
              {typeName === "string" ? (
                <ReactQuill
                    key={`quill-${index}-${name}`}
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
                  className="bm-attribute-value-field"
                  onChange={(e) => {
                    // Keep string while typing to allow partial numeric input (e.g., "-", "1.")
                    field.onChange(e.target.value);
                  }}
                  onBlur={(e) => {
                    if (e.target.value !== "") {
                      const numValue = Number(e.target.value);
                      if (!isNaN(numValue)) {
                        field.onChange(numValue);
                      }
                    }
                  }}
                  variant="outlined"
                  size="small"
                  type="number"
                  placeholder={name}
                  value={
                    field.value !== null && field.value !== undefined
                      ? String(field.value)
                      : ""
                  }
                  inputProps={{
                    step: typeName === "float" || typeName === "double" ? "any" : undefined
                  }}
                />
              )}
            </div>
          </Stack>
          );
        }}
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
                className="bm-attribute-value-field"
                size="small"
                id={booleanSelectId}
                displayEmpty
                value={value ?? ""}
                onChange={(e: SelectChangeEvent) => {
                  onChange(e.target.value);
                }}
                renderValue={(selected) => {
                  if (!selected) {
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
    typeName === "array<double>" ||
    typeName === "array<boolean>" ||
    typeName === "array<long>"
  ) {
    return (
      <Controller
        name={`businessMetadata.${index}.value` as const}
        control={control}
        key={`autocomplete-${name}`}
        defaultValue={[]}
        render={({ field: { onChange, value }, fieldState: { error } }) => {
          return (
            <>
              <Autocomplete
                size="small"
                freeSolo
                multiple
                className="bm-attribute-value-field"
                onChange={(_event, newValue) => {
                  onChange(newValue);
                }}
                sx={{
                  ...fieldWidthSx,
                  paddingTop: "4px",
                  paddingBottom: "4px",
                  paddingLeft: "6px",
                  gap: "4px",
                  "& .MuiAutocomplete-inputRoot": {
                    flexWrap: "wrap"
                  }
                }}
                filterOptions={(optionList, params) => {
                  const filtered = filter(optionList, params);

                  const { inputValue } = params;

                  const isExisting = optionList.some(
                    (option) => inputValue === option
                  );
                  if (inputValue !== "" && !isExisting) {
                    filtered.push({
                      inputValue
                    });
                  }

                  return typeName !== "array<boolean>" ? filtered : optionList;
                }}
                value={!isEmpty(value) && Array.isArray(value) ? value : []}
                getOptionLabel={(option) => {
                  if (typeof option === "string") {
                    return option;
                  }

                  return !isEmpty(option?.inputValue)
                    ? option.inputValue
                    : option.label ?? "";
                }}
                options={!isEmpty(options) ? options : []}
                renderOption={(props, option) => {
                  const { ...optionProps } = props;
                  const optionLabel =
                    typeof option === "string"
                      ? option
                      : option.inputValue || option.label;
                  return <li {...optionProps}>{optionLabel}</li>;
                }}
                isOptionEqualToValue={(option, selectedValue) => {
                  if (typeof option === "string" || typeof selectedValue === "string") {
                    return option === selectedValue;
                  }
                  if (!isEmpty(option?.label) && !isEmpty(selectedValue?.label)) {
                    return option.label === selectedValue.label;
                  }
                  return option.inputValue === selectedValue.inputValue;
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
                    type={typeName === "array<string>" ? "string" : "number"}
                    placeholder={`Select a ${typeName} from the dropdown list`}
                  />
                )}
              />
            </>
          );
        }}
      />
    );
  } else if (isMultiValued && isEnumType) {
    return (
      <Controller
        control={control}
        name={`businessMetadata.${index}.value` as const}
        key={`autocomplete-${name}`}
        defaultValue={[]}
        render={({ field: { onChange, value }, fieldState: { error } }) => {
          const selectedValues = normalizeMultiEnumValue(value, options);

          return (
            <>
              <Autocomplete
                size="small"
                multiple
                disableCloseOnSelect
                className="bm-attribute-value-field"
                onChange={(_event, selectedOptions) => {
                  onChange(selectedOptions);
                }}
                sx={fieldWidthSx}
                value={selectedValues}
                filterSelectedOptions
                getOptionLabel={getEnumOptionLabel}
                isOptionEqualToValue={areEnumOptionsEqual}
                options={options}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    error={!!error}
                    size="small"
                    InputProps={{
                      ...params.InputProps
                    }}
                    placeholder="Select enum values"
                  />
                )}
              />
            </>
          );
        }}
      />
    );
  } else if (isEnumType && !isMultiValued) {
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
                className="bm-attribute-value-field"
                size="small"
                id={enumSelectId}
                inputProps={{ "aria-label": `Select ${name} enum value` }}
                displayEmpty
                value={!isEmpty(value) ? value : ""}
                onChange={(e: SelectChangeEvent) => {
                  onChange(e.target.value);
                }}
                renderValue={(selected) => {
                  if (!selected) {
                    return <em>--Select Value--</em>;
                  }

                  return selected;
                }}
              >
                <MenuItem value="">
                  <em>--Select Value--</em>
                </MenuItem>
                {options.map((option: EnumOption) => (
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
              className="bm-attribute-value-field"
              onChange={onChange}
              value={value}
              variant="outlined"
              size="small"
              type={typeName === "string" ? "text" : "number"}
              placeholder={name}
            />
          </>
        )}
      />
    );
  }
};

export default BMAttributesFields;
