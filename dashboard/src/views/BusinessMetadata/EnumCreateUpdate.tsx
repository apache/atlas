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

import { CustomButton } from "@components/muiComponents";
import { useAppSelector } from "@hooks/reducerHook";
import {
  Button,
  CircularProgress,
  FilterOptionsState,
  Grid,
  InputLabel,
  Stack,
  TextField
} from "@mui/material";
import { customSortBy, isEmpty } from "@utils/Utils";
import Autocomplete, { createFilterOptions } from "@mui/material/Autocomplete";
import { Controller } from "react-hook-form";

const filter = createFilterOptions<any>();

const EnumCreateUpdate = ({
  control,
  handleSubmit,
  setValue,
  isSubmitting,
  watch,
  reset,
  onSubmit,
  isDirty
}: any) => {
  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const { enumDefs } = enumObj?.data || {};
  let enumTypes = !isEmpty(enumDefs)
    ? enumDefs.map((obj: { name: any }) => {
        return obj.name;
      })
    : [];

  const selectedEnum = watch("enumType" as any);
  let selectedEnumObj = !isEmpty(enumDefs)
    ? enumDefs.find((obj: { name: any }) => {
        return obj.name == selectedEnum;
      })
    : {};
  let selectedEnumValues = !isEmpty(selectedEnumObj)
    ? selectedEnumObj?.elementDefs
    : [];

  let enumTypeOptions = [...selectedEnumValues];

  const filterOptions = (
    options: any[],
    { inputValue }: FilterOptionsState<any>,
    selectedValues: { value: string }[]
  ) => {
    const lowerInputValue = inputValue ? inputValue?.toLowerCase() : "";
    const filteredOptions: any[] = [];
    const isExisting = options.some((option) => inputValue === option?.value);

    let selectedEnumValues = !isEmpty(selectedValues)
      ? selectedValues.map((obj: { value: string }) => {
          return obj?.value?.toLowerCase();
        })
      : [];

    options.forEach((option: { value: string }) => {
      const labelLower = option.value.toLowerCase();
      if (
        labelLower.includes(lowerInputValue) &&
        !selectedEnumValues.includes(labelLower)
      ) {
        filteredOptions.push(option);
      }
    });

    if (!isEmpty(lowerInputValue) && !isExisting) {
      filteredOptions.push({
        value: inputValue,
        label: inputValue
      });
    }

    return filteredOptions;
  };

  return (
    <>
      <form onSubmit={handleSubmit(onSubmit)}>
        <Stack gap={2}>
          <Controller
            control={control}
            name={`enumType` as const}
            data-cy={`enumType`}
            rules={{
              required: true
            }}
            defaultValue={null}
            render={({
              field: { value, onChange, onBlur },
              fieldState: { error }
            }) => {
              return (
                <>
                  <Grid
                    container
                    columnSpacing={{ xs: 1, sm: 2, md: 2 }}
                    marginBottom="1.5rem"
                    alignItems="center"
                  >
                    <Grid item md={3} textAlign="right">
                      <InputLabel required>Enum Name</InputLabel>
                    </Grid>
                    <Grid item md={7}>
                      <Stack direction="row" gap={1}>
                        <Autocomplete
                          freeSolo
                          sx={{ flex: "1" }}
                          size="small"
                          value={value}
                          onChange={(_event, newValue) => {
                            if (!isEmpty(newValue.label)) {
                              onChange(newValue.value);
                            } else {
                              onChange(newValue);
                            }

                            if (newValue) {
                              let selectedEnumObj = enumDefs.find(
                                (obj: { name: any }) => {
                                  return obj.name == newValue;
                                }
                              );
                              let selectedEnumValues = !isEmpty(selectedEnumObj)
                                ? selectedEnumObj?.elementDefs
                                : [];

                              let enumTypeOptions = [...selectedEnumValues];
                              setValue("enumValues", enumTypeOptions);
                            } else {
                              setValue("enumValues", []);
                            }
                          }}
                          filterOptions={(options, params) => {
                            const filtered = filter(options, params);

                            const { inputValue } = params;
                            const isExisting = options.some(
                              (option) => inputValue === option
                            );
                            if (inputValue !== "" && !isExisting) {
                              filtered.push({
                                value: inputValue,
                                label: `Create new enum "${inputValue}"`
                              });
                            }

                            return filtered;
                          }}
                          filterSelectedOptions
                          onBlur={onBlur}
                          options={
                            !isEmpty(enumTypes)
                              ? enumTypes.sort().map((option: any) => option)
                              : []
                          }
                          className="advanced-search-autocomplete"
                          renderInput={(params) => (
                            <TextField
                              {...params}
                              error={!!error}
                              fullWidth
                              label="Select Enum name"
                              InputProps={{
                                style: {
                                  padding: "0px 32px 0px 4px",
                                  height: "36px",
                                  lineHeight: "1.2"
                                },
                                ...params.InputProps,
                                type: "search"
                              }}
                            />
                          )}
                        />
                      </Stack>
                    </Grid>{" "}
                  </Grid>
                </>
              );
            }}
          />

          {!isEmpty(selectedEnum) && (
            <Controller
              control={control}
              name={`enumValues` as const}
              data-cy={`enumValues`}
              rules={{
                required: true
              }}
              render={({
                field: { value, onChange, onBlur },
                fieldState: { error }
              }) => {
                return (
                  <>
                    <Grid
                      container
                      columnSpacing={{ xs: 1, sm: 2, md: 2 }}
                      marginBottom="1.5rem"
                      alignItems="center"
                    >
                      <Grid item md={3} textAlign="right">
                        <InputLabel required>Enum Value</InputLabel>
                      </Grid>
                      <Grid item md={7}>
                        <Autocomplete
                          freeSolo
                          size="small"
                          multiple={true}
                          clearIcon={null}
                          value={!isEmpty(value) ? value : []}
                          onChange={(_event, newValue) => {
                            const filteredNewValue = Array.isArray(newValue)
                              ? newValue.filter(
                                  (option, idx, arr) =>
                                    arr.findIndex(
                                      (o) =>
                                        o.value &&
                                        option.value &&
                                        o.value.toLowerCase() ===
                                          option.value.toLowerCase()
                                    ) === idx
                                )
                              : newValue;
                            onChange(filteredNewValue);
                          }}
                          getOptionLabel={(option) => option.value}
                          filterSelectedOptions={false}
                          filterOptions={(options, params) =>
                            filterOptions(options, params, value)
                          }
                          data-cy="enumValueSelector"
                          options={
                            !isEmpty(enumTypeOptions)
                              ? customSortBy(enumTypeOptions, ["value"])
                              : []
                          }
                          onBlur={onBlur}
                          className="advanced-search-autocomplete"
                          renderInput={(params) => (
                            <TextField
                              {...params}
                              error={!!error}
                              fullWidth
                              label="Select Enum value"
                              InputProps={{
                                ...params.InputProps,
                                type: "search"
                              }}
                            />
                          )}
                        />
                      </Grid>
                    </Grid>
                  </>
                );
              }}
            />
          )}
          {!isEmpty(onSubmit) && (
            <Stack direction="row" gap={2} justifyContent="center">
              <CustomButton
                variant="outlined"
                size="small"
                data-cy="clearButton"
                color="primary"
                onClick={(_e: Event) => {
                  reset({ enumType: "", enumValues: [] });
                }}
                disabled={
                  isSubmitting || (isDirty != undefined ? !isDirty : false)
                }
                sx={{
                  ...(isSubmitting && {
                    "&.Mui-disabled": {
                      pointerEvents: "unset",
                      cursor: "not-allowed"
                    }
                  })
                }}
              >
                Clear
              </CustomButton>
              <Button
                type="submit"
                size="small"
                data-cy="updateButton"
                disabled={
                  isSubmitting || (isDirty != undefined ? !isDirty : false)
                }
                sx={{
                  ...(isSubmitting && {
                    "&.Mui-disabled": {
                      pointerEvents: "unset",
                      cursor: "not-allowed"
                    }
                  })
                }}
                startIcon={
                  isSubmitting && (
                    <CircularProgress
                      sx={{ color: "white", fontWeight: "600" }}
                      size="20px"
                    />
                  )
                }
                variant="contained"
                color="primary"
              >
                Update
              </Button>
            </Stack>
          )}
        </Stack>
      </form>
    </>
  );
};

export default EnumCreateUpdate;
