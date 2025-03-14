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

import { CustomButton } from "@components/muiComponents";
import { useAppSelector } from "@hooks/reducerHook";
import {
  Autocomplete,
  Button,
  CircularProgress,
  FilterOptionsState,
  Grid,
  InputLabel,
  Stack,
  TextField
} from "@mui/material";
import { customSortBy, isEmpty } from "@utils/Utils";
import { Controller } from "react-hook-form";

const EnumCreateUpdate = ({
  control,
  handleSubmit,
  setValue,
  isSubmitting,
  watch,
  reset,
  onSubmit
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
    const lowerInputValue = inputValue ? inputValue.toLowerCase() : "";
    const filteredOptions: any[] = [];

    let selectedEnumValues = !isEmpty(selectedValues)
      ? selectedValues.map((obj: { value: string }) => {
          return obj.value.toLowerCase();
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
                            onChange(newValue);

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
                              // helperText={error ? "This field is required" : ""}
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
                          value={!isEmpty(value) ? value : []}
                          onChange={(_event, newValue) => {
                            let newValues = newValue;
                            onChange(newValues);
                          }}
                          getOptionLabel={(option) => option.value}
                          filterSelectedOptions
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
                              // helperText={error ? "This field is required" : ""}
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
                color="primary"
                onClick={(_e: Event) => {
                  reset({ enumType: "", enumValues: [] });
                }}
              >
                Clear
              </CustomButton>
              <Button
                type="submit"
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
