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

import { CustomButton, LightTooltip } from "@components/muiComponents";
import {
  Autocomplete,
  TextField,
  IconButton,
  InputLabel,
  Stack,
  Card,
  CardContent
} from "@mui/material";
import { Controller, useFieldArray } from "react-hook-form";
import AddIcon from "@mui/icons-material/Add";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { DemoContainer } from "@mui/x-date-pickers/internals/demo";
import { AdapterMoment } from "@mui/x-date-pickers/AdapterMoment";
import moment from "moment";
import ClearOutlinedIcon from "@mui/icons-material/ClearOutlined";
import { DateTimePicker } from "@mui/x-date-pickers/DateTimePicker";
import { useAppSelector } from "@hooks/reducerHook";

const AddValidityPeriod = (props: { control: any }) => {
  const { control } = props;
  const { sessionObj } = useAppSelector((state) => state.session);
  const { fields, append, remove } = useFieldArray({
    name: "validityPeriod",
    control
  });
  // const tomorrow = moment();

  const { timezones = [] } = sessionObj.data || {};
  const timeZonesList = timezones.map((obj: string) => ({
    label: obj,
    value: obj
  }));

  return (
    <>
      <Card variant="outlined" sx={{ marginBottom: "2rem" }}>
        <CardContent>
          <Stack gap="1rem">
            <LightTooltip title={"Add Validity Period"}>
              <CustomButton
                sx={{
                  alignSelf: "flex-end"
                }}
                variant="outlined"
                size="small"
                onClick={(_e: any) => {
                  append({
                    validityPeriod: ""
                  });
                }}
                startIcon={<AddIcon />}
              >
                Add Validity Period
              </CustomButton>
            </LightTooltip>
            {fields.map((field, index) => {
              return (
                <Stack
                  // position="relative"
                  gap="1rem"
                  key={field.id}
                  direction="row"
                  alignItems="center"
                >
                  <Controller
                    control={control}
                    name={`validityPeriod.${index}.startTime` as const}
                    rules={{
                      required: true
                    }}
                    render={({ field: { onChange, value, ref } }) => (
                      <>
                        <Stack>
                          <InputLabel required={true}>Start Time</InputLabel>
                          <LocalizationProvider dateAdapter={AdapterMoment}>
                            <DemoContainer
                              components={["DatePicker"]}
                              sx={{
                                overflow: "hidden"
                                // marginBottom: "1rem"
                              }}
                            >
                              <DateTimePicker
                                views={[
                                  "year",
                                  "day",
                                  "hours",
                                  "minutes",
                                  "seconds"
                                ]}
                                timeSteps={{ hours: 1, minutes: 1, seconds: 1 }}
                                // minDate={tomorrow}
                                slotProps={{
                                  textField: {
                                    size: "small",
                                    sx: {
                                      "& .MuiInputBase-root": {
                                        height: "34px",
                                        alignItems: "center",
                                        position: "relative"
                                      },
                                      "& .MuiInputBase-input": {
                                        padding: "10px 14px"
                                      }
                                    }
                                  }
                                  // openPickerButton: { size: "small" }
                                  // openPickerIcon: () => (
                                  //   <CalendarMonthOutlinedIcon fontSize="inherit" />
                                  // )
                                }}
                                value={value ? moment(value) : null}
                                inputRef={ref}
                                onChange={(date) => {
                                  onChange(date ? date.toISOString() : null);
                                }}
                              />
                            </DemoContainer>
                          </LocalizationProvider>
                        </Stack>
                      </>
                    )}
                  />
                  <Controller
                    control={control}
                    name={`validityPeriod.${index}.endTime` as const}
                    rules={{
                      required: true
                    }}
                    render={({ field: { onChange, value, ref } }) => (
                      <>
                        <Stack>
                          {" "}
                          <InputLabel required={true}>End Time</InputLabel>
                          <LocalizationProvider dateAdapter={AdapterMoment}>
                            <DemoContainer
                              components={["DatePicker"]}
                              sx={{
                                overflow: "hidden"
                                // marginBottom: "1rem"
                              }}
                            >
                              <DateTimePicker
                                views={[
                                  "year",
                                  "day",
                                  "hours",
                                  "minutes",
                                  "seconds"
                                ]}
                                // minDate={tomorrow}
                                slotProps={{
                                  textField: {
                                    size: "small",
                                    sx: {
                                      // height: "34px", // Set the height of the TextField
                                      "& .MuiInputBase-root": {
                                        height: "34px",
                                        alignItems: "center"
                                      },
                                      "& .MuiInputBase-input": {
                                        padding: "10px 14px" // Adjust input padding if necessary
                                      }
                                    }
                                  }
                                  // openPickerButton: { size: "small" }
                                  // openPickerIcon: () => (
                                  //   <CalendarMonthOutlinedIcon fontSize="inherit" />
                                  // )
                                }}
                                timeSteps={{ hours: 1, minutes: 1, seconds: 1 }}
                                value={value ? moment(value) : null}
                                inputRef={ref}
                                onChange={(date) => {
                                  onChange(date ? date.toISOString() : null);
                                }}
                              />
                            </DemoContainer>
                          </LocalizationProvider>
                        </Stack>{" "}
                      </>
                    )}
                  />

                  <Controller
                    control={control}
                    rules={{
                      required: true
                    }}
                    name={`validityPeriod.${index}.timeZone` as const}
                    data-cy="addTagOptions"
                    key={`validityPeriod.${index}.timeZone`}
                    render={({
                      field: { onChange, value },
                      fieldState: { error }
                    }) => {
                      return (
                        <Stack flex="1">
                          <InputLabel required={true}>TimeZone</InputLabel>
                          <Autocomplete
                            value={value}
                            onChange={(_event, item) => onChange(item)}
                            sx={{
                              paddingTop: "8px",
                              overflow: "hidden",
                              // marginBottom: "1rem",
                              "& .MuiInputBase-root": {
                                height: "34px",
                                alignItems: "center"
                              },
                              "& .MuiInputBase-input": {
                                padding: "10px 14px"
                              }
                            }}
                            id="timeZone"
                            size="small"
                            filterSelectedOptions
                            getOptionLabel={(option: { label: string }) =>
                              option.label
                            }
                            isOptionEqualToValue={(option, value) =>
                              option.label === value.label
                            }
                            options={timeZonesList}
                            autoHighlight
                            disableClearable
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                error={!!error}
                                size="small"
                                InputProps={{
                                  ...params.InputProps
                                }}
                                variant="outlined"
                                placeholder="Select TimeZone"
                                helperText={
                                  error ? "This field is required" : ""
                                }
                              />
                            )}
                          />
                        </Stack>
                      );
                    }}
                  />

                  <IconButton
                    aria-label="back"
                    color="error"
                    size="small"
                    sx={{
                      display: "inline-flex",
                      position: "relative",
                      padding: "4px",
                      marginLeft: "4px",
                      marginTop: "1.5rem !important"
                    }}
                    onClick={() => remove(index)}
                  >
                    <ClearOutlinedIcon fontSize="small" />
                  </IconButton>
                </Stack>
              );
            })}
          </Stack>
        </CardContent>
      </Card>
    </>
  );
};

export default AddValidityPeriod;
