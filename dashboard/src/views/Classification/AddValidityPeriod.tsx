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
import moment from "moment";
import ClearOutlinedIcon from "@mui/icons-material/ClearOutlined";
import { useAppSelector } from "@hooks/reducerHook";

// Start Date Picker Component
import { Control } from "react-hook-form";
import CustomDatepicker from "@components/DatePicker/CustomDatePicker";

interface StartDatePickerProps {
  control: Control<any>;
  name: string;
}

const StartDatePicker = ({ control, name }: StartDatePickerProps) => (
  <Controller
    control={control}
    name={name}
    rules={{ required: true }}
    render={({ field: { onChange, value, ref } }) => (
      <Stack minWidth={210}>
        <InputLabel required={true}>Start Time</InputLabel>
        <CustomDatepicker
          timeIntervals={1}
          timeFormat="hh:mm aa"
          timeCaption="Time"
          showPopperArrow={false}
          popperProps={{ strategy: "fixed" }}
          selected={
            value && moment(value).isValid()
              ? moment(value).toDate()
              : moment().toDate()
          }
          ref={ref}
          onChange={(date: { toISOString: () => any }) => {
            onChange(date ? date.toISOString() : null);
          }}
          showTimeInput
          dateFormat="MM/dd/yyyy h:mm:ss aa"
        />
      </Stack>
    )}
  />
);

// End Date Picker Component
interface EndDatePickerProps {
  control: Control<any>;
  name: string;
}

const EndDatePicker = ({ control, name }: EndDatePickerProps) => (
  <Controller
    control={control}
    name={name}
    rules={{ required: true }}
    render={({ field: { onChange, value, ref } }) => (
      <Stack minWidth={210}>
        <InputLabel required={true}>End Time</InputLabel>
        <CustomDatepicker
          timeIntervals={1}
          timeFormat="hh:mm aa"
          timeCaption="Time"
          showPopperArrow={false}
          popperProps={{ strategy: "fixed" }}
          selected={
            value && moment(value).isValid()
              ? moment(value).toDate()
              : moment().toDate()
          }
          ref={ref}
          onChange={(date: { toISOString: () => any }) => {
            onChange(date ? date.toISOString() : null);
          }}
          showTimeInput
          dateFormat="MM/dd/yyyy h:mm:ss aa"
        />
      </Stack>
    )}
  />
);

// TimeZone Autocomplete Component
interface TimeZoneOption {
  label: string;
  value: string;
}

interface TimeZoneAutocompleteProps {
  control: Control<any>;
  name: string;
  options: TimeZoneOption[];
}

const TimeZoneAutocomplete = ({
  control,
  name,
  options
}: TimeZoneAutocompleteProps) => (
  <Controller
    control={control}
    rules={{ required: true }}
    name={name}
    render={({ field: { onChange, value }, fieldState: { error } }) => (
      <Stack minWidth={210}>
        <InputLabel required={true}>TimeZone</InputLabel>
        <Autocomplete
          value={value}
          onChange={(_event, item) => onChange(item)}
          id="timeZone"
          size="small"
          filterSelectedOptions
          getOptionLabel={(option) => option?.label || ""}
          isOptionEqualToValue={(option, value) => option.label === value.label}
          options={options}
          autoHighlight
          disableClearable
          renderInput={(params) => (
            <TextField
              {...params}
              error={!!error}
              size="small"
              variant="outlined"
              placeholder="Select TimeZone"
              helperText={error ? "This field is required" : ""}
              sx={{
                height: "34px",
                "& .MuiInputBase-root": { height: "34px" }
              }}
            />
          )}
        />
      </Stack>
    )}
  />
);

const AddValidityPeriod = (props: { control: any }) => {
  const { control } = props;
  const { sessionObj } = useAppSelector((state) => state.session);
  const { fields, append, remove } = useFieldArray({
    name: "validityPeriod",
    control
  });

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
            {fields.map((field, index) => (
              <Stack
                gap="1rem"
                key={field.id}
                direction="row"
                alignItems="center"
                sx={{ minHeight: "56px" }}
              >
                <StartDatePicker
                  control={control}
                  name={`validityPeriod.${index}.startTime`}
                />
                <EndDatePicker
                  control={control}
                  name={`validityPeriod.${index}.endTime`}
                />
                <TimeZoneAutocomplete
                  control={control}
                  name={`validityPeriod.${index}.timeZone`}
                  options={timeZonesList}
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
            ))}
          </Stack>
        </CardContent>
      </Card>
    </>
  );
};

export default AddValidityPeriod;
