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
import { Autocomplete, TextField } from "@mui/material";
import { AdapterMoment } from "@mui/x-date-pickers/AdapterMoment";
import { DateTimePicker } from "@mui/x-date-pickers/DateTimePicker";
import { DemoContainer } from "@mui/x-date-pickers/internals/demo";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { timeRangeOptions } from "@utils/Enum";
import { isEmpty } from "@utils/Utils";
import moment from "moment";
// import moment from "moment";
import { useState } from "react";
import DatePicker from "react-datepicker";
import { ValueEditorProps, ValueEditor } from "react-querybuilder";

export const TypeCustomValueEditor = (props: ValueEditorProps) => {
  // const { field, operator, value, handleOnChange } = props;

  const { classificationData }: any = useAppSelector(
    (state: any) => state.classification
  );
  const { typeHeaderData }: any = useAppSelector(
    (state: any) => state.typeHeader
  );
  const { classificationDefs } = classificationData || {};
  const [selectedValue, setSelectedValue] = useState(props.value);
  const [selectedTypenameValue, setSelectedTypenameValue] = useState(
    props.value
  );
  const [selectedDateValue, setSelectedDateValue] = useState(
    moment(props.value).isValid() ? moment(props.value) : null
  );
  const [_customRange, setCustomRange] = useState<[Date | null, Date | null]>([
    null,
    null
  ]);
  const [showDatePicker, setShowDatePicker] = useState(false);
  const [dateRange, setDateRange] = useState<any>([null, null]);

  const [startDate, endDate] = dateRange;

  const handleTagChange = (_event: any, newValue: any) => {
    setSelectedValue(newValue);
    props.handleOnChange(newValue);
  };

  const handleTypeNameChange = (_event: any, newValue: any) => {
    setSelectedTypenameValue(newValue);
    props.handleOnChange(newValue);
  };

  // const handleDateChange = (date: any) => {
  //   setSelectedDateValue(date ? date.toISOString() : undefined);
  //   props.handleOnChange(date ? date.toISOString() : null);
  // };

  let tagOptions = !isEmpty(classificationDefs)
    ? classificationDefs
        .map((obj: { name: any }) => {
          return obj.name;
        })
        .sort()
    : [];

  let typeData = !isEmpty(typeHeaderData)
    ? typeHeaderData.filter((obj: { category: string }) => {
        if (obj.category == "ENTITY") {
          return obj;
        }
      })
    : [];

  let entityOptions = !isEmpty(typeData)
    ? typeData
        .map((obj: { name: any }) => {
          return obj.name;
        })
        .sort()
    : [];

  if (
    props.field == "__classificationNames" ||
    props.field === "__propagatedClassificationNames"
  ) {
    return (
      <Autocomplete
        value={selectedValue}
        onChange={handleTagChange}
        options={tagOptions}
        disableClearable
        clearOnEscape={false}
        size="small"
        renderInput={(params) => (
          <TextField {...params} variant="outlined" size="small" fullWidth />
        )}
        sx={{ minWidth: "200px" }}
      />
    );
  }

  if (props.field == "__typeName") {
    return (
      <Autocomplete
        value={selectedTypenameValue}
        onChange={handleTypeNameChange}
        options={entityOptions}
        getOptionLabel={(option) => option}
        disableClearable
        clearOnEscape={false}
        size="small"
        renderInput={(params) => (
          <TextField {...params} variant="outlined" size="small" fullWidth />
        )}
        sx={{ minWidth: "200px" }}
      />
    );
  }

  if (props.operator === "is_null" || props.operator === "not_null") {
    return;
  }

  const handleTimeRangeChange = (selectedValue: string) => {
    props.handleOnChange(selectedValue);
    if (selectedValue === "custom_range") {
      setShowDatePicker(true);
    } else {
      setShowDatePicker(false);
      setCustomRange([null, null]);
    }
  };

  if (props.inputType == "datetime-local" && props.operator === "TIME_RANGE") {
    return (
      <div>
        <select
          value={props.value}
          className="rule-operators"
          onChange={(e) => handleTimeRangeChange(e.target.value)}
        >
          <option value="">Select Time Range</option>
          {timeRangeOptions.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
        {showDatePicker && (
          <DatePicker
            selectsRange
            showTimeSelect
            showPopperArrow={false}
            popperProps={{ strategy: "fixed" }}
            showYearDropdown
            showMonthDropdown
            startDate={
              moment(startDate).isValid()
                ? moment(startDate).toDate()
                : undefined
            }
            endDate={
              moment(endDate).isValid() ? moment(endDate).toDate() : undefined
            }
            onChange={(update: [Date | null, Date | null] | null) => {
              setDateRange(update);
              props.handleOnChange(
                update
                  ? update.map((date) => date?.toISOString() || "").join(",")
                  : ""
              );
            }}
            isClearable={true}
          />
        )}
      </div>
    );
  }

  if (props.inputType == "datetime-local") {
    return (
      <LocalizationProvider dateAdapter={AdapterMoment}>
        <DemoContainer
          components={["DatePicker"]}
          sx={{
            overflow: "hidden",
            marginBottom: "0.5rem"
          }}
        >
          <DateTimePicker
            views={["year", "day", "hours", "minutes", "seconds"]}
            slotProps={{
              textField: {
                size: "small",
                sx: {
                  "& .MuiInputBase-root": {
                    height: "34px",
                    alignItems: "center"
                  },
                  "& .MuiInputBase-input": {
                    padding: "10px 14px"
                  }
                }
              }
            }}
            onChange={(value: moment.Moment | null) => {
              // setSelectedDateValue(value ? value : moment());
              // props.handleOnChange(value ? value.toISOString() : null);
              const isoString = value
                ? value.format("MM/DD/YYYY hh:mm:ss A")
                : null;
              setSelectedDateValue(value);
              props.handleOnChange(isoString);
            }}
            timeSteps={{ hours: 1, minutes: 1, seconds: 1 }}
            // value={props.value ? moment(props.value) : moment()}
            value={selectedDateValue}
            defaultValue={moment()}
            // onChange={(date) => {
            //   props.handleOnChange(date ? date.toISOString() : null);
            // }}
            // onChange={handleDateChange}
          />
        </DemoContainer>
      </LocalizationProvider>
    );
  }

  if (props.inputType == "datetime-local") {
    return (
      <LocalizationProvider dateAdapter={AdapterMoment}>
        <DemoContainer
          components={["DatePicker"]}
          sx={{
            overflow: "hidden",
            marginBottom: "0.5rem"
          }}
        >
          <DateTimePicker
            views={["year", "day", "hours", "minutes", "seconds"]}
            slotProps={{
              textField: {
                size: "small",
                sx: {
                  "& .MuiInputBase-root": {
                    height: "34px",
                    alignItems: "center"
                  },
                  "& .MuiInputBase-input": {
                    padding: "10px 14px"
                  }
                }
              }
            }}
            onChange={(value: moment.Moment | null) => {
              // setSelectedDateValue(value ? value : moment());
              // props.handleOnChange(value ? value.toISOString() : null);
              const isoString = value
                ? value.format("MM/DD/YYYY hh:mm:ss A")
                : null;
              setSelectedDateValue(value);
              props.handleOnChange(isoString);
            }}
            timeSteps={{ hours: 1, minutes: 1, seconds: 1 }}
            // value={props.value ? moment(props.value) : moment()}
            value={selectedDateValue}
            defaultValue={moment()}
            // onChange={(date) => {
            //   props.handleOnChange(date ? date.toISOString() : null);
            // }}
            // onChange={handleDateChange}
          />
        </DemoContainer>
      </LocalizationProvider>
    );
  }
  return <ValueEditor {...props} />;
};
