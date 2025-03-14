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
import { isEmpty } from "@utils/Utils";
import moment from "moment";
import { useState } from "react";
import { ValueEditorProps, ValueEditor } from "react-querybuilder";

export const TagCustomValueEditor = (props: ValueEditorProps) => {
  const { typeHeaderData }: any = useAppSelector(
    (state: any) => state.typeHeader
  );
  const [selectedTypenameValue, setSelectedTypenameValue] = useState(
    props.value
  );
  const [selectedDateValue, setSelectedDateValue] = useState(
    moment(props.value).isValid() ? moment(props.value) : null
  );
  const handleTypeNameChange = (_event: any, newValue: any) => {
    setSelectedTypenameValue(newValue);
    props.handleOnChange(newValue);
  };

  // const handleDateChange = (date: any) => {
  //   const isoString = date ? date.toISOString() : null;
  //   setSelectedDateValue(date);
  //   props.handleOnChange(isoString);
  // };

  let tagData = typeHeaderData.filter((obj: { category: string }) => {
    if (obj.category == "CLASSIFICATION") {
      return obj;
    }
  });

  let tagOptions = !isEmpty(tagData)
    ? tagData
        .map((obj: { name: any }) => {
          return obj.name;
        })
        .sort()
    : [];

  if (props.field == "__typeName") {
    return (
      <Autocomplete
        value={selectedTypenameValue}
        className="query-field-value-autocomplete"
        onChange={handleTypeNameChange}
        options={tagOptions}
        getOptionLabel={(option) => option}
        disableClearable
        clearOnEscape={false}
        size="small"
        renderInput={(params) => (
          <TextField
            {...params}
            variant="outlined"
            size="small"
            sx={{
              background: "white"
            }}
          />
        )}
        sx={{
          width: "50%"
        }}
      />
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
