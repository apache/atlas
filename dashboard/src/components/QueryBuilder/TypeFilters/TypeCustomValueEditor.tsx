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
import { Autocomplete, TextField } from "@mui/material";
import { timeRangeOptions } from "@utils/Enum";
import { isEmpty } from "@utils/Utils";
import moment from "moment";
import { useState } from "react";
import { ValueEditorProps, ValueEditor } from "react-querybuilder";

export const TypeCustomValueEditor = (props: ValueEditorProps) => {
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
  const [dateRange, setDateRange] = useState<[Date | null, Date | null]>([
    null,
    null
  ]);

  const [startDate, endDate] = Array.isArray(dateRange)
    ? dateRange
    : [null, null];

  const handleTagChange = (_event: any, newValue: any) => {
    setSelectedValue(newValue);
    props.handleOnChange(newValue);
  };

  const handleTypeNameChange = (_event: any, newValue: any) => {
    setSelectedTypenameValue(newValue);
    props.handleOnChange(newValue);
  };

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
    if (selectedValue === "CUSTOM_RANGE") {
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
          <CustomDatepicker
            selectsRange
            timeIntervals={1}
            timeFormat="hh:mm aa"
            timeCaption="Time"
            shoowTimeInput
            showPopperArrow={false}
            popperProps={{ strategy: "fixed" }}
            startDate={
              moment(startDate).isValid()
                ? moment(startDate).toDate()
                : undefined
            }
            endDate={
              moment(endDate).isValid() ? moment(endDate).toDate() : undefined
            }
            onChange={(update: [Date | null, Date | null] | null) => {
              const safeUpdate: [Date | null, Date | null] = update ?? [
                null,
                null
              ];
              setDateRange(safeUpdate);
              if (safeUpdate[0] && safeUpdate[1]) {
                const startEpoch = moment(safeUpdate[0]).valueOf();
                const endEpoch = moment(safeUpdate[1]).valueOf();
                props.handleOnChange(`${startEpoch},${endEpoch}`);
              } else {
                props.handleOnChange("");
              }
            }}
            selected={undefined}
          />
        )}
      </div>
    );
  }

  if (props.inputType == "datetime-local") {
    if (!selectedDateValue) {
      const now = moment();
      setSelectedDateValue(now);
      props.handleOnChange(now.valueOf());
    }
    return (
      <CustomDatepicker
        timeIntervals={1}
        timeFormat="hh:mm aa"
        timeCaption="Time"
        showPopperArrow={false}
        popperProps={{ strategy: "fixed" }}
        selected={
          selectedDateValue && moment(selectedDateValue).isValid()
            ? moment(selectedDateValue).toDate()
            : moment().toDate()
        }
        onChange={(date: Date | null) => {
          const value = date ? moment(date) : moment();
          setSelectedDateValue(value);
          props.handleOnChange(value.valueOf());
        }}
        dateFormat="MM/dd/yyyy h:mm:ss aa"
        showTimeInput
      />
    );
  }

  return <ValueEditor {...props} />;
};
