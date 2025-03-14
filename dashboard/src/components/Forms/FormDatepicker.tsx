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

import { InputLabel, Typography } from "@mui/material";
import { Capitalize, isEmpty } from "@utils/Utils";
import { Controller } from "react-hook-form";
import { DemoContainer } from "@mui/x-date-pickers/internals/demo";
import { AdapterMoment } from "@mui/x-date-pickers/AdapterMoment";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import { LightTooltip } from "@components/muiComponents";
import moment from "moment-timezone";

const FormDatepicker = ({ data, control, fieldName }: any) => {
  const { name, isOptional, typeName } = data;
  // const tomorrow = name == "modifiedTime" ? moment() : undefined;
  return (
    <Controller
      name={!isEmpty(fieldName) ? `${fieldName}.${name}` : name}
      control={control}
      rules={{
        required: isOptional ? false : true
      }}
      key={name}
      render={({ field: { onChange, value, ref } }) => (
        <>
          <div className="form-fields">
            <InputLabel
              className="form-textfield-label"
              required={isOptional ? false : true}
            >
              {Capitalize(name)}
            </InputLabel>
            <LightTooltip title={`Data Type: (${typeName})`}>
              <Typography
                color="#666666"
                overflow="hidden"
                textOverflow="ellipsis"
                maxWidth="160px"
                fontSize={14}
              >{`(${typeName})`}</Typography>
            </LightTooltip>
          </div>
          <div className="w-100">
            <LocalizationProvider dateAdapter={AdapterMoment}>
              <DemoContainer
                components={["DatePicker"]}
                sx={{
                  marginTop: "8px",
                  overflow: "hidden",
                  paddingTop: "0 ",
                  marginBottom: "1.5rem"
                }}
              >
                <DatePicker
                  // minDate={tomorrow}
                  sx={{
                    "& .MuiInputBase-input": {
                      height: "1.375em",
                      padding: "6px 14px",
                      backgroundColor: "white"
                    }
                  }}
                  slotProps={{
                    textField: {
                      size: "small"
                    }
                  }}
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
};

export default FormDatepicker;
