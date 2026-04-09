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
import { LightTooltip } from "@components/muiComponents";
import moment from "moment-timezone";
import { useEffect, useState } from "react";
import CustomDatepicker from "@components/DatePicker/CustomDatePicker";

const FormDatepicker = ({ data, control, fieldName }: any) => {
  const { name, isOptional, typeName } = data;
  const [cleared, setCleared] = useState<boolean>(false);

  useEffect(() => {
    if (cleared) {
      const timeout = setTimeout(() => {
        setCleared(false);
      }, 1500);

      return () => clearTimeout(timeout);
    }
    return () => {};
  }, [cleared]);

  return (
    <Controller
      name={!isEmpty(fieldName) ? `${fieldName}.${name}` : name}
      control={control}
      rules={{
        required: isOptional ? false : true
      }}
      key={name}
      render={({ field: { onChange, value, ref } }) => {
        if (!value) {
          const defaultDate = new Date();
          onChange(defaultDate.toISOString());
        }
        return (
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
            </div>
          </>
        );
      }}
    />
  );
};

export default FormDatepicker;
