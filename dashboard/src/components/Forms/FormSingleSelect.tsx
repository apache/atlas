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

import { Typography } from "@mui/material";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import Select, { SelectChangeEvent } from "@mui/material/Select";
import { Capitalize, isEmpty } from "@utils/Utils";
import { Controller } from "react-hook-form";

const FormSingleSelect = ({
  data,
  control,
  optionsList,
  typeName,
  fieldName
}: any) => {
  const { name, isOptional } = data;

  return (
    <Controller
      name={!isEmpty(fieldName) ? `${fieldName}.${name}` : name}
      control={control}
      rules={{
        required: isOptional ? false : true
      }}
      key={name}
      defaultValue={null}
      render={({ field: { onChange, value } }) => (
        <>
          <div className="form-fields">
            <InputLabel
              className="form-textfield-label"
              required={isOptional ? false : true}
            >
              {Capitalize(name)}
            </InputLabel>

            <Typography
              color="#666666"
              textOverflow="ellipsis"
              overflow="hidden"
              maxWidth="160px"
              fontSize={14}
            >{`(${typeName})`}</Typography>
          </div>
          <div style={{ width: "100%" }} className="form-textfield">
            <Select
              fullWidth
              size="small"
              id="demo-select-small "
              value={value}
              onChange={(e: SelectChangeEvent) => {
                onChange(e.target.value);
              }}
              renderValue={(selected) => {
                if (selected.length === 0) {
                  return <em>--Select {typeName}--</em>;
                }

                return selected;
              }}
            >
              <MenuItem value="">
                <em>--Select {typeName}--</em>
              </MenuItem>
              {optionsList?.map((obj: { value: string }) => {
                return <MenuItem value={obj.value}>{obj.value}</MenuItem>;
              })}
            </Select>
          </div>
        </>
      )}
    />
  );
};

export default FormSingleSelect;
