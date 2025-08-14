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

import { LightTooltip } from "@components/muiComponents";
import { InputLabel, TextField, Typography } from "@mui/material";
import Autocomplete, { createFilterOptions } from "@mui/material/Autocomplete";
import { Capitalize, isEmpty } from "@utils/Utils";
import { Controller } from "react-hook-form";

const filter = createFilterOptions<any>();

const FormCreatableSelect = ({ data, control, fieldName }: any) => {
  const { name, isOptional, typeName, cardinality } = data;

  return (
    <Controller
      name={!isEmpty(fieldName) ? `${fieldName}.${name}` : name}
      control={control}
      key={`autocomplete-${name}`}
      rules={{
        required: isOptional ? false : true
      }}
      defaultValue={[]}
      render={({ field: { onChange, value }, fieldState: { error } }) => {
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
                  textOverflow="ellipsis"
                  maxWidth="160px"
                  overflow="hidden"
                  fontSize={14}
                >{`(${typeName}) ${cardinality}`}</Typography>
              </LightTooltip>
            </div>
            <Autocomplete
              size="small"
              multiple
              onChange={(_event, newValue) => {
                onChange(newValue);
              }}
              filterOptions={(options, params) => {
                const filtered = filter(options, params);

                const { inputValue } = params;

                const isExisting = options.some(
                  (option) => inputValue === option
                );
                if (inputValue !== "" && !isExisting) {
                  filtered.push({
                    inputValue
                  });
                }

                return filtered;
              }}
              value={value || []}
              getOptionLabel={(option) => {
                return !isEmpty(option?.inputValue)
                  ? option.inputValue
                  : option;
              }}
              options={[]}
              className="form-autocomplete-field"
              renderOption={(props, option) => {
                const { ...optionProps } = props;
                const { inputValue } = option;
                return <li {...optionProps}>{inputValue}</li>;
              }}
              isOptionEqualToValue={(option, value) =>
                option.inputValue === value.inputValue
              }
              filterSelectedOptions
              renderInput={(params) => (
                <TextField
                  {...params}
                  error={!!error}
                  sx={{ padding: "0 !important" }}
                  className="form-textfield"
                  size="small"
                  InputProps={{
                    ...params.InputProps
                  }}
                  placeholder={`Select a ${typeName} from the dropdown list`}
                  // helperText={error ? "This field is required" : ""}
                />
              )}
            />
          </>
        );
      }}
    />
  );
};

export default FormCreatableSelect;
