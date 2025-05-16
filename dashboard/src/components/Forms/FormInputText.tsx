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
import { Capitalize, isEmpty } from "@utils/Utils";
import { Controller } from "react-hook-form";

const FormInputText = ({
  data,
  control,
  fieldName,
  isLabelCapitalized
}: any) => {
  const { name, isOptional, typeName } = data;
  return (
    <Controller
      name={!isEmpty(fieldName) ? `${fieldName}.${name}` : name}
      control={control}
      rules={{
        required: !isOptional
      }}
      key={name}
      render={({ field: { onChange, value }, fieldState: { error } }) => (
        <>
          <div className="form-fields">
            <InputLabel className="form-textfield-label" required={!isOptional}>
              {!isEmpty(isLabelCapitalized) ? name : Capitalize(name)}
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
          <TextField
            margin="normal"
            error={!!error}
            fullWidth
            onChange={onChange}
            value={value}
            variant="outlined"
            size="small"
            type={typeName == "string" ? "text" : "number"}
            placeholder={name}
            // helperText={error ? "This field is required" : ""}
            className="form-textfield"
          />
        </>
      )}
    />
  );
};

export default FormInputText;
