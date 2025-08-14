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

import { getAttributes } from "@api/apiMethods/entityFormApiMethod";
import { LightTooltip } from "@components/muiComponents";
import {
  CircularProgress,
  InputLabel,
  TextField,
  Typography
} from "@mui/material";
import Autocomplete from "@mui/material/Autocomplete";
import {
  Capitalize,
  extractKeyValueFromEntity,
  isEmpty,
  serverError
} from "@utils/Utils";
import { AxiosResponse } from "axios";
import { useRef, useState } from "react";
import { Controller } from "react-hook-form";
import { toast } from "react-toastify";

const FormAutocomplete = ({ data, control }: any) => {
  const { name, isOptional, typeName } = data;
  const [options, setOptions] = useState([]);
  const [loading, setloading] = useState<boolean>(false);
  const toastId: any = useRef(null);

  const getData = async (searchTerm: string) => {
    let splitTypeName = typeName.split("<");
    if (splitTypeName.length > 1) {
      splitTypeName = splitTypeName[1].split(">")[0];
    } else {
      splitTypeName = typeName;
    }
    let params = {
      attrValuePrefix: searchTerm,
      typeName: splitTypeName,
      limit: 10,
      offset: 0
    };

    try {
      let searchAttributeResp: AxiosResponse = await getAttributes(params);

      const { entities } = searchAttributeResp.data || {};
      if (!isEmpty(entities)) {
        let optionList = entities.map((obj: any) => {
          const { name } = extractKeyValueFromEntity(obj, "qualifiedName");
          return { label: name, typeName: obj.typeName, guid: obj.guid };
        });

        setOptions(optionList);
        setloading(false);
      } else {
        setloading(false);
      }
    } catch (error) {
      setloading(false);
      console.log("Error while fetching attributes", error);
      toast.dismiss(toastId.current);
      serverError(error, toastId);
    }
  };

  const onInputChange = (_event: any, value: string) => {
    if (!isEmpty(value) && value != "undefined") {
      setloading(true);
      getData(value);
    } else {
      setOptions([]);

      setloading(false);
    }
  };

  return (
    <Controller
      control={control}
      name={name}
      key={`autocomplete-${name}`}
      rules={{
        required: isOptional ? false : true
      }}
      defaultValue={typeName.indexOf("array") > -1 ? [] : null}
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
                >{`(${typeName})`}</Typography>
              </LightTooltip>
            </div>
            <Autocomplete
              multiple={typeName.indexOf("array") > -1 ? true : false}
              onChange={(_event, item) => {
                onChange(item);
              }}
              value={value || []}
              filterSelectedOptions
              getOptionLabel={(option) => {
                if (typeof option === "string") {
                  return option;
                }
                if (option && typeof option === "object") {
                  if (!isEmpty(option.label)) {
                    return option.label;
                  }
                  if (!isEmpty(option.inputValue)) {
                    return option.inputValue;
                  }
                }
                return "";
              }}
              isOptionEqualToValue={(option, value) => {
                if (!isEmpty(options)) {
                  return option === value;
                }
                return option.inputValue === value.inputValue;
              }}
              options={options}
              loading={loading}
              className="form-autocomplete-field"
              renderInput={(params) => (
                <TextField
                  {...params}
                  error={!!error}
                  className="form-textfield"
                  size="small"
                  InputProps={{
                    ...params.InputProps,
                    endAdornment: (
                      <>
                        {loading ? (
                          <CircularProgress color="inherit" size={30} />
                        ) : null}
                        {params.InputProps.endAdornment}
                      </>
                    )
                  }}
                  placeholder={`Select a ${typeName} from the dropdown list`}
                  // helperText={error ? "This field is required" : ""}
                />
              )}
              onInputChange={(e, value) => onInputChange(e, value)}
            />
          </>
        );
      }}
    />
  );
};

export default FormAutocomplete;
