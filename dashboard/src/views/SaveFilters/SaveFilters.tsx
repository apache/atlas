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

import { editSavedSearch } from "@api/apiMethods/savedSearchApiMethod";
import CustomModal from "@components/Modal";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { CustomFiltersNodeType } from "@models/customFiltersType";
import {
  Autocomplete,
  createFilterOptions,
  InputLabel,
  Stack,
  TextField,
  Typography
} from "@mui/material";
import { fetchSavedSearchData } from "@redux/slice/savedSearchSlice";
import { generateObjectForSaveSearchApi } from "@utils/CommonViewFunction";
import { cloneDeep } from "@utils/Helper";
import { isEmpty, serverError } from "@utils/Utils";
import { useRef } from "react";
import { Controller, useForm } from "react-hook-form";
import { useLocation } from "react-router-dom";
import { toast } from "react-toastify";

const filter = createFilterOptions<any>();

const SaveFilters = ({
  open,
  onClose
}: {
  open: boolean;
  onClose: () => void;
}) => {
  const dispatchApi = useAppDispatch();
  const { savedSearchData }: any = useAppSelector(
    (state: any) => state.savedSearch
  );
  const toastId = useRef<any>(null);
  const location = useLocation();
  const saveSearch = cloneDeep(savedSearchData);

  let searchParams = new URLSearchParams(location.search);
  let paramKeys = [
    "searchType",
    "type",
    "tag",
    "term",
    "relationshipName",
    "includeDE",
    "excludeSC",
    "excludeST"
  ];

  let params: any = paramKeys.reduce((acc: any, key) => {
    acc[key] = searchParams.get(key);
    return acc;
  }, {});

  const {
    searchType,
    type,
    tag,
    query,
    term,
    relationshipName,
    includeDE,
    excludeSC,
    excludeST
  } = params;

  let isBasic = searchType == "basic" ? true : false;
  let value = Object.fromEntries(searchParams.entries());
  let urlObj: any = {
    includeDE: includeDE,
    excludeSC: excludeSC,
    excludeST: excludeST
  };

  if (!isEmpty(searchParams) && (type || tag || query || term)) {
    searchType == "basic" ? (isBasic = true) : (isBasic = false);
    urlObj.includeDE = urlObj.includeDE == "true" ? true : false;
    urlObj.excludeSC = urlObj.excludeSC == "true" ? true : false;
    urlObj.excludeST = urlObj.excludeST == "true" ? true : false;
  }

  const getValue = () => {
    return { ...{}, ...value, ...urlObj };
  };

  const {
    control,
    handleSubmit,
    formState: { isSubmitting }
  } = useForm({
    mode: "onChange",
    shouldUnregister: true
  });

  const options = !isEmpty(saveSearch)
    ? saveSearch
        .filter((item: { searchType: string }) => {
          if (relationshipName) {
            return item.searchType === "BASIC_RELATIONSHIP";
          } else {
            const searchType = isBasic ? "BASIC" : "ADVANCED";
            return item.searchType === searchType;
          }
        })
        .sort((a: { name: string }, b: { name: any }) =>
          a.name.localeCompare(b.name)
        )
        .map((obj: { name: any }) => {
          return { label: obj.name, value: obj.name };
        })
    : [];

  const updatedData = async () => {
    await dispatchApi(fetchSavedSearchData());
  };

  const onSubmit = async (values: any) => {
    const { saveFilter } = values;
    const { label } = saveFilter;
    let selectedSearchData: any = {};

    selectedSearchData = !isEmpty(saveSearch)
      ? saveSearch?.find((obj: any) => {
          if (obj.name == label) {
            return obj;
          }
        })
      : {};

    let obj = {
      name: label || null,
      value: getValue()
    };

    let saveObj: any = generateObjectForSaveSearchApi(obj);

    if (selectedSearchData && Object.keys(selectedSearchData).length > 0) {
      for (let key in saveObj.searchParameters) {
        if (saveObj.searchParameters.hasOwnProperty(key)) {
          selectedSearchData.searchParameters[key] =
            saveObj.searchParameters[key];
        }
      }

      selectedSearchData.name = obj.name;
      saveObj = selectedSearchData;
    } else {
      if (isBasic && !relationshipName) {
        saveObj["searchType"] = "BASIC";
      } else if (relationshipName) {
        saveObj["searchType"] = "BASIC_RELATIONSHIP";
      } else {
        saveObj["searchType"] = "ADVANCED";
      }
    }

    try {
      await editSavedSearch(
        saveObj as CustomFiltersNodeType,
        saveObj?.guid ? "PUT" : "POST"
      );
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        `${saveObj.name} was updated successfully`
      );
      updatedData();
      onClose();
    } catch (error) {
      console.log(`Error occur while updating filters`, error);
      serverError(error, toastId);
    }
  };
  const getSearchType = () => {
    if (relationshipName) {
      return "Relationship";
    }
    return isBasic ? "Basic" : "Advanced";
  };

  return (
    <>
      <CustomModal
        open={open}
        onClose={onClose}
        title={`Save ${getSearchType()} Custom Filter`}
        button1Label="Cancel"
        button1Handler={onClose}
        button2Label={!isEmpty(saveSearch) ? "Save as" : "Save"}
        maxWidth="sm"
        button2Handler={handleSubmit(onSubmit)}
        disableButton2={isSubmitting}
      >
        {" "}
        <form onSubmit={handleSubmit(onSubmit)}>
          <Stack marginBottom="1rem">
            <Controller
              name={"saveFilter"}
              control={control}
              key={`autocomplete-saveFilter`}
              rules={{
                required: true
              }}
              render={({
                field: { onChange, value },
                fieldState: { error }
              }) => {
                return (
                  <>
                    <div className="form-fields">
                      <InputLabel required={true}>Name</InputLabel>
                    </div>
                    <Autocomplete
                      onChange={(_event, newValue) => {
                        if (typeof newValue === "string") {
                          onChange({
                            label: newValue,
                            value: newValue
                          });
                        } else if (newValue && newValue.inputValue) {
                          onChange({
                            label: newValue.inputValue,
                            value: newValue.inputValue
                          });
                        } else {
                          onChange(newValue || null);
                        }
                      }}
                      filterOptions={(options, params) => {
                        const filtered = filter(options, params);

                        const { inputValue } = params;
                        const isExisting = options.some(
                          (option) => inputValue === option.label
                        );
                        if (inputValue !== "" && !isExisting) {
                          filtered.push({
                            inputValue,
                            label: `Add: "${inputValue}"`
                          });
                        } else {
                        }

                        return filtered;
                      }}
                      value={value || null}
                      getOptionLabel={(option) => {
                        if (typeof option === "string") {
                          return option;
                        }
                        if (option.inputValue) {
                          return option.inputValue;
                        }
                        return option.label;
                      }}
                      options={options}
                      className="form-autocomplete-field"
                      renderOption={(props, option) => {
                        const { key, ...optionProps }: any = props;
                        const isAddOption = option.label.startsWith("Add:");

                        return (
                          <li key={key} {...optionProps}>
                            <Typography fontWeight="400" color="text.secondary">
                              {isAddOption ? "Add:" : "Update:"}
                            </Typography>{" "}
                            <Typography fontWeight="600">
                              {option.label.replace(/^Add: /, "")}
                            </Typography>
                          </li>
                        );
                      }}
                      isOptionEqualToValue={(option, value) =>
                        option.inputValue === value.inputValue
                      }
                      renderInput={(params) => (
                        <TextField
                          {...params}
                          error={!!error}
                          className="form-textfield"
                          size="small"
                          InputProps={{
                            ...params.InputProps
                          }}
                          placeholder={`Enter filter name`}
                        />
                      )}
                    />
                  </>
                );
              }}
            />
          </Stack>
        </form>
      </CustomModal>
    </>
  );
};

export default SaveFilters;
