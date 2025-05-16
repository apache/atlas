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

import { useEffect, useRef, useState } from "react";
import { EllipsisText } from "@components/commonComponents";
import {
  LightTooltip,
  CustomButton,
  AccordionDetails,
  AccordionSummary
} from "@components/muiComponents";
import SkeletonLoader from "@components/SkeletonLoader";
import {
  Accordion,
  Autocomplete,
  Chip,
  CircularProgress,
  createFilterOptions,
  Stack,
  TextField,
  Typography
} from "@mui/material";
import { isEmpty, serverError } from "@utils/Utils";
import { Controller, useForm } from "react-hook-form";
import { getGlobalSearchResult } from "@api/apiMethods/searchApiMethod";
import { AxiosResponse } from "axios";
import { toast } from "react-toastify";
import { useParams } from "react-router-dom";
import { getLabels } from "@api/apiMethods/detailpageApiMethod";
import { useAppDispatch } from "@hooks/reducerHook";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";

const filter = createFilterOptions<any>();

const Labels = ({ loading, labels }: any) => {
  const { guid }: any = useParams();
  const toastId: any = useRef(null);
  const dispatchApi = useAppDispatch();
  const [addLabel, setAddLabel] = useState<boolean>(true);
  const [expanded, setExpanded] = useState<string | false>(false);
  const [open, setOpen] = useState(false);
  const [options, setOptions] = useState([]);
  const [loader, setLoader] = useState(false);
  const {
    control,
    handleSubmit,
    reset,
    formState: { isSubmitting }
  } = useForm();

  useEffect(() => {
    if (!isEmpty(labels)) {
      setExpanded("labelsPanel");
    }
  }, [labels]);

  const handleChange =
    (panel: string) => (_event: React.SyntheticEvent, isExpanded: boolean) => {
      setExpanded(isExpanded ? panel : false);
      if (expanded) {
        setAddLabel(true);
      }
    };

  const getData = async (searchTerm: string) => {
    let suggestionSearchResp: AxiosResponse = await getGlobalSearchResult(
      "suggestions",
      {
        params: {
          fieldName: "__labels",
          ...(!isEmpty(searchTerm) && { prefixString: searchTerm })
        }
      }
    );
    const { suggestions } = suggestionSearchResp.data;
    setOptions(suggestions);
    setLoader(false);
  };

  const handleOpen = () => {
    setOpen(true);
    setLoader(true);
    getData("");
  };
  const handleClose = () => {
    setLoader(false);
    setOpen(false);
  };
  const onInputChange = (_event: any, value: string) => {
    if (value) {
      setOpen(true);
      setLoader(true);
      getData(value);
    } else {
      setLoader(false);
      setOpen(false);
    }
  };

  const onSubmit = async (values: any) => {
    let formData = { ...values };
    let data = formData.labels.map((obj: { inputValue: any }) => {
      if (obj.inputValue) {
        return obj.inputValue;
      }
      return obj;
    });
    try {
      await getLabels(guid, data);
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        "One or more labels were updated successfully"
      );

      if (!isEmpty(guid)) {
        dispatchApi(fetchDetailPageData(guid as string));
      }

      setAddLabel(true);
    } catch (error) {
      console.log("Error while adding labels", error);
      toast.dismiss(toastId.current);
      serverError(error, toastId);
    }
  };

  const handleSave = (e: React.MouseEvent) => {
    e.stopPropagation();
    handleSubmit(onSubmit)();
  };

  return (
    <form onSubmit={handleSubmit(onSubmit)}>
      <Accordion
        variant="outlined"
        expanded={expanded === "labelsPanel"}
        onChange={handleChange("labelsPanel")}
        data-cy="labels"
      >
        <AccordionSummary
          className="custom-accordion-summary"
          aria-controls="User-defined properties-content"
          id="User-defined properties-header"
        >
          <Stack direction="row" alignItems="center" flex="1">
            <div className="properties-panel-name">
              <Typography fontWeight="600" className="text-color-green">
                <Stack direction="row" alignItems="center" flex="1">
                  <div className="properties-panel-name">
                    <Typography fontWeight="600" className="text-color-green">
                      Labels
                    </Typography>
                  </div>

                  <Stack direction="row" alignItems="center" gap="0.5rem">
                    {addLabel ? (
                      <CustomButton
                        variant="outlined"
                        color="success"
                        size="small"
                        onClick={(e: { stopPropagation: () => void }) => {
                          e.stopPropagation();
                          setExpanded("labelsPanel");
                          setAddLabel(false);
                        }}
                      >
                        {!isEmpty(labels) ? "Edit" : "Add"}
                      </CustomButton>
                    ) : (
                      <>
                        <CustomButton
                          variant="outlined"
                          color="success"
                          size="small"
                          onClick={handleSave}
                          startIcon={
                            isSubmitting && <CircularProgress size="14px" />
                          }
                        >
                          Save
                        </CustomButton>
                        <CustomButton
                          variant="outlined"
                          color="success"
                          size="small"
                          onClick={(e: { stopPropagation: () => void }) => {
                            e.stopPropagation();
                            setAddLabel(true);
                            reset();
                          }}
                        >
                          Cancel
                        </CustomButton>
                      </>
                    )}
                  </Stack>
                </Stack>{" "}
              </Typography>{" "}
            </div>
          </Stack>
        </AccordionSummary>
        <AccordionDetails>
          {loading == undefined || loading ? (
            <SkeletonLoader count={3} animation="wave" />
          ) : (
            <Stack
              direction="row"
              alignItems="center"
              flexWrap="wrap"
              gap="0.5rem"
              spacing={1}
              justifyContent={isEmpty(labels) ? "center" : "flex-start"}
            >
              {addLabel ? (
                <>
                  {!isEmpty(labels) ? (
                    labels.map((label: string) => {
                      return (
                        <LightTooltip title={label}>
                          <Chip
                            label={<EllipsisText>{label}</EllipsisText>}
                            size="small"
                            sx={{
                              "& .MuiChip-label": {
                                display: "block",
                                overflow: "ellipsis",
                                maxWidth: "76px"
                              }
                            }}
                            className="chip-items properties-labels-chip"
                            data-cy="tagClick"
                            clickable
                          />
                        </LightTooltip>
                      );
                    })
                  ) : (
                    <span>
                      No labels have been created yet. To add a labels, click{" "}
                      <Typography
                        className="text-color-green cursor-pointer"
                        component="span"
                        onClick={(e: { stopPropagation: () => void }) => {
                          e.stopPropagation();
                          setAddLabel(false);
                        }}
                        style={{ textDecoration: "underline" }}
                      >
                        here
                      </Typography>
                    </span>
                  )}
                </>
              ) : (
                <Controller
                  name={"labels"}
                  control={control}
                  defaultValue={labels}
                  render={({ field: { onChange, value } }) => {
                    return (
                      <>
                        <Autocomplete
                          multiple
                          open={open}
                          onOpen={() => {
                            handleOpen();
                          }}
                          size="small"
                          onClose={() => {
                            handleClose();
                          }}
                          onChange={(_event, newValue) => {
                            const uniqueLabels = [];
                            const seen = new Set();
                            if (Array.isArray(newValue)) {
                              for (const option of newValue) {
                                const labelValue =
                                  typeof option === "string"
                                    ? option.toLowerCase()
                                    : option.inputValue
                                    ? option.inputValue.toLowerCase()
                                    : option.value
                                    ? option.value.toLowerCase()
                                    : "";
                                if (!seen.has(labelValue)) {
                                  seen.add(labelValue);
                                  uniqueLabels.push(option);
                                }
                              }
                              onChange(uniqueLabels);
                            } else {
                              onChange(newValue);
                            }
                          }}
                          isOptionEqualToValue={(option, value) => {
                            if (
                              typeof option === "string" &&
                              typeof value === "string"
                            ) {
                              return option === value;
                            }

                            if (
                              typeof option === "object" &&
                              typeof value === "object"
                            ) {
                              return option.inputValue === value.inputValue;
                            }
                            return false;
                          }}
                          filterOptions={(options, params) => {
                            const filtered = filter(options, params);

                            const { inputValue } = params;

                            const isExisting = options.some(
                              (option) => inputValue === option?.value
                            );
                            if (inputValue !== "" && !isExisting) {
                              filtered.push({
                                inputValue
                              });
                            }

                            return filtered;
                          }}
                          value={value}
                          defaultValue={labels}
                          getOptionLabel={(option) => {
                            return !isEmpty(option?.inputValue)
                              ? option.inputValue
                              : option;
                          }}
                          options={options}
                          onInputChange={onInputChange}
                          className="form-autocomplete-field"
                          filterSelectedOptions
                          renderInput={(params) => (
                            <TextField
                              {...params}
                              className="form-textfield"
                              size="small"
                              InputProps={{
                                ...params.InputProps,
                                endAdornment: (
                                  <>
                                    {loader ? (
                                      <CircularProgress size="14px" />
                                    ) : null}
                                    {params.InputProps.endAdornment}
                                  </>
                                )
                              }}
                              placeholder={`Select Label`}
                            />
                          )}
                        />
                      </>
                    );
                  }}
                />
              )}
            </Stack>
          )}
        </AccordionDetails>
      </Accordion>
    </form>
  );
};

export default Labels;
