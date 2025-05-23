// @ts-nocheck

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

import {
  Accordion,
  Stack,
  Typography,
  CircularProgress,
  IconButton,
  TextField,
  Divider
} from "@mui/material";
import {
  CustomButton,
  AccordionDetails,
  AccordionSummary,
  TextArea
} from "@components/muiComponents";
import { isEmpty, serverError } from "@utils/Utils";
import { useEffect, useRef, useState } from "react";
import {
  Controller,
  FieldError,
  useFieldArray,
  useForm
} from "react-hook-form";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";
import RemoveOutlinedIcon from "@mui/icons-material/RemoveOutlined";
import SkeletonLoader from "@components/SkeletonLoader";
import { createEntity } from "@api/apiMethods/entityFormApiMethod";
import { toast } from "react-toastify";
import { useAppDispatch } from "@hooks/reducerHook";
import { useParams } from "react-router-dom";
import { cloneDeep } from "@utils/Helper";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";

const defaultField = {
  key: "",
  value: ""
};

const UserDefinedProperties = ({ loading, customAttributes, entity }: any) => {
  const dispatchApi = useAppDispatch();
  const { guid }: any = useParams();
  const toastId: any = useRef(null);
  const [addLabel, setAddLabel] = useState<boolean>(true);
  const [expanded, setExpanded] = useState<string | false>(false);
  let attributes = cloneDeep(customAttributes);
  const defaultFieldValues = !isEmpty(attributes)
    ? Object.entries(attributes).map(([key, value]) => ({
        key,
        value
      }))
    : [defaultField];
  const {
    control,
    handleSubmit,
    setValue,
    getValues,
    reset,
    formState: { isSubmitting }
  } = useForm({
    defaultValues: {
      customAttributes: defaultFieldValues
    }
  });

  const { fields, append, remove } = useFieldArray({
    control,
    name: "customAttributes"
  });

  useEffect(() => {
    if (!isEmpty(customAttributes)) {
      setExpanded("userDefinedPanel");
    }
  }, [customAttributes]);

  const handleChange =
    (panel: string) => (event: React.SyntheticEvent, isExpanded: boolean) => {
      setExpanded(isExpanded ? panel : false);
      if (expanded) {
        setAddLabel(true);
      }
    };

  const structureAttributes = (list: any) => {
    let obj: any = {};
    list.map((o: any) => {
      obj[o.key] = o.value;
    });
    return obj;
  };

  const onSubmit = async (values: any) => {
    let formData = { ...values };
    let entityObj = { ...entity };
    let properties = structureAttributes(formData.customAttributes);
    entityObj.customAttributes = !isEmpty(properties) ? properties : {};
    try {
      await createEntity({ entity: entityObj });
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        `One or more user-defined properties were created successfully`
      );

      if (!isEmpty(guid)) {
        dispatchApi(fetchDetailPageData(guid as string));
      }

      setAddLabel(true);
    } catch (error) {
      console.error(
        "Errow while creating or updating user-defined properties",
        error
      );
      toast.dismiss(toastId.current);
      serverError(error, toastId);
    }
  };

  const handleSave = (e: React.MouseEvent) => {
    e.stopPropagation();
    handleSubmit(onSubmit)();
  };

  const validateKeyUnique = (value: any, index: number): any => {
    const items = getValues("customAttributes");
    const duplicate = items.some(
      (item: { key: any }, i: any) => item.key === value && i !== index
    );
    return duplicate ? "Key must be unique" : true;
  };

  const errorMssg = (error: FieldError | undefined) => {
    if (error?.message) {
      return error.message;
    } else {
      return "";
    }
  };

  return (
    <>
      <form onSubmit={handleSubmit(onSubmit)}>
        <Accordion
          variant="outlined"
          data-cy="userDefined"
          expanded={expanded === "userDefinedPanel"}
          onChange={handleChange("userDefinedPanel")}
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
                        User-defined properties
                      </Typography>{" "}
                    </div>

                    <Stack direction="row" alignItems="center" gap="0.5rem">
                      {addLabel ? (
                        <CustomButton
                          variant="outlined"
                          color="success"
                          size="small"
                          onClick={(e: { stopPropagation: () => void }) => {
                            e.stopPropagation();
                            setExpanded("userDefinedPanel");
                            setAddLabel(false);
                            if (!isEmpty(defaultFieldValues)) {
                              reset({ customAttributes: defaultFieldValues });
                            }
                          }}
                        >
                          {!isEmpty(customAttributes) ? "Edit" : "Add"}
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
                              reset({ customAttributes: [defaultField] });
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
            {addLabel ? (
              <>
                {loading == undefined || loading ? (
                  <SkeletonLoader count={3} animation="wave" />
                ) : (
                  <Stack
                    direction="column"
                    alignItems="center"
                    flexWrap="wrap"
                    gap="0.25rem"
                    spacing={1}
                    justifyContent="center"
                  >
                    {!isEmpty(customAttributes) ? (
                      Object.entries(customAttributes)
                        .sort()
                        .map(([keys, value]: [string, any]) => {
                          return (
                            <>
                              <Stack
                                direction="row"
                                spacing={4}
                                marginBottom={1}
                                marginTop={1}
                                width="100%"
                              >
                                <div
                                  style={{
                                    flex: "0 0 8em",
                                    wordBreak: "break-all",
                                    textAlign: "left",
                                    fontWeight: "600"
                                  }}
                                >
                                  {keys}
                                </div>
                                <div
                                  style={{
                                    flex: 1,
                                    // wordBreak: "break-all",
                                    textAlign: "left"
                                  }}
                                >
                                  {value}
                                </div>
                              </Stack>
                              <Divider sx={{ width: "100%" }} />
                            </>
                          );
                        })
                    ) : (
                      <span>
                        No properties have been created yet. To add a
                        property,click{" "}
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
                  </Stack>
                )}
              </>
            ) : (
              <>
                {fields.map((item: any, index) => {
                  return (
                    <Stack
                      gap="0.5rem"
                      direction="row"
                      key={item?.id}
                      alignItems="flex-start"
                    >
                      <Controller
                        control={control}
                        name={`customAttributes.${index}.key` as const}
                        rules={{
                          required: true,
                          validate: (value) => validateKeyUnique(value, index)
                        }}
                        defaultValue={item.key}
                        render={({
                          field: { onChange, value },
                          fieldState: { error }
                        }) => (
                          <>
                            <TextField
                              margin="normal"
                              fullWidth
                              error={!!error}
                              onChange={onChange}
                              value={value}
                              variant="outlined"
                              size="small"
                              placeholder={"key"}
                              className="form-textfield"
                              sx={{
                                flexBasis: "35%",
                                marginTop: "8px !important",
                                marginBottom: "8px !important"
                              }}
                              helperText={errorMssg(error)}
                            />
                          </>
                        )}
                      />
                      <span style={{ margin: "12px 0" }}>:</span>
                      <Controller
                        control={control}
                        name={`customAttributes.${index}.value` as const}
                        defaultValue={item.value}
                        render={({ field: { onChange, value } }) => (
                          <>
                            <div style={{ flex: "1" }}>
                              <textarea
                                maxRows={3}
                                rows={1}
                                aria-label="maximum height"
                                placeholder={"value"}
                                onChange={onChange}
                                className="form-textfield form-textarea-field"
                                value={value}
                              ></textarea>
                            </div>
                          </>
                        )}
                      />
                      <Stack direction="row" gap="0.5rem" marginTop="8px">
                        <IconButton
                          aria-label="back"
                          size="small"
                          color="error"
                          onClick={(e) => {
                            e.stopPropagation();
                            remove(index);
                          }}
                        >
                          <RemoveOutlinedIcon fontSize="small" />
                        </IconButton>

                        <IconButton
                          size="small"
                          aria-label="add"
                          className="cursor-pointer"
                          onClick={(e: any) => {
                            e.stopPropagation();
                            append(defaultField);
                          }}
                        >
                          <AddOutlinedIcon fontSize="small" />
                        </IconButton>
                      </Stack>
                    </Stack>
                  );
                })}
              </>
            )}
          </AccordionDetails>
        </Accordion>
      </form>
    </>
  );
};

export default UserDefinedProperties;
