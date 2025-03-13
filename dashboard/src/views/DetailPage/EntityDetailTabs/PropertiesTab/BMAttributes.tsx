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

import { useEffect, useRef, useState } from "react";
import {
  CustomButton,
  Accordion,
  AccordionDetails,
  AccordionSummary,
  LightTooltip
} from "@components/muiComponents";
import SkeletonLoader from "@components/SkeletonLoader";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import {
  Stack,
  Typography,
  CircularProgress,
  TextField,
  IconButton,
  Autocomplete,
  MenuItem,
  InputLabel
} from "@mui/material";

import {
  dateFormat,
  formatedDate,
  isEmpty,
  isNull,
  serverError
} from "@utils/Utils";
import { Controller, useFieldArray, useForm, useWatch } from "react-hook-form";
import { useParams } from "react-router-dom";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";
import RemoveOutlinedIcon from "@mui/icons-material/RemoveOutlined";
import HtmlRenderer from "@components/HtmlRenderer";
import AddIcon from "@mui/icons-material/Add";
import BMAttributesFields from "./BMAttributesFields";
import { getEntityBusinessMetadata } from "@api/apiMethods/detailpageApiMethod";
import { toast } from "react-toastify";
import { cloneDeep } from "@utils/Helper";
import moment from "moment-timezone";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";

const defaultField = {
  key: null,
  value: ""
};

const BMAttributes = ({ loading, bmAttributes, entity }: any) => {
  const dispatchApi = useAppDispatch();
  const { guid }: any = useParams();
  const toastId: any = useRef(null);
  const { entityData }: any = useAppSelector((state: any) => state.entity);
  const { entityDefs } = entityData || {};
  let filterEntityData = cloneDeep(entityDefs);
  const { businessMetaData }: any = useAppSelector(
    (state: any) => state.businessMetaData
  );
  const { businessMetadataDefs } = businessMetaData || {};

  let businessAttributes = cloneDeep(bmAttributes);
  let bmAttributesData = Object.entries(businessAttributes).map(
    ([key, value]: any) => {
      let foundBusinessMetadata = businessMetadataDefs.find(
        (obj: { name: any }) => obj.name == key
      );
      let businessMetadata = key;
      let newValue = { ...value };

      if (foundBusinessMetadata) {
        for (let val in newValue as any) {
          var foundAttr = foundBusinessMetadata.attributeDefs.find(
            (o: { name: string }) => o.name == val
          );
          if (foundAttr) {
            newValue[val] = {
              value: newValue[val],
              typeName: foundAttr.typeName
            };
          }
          if (foundAttr && foundAttr.typeName === "string") {
            newValue[val].key =
              businessMetadata.replace(/ /g, "_") +
              "_" +
              val.replace(/ /g, "_");
          }
        }
      }

      return { ...newValue, ...{ __internal_UI_businessMetadataName: key } };
    }
  );

  let typeDefEntityData =
    !isEmpty(filterEntityData) && !isEmpty(entity)
      ? filterEntityData?.find((entitys: { name: string }) => {
          if (entitys.name == entity.typeName) {
            return entitys;
          }
        })
      : {};

  const { businessAttributeDefs = {} } = typeDefEntityData;
  let options = !isEmpty(businessAttributeDefs)
    ? Object.keys(businessAttributeDefs).map((obj) => {
        return {
          label: obj,
          value: businessAttributeDefs[obj].map(
            (bm: { name: any; typeName: any }) => ({
              label: bm.name,
              typeName: bm.typeName,
              obj: bm
            })
          )
        };
      })
    : [];

  let bmOptions = options.flatMap((group) =>
    group.value.map((item: any) => ({
      typeName: item.typeName,
      label: item.label,
      group: group.label,
      obj: item.obj
    }))
  );
  const [addLabel, setAddLabel] = useState<boolean>(true);
  const [expanded, setExpanded] = useState<string | false>(false);
  const [selectedIndex, setSelectedIndex] = useState();

  let attributes = cloneDeep(bmAttributes);

  let defaultFieldValues = !isEmpty(attributes)
    ? Object.entries(attributes)
        .map(([group, attrs]) =>
          Object.entries(attrs)
            .map(([key, value]) => {
              const matchedDef = businessAttributeDefs[group].find(
                (bm) => bm.name === key
              );

              if (!matchedDef) {
                return null;
              }

              return {
                key: {
                  label: matchedDef.name,
                  group: group,
                  value: matchedDef.typeName,
                  obj: matchedDef
                },
                value: value
              };
            })
            .filter((item) => item !== null)
        )
        .flat()
    : [defaultField];

  const {
    control,
    handleSubmit,
    reset,
    watch,
    setValue,
    formState: { isSubmitting, errors }
  } = useForm({
    defaultValues: {
      businessMetadata: defaultFieldValues
    }
  });
  const { fields, append, remove } = useFieldArray({
    control,
    name: "businessMetadata"
  });

  useEffect(() => {
    if (!isEmpty(bmAttributes)) {
      setExpanded("bmDataPanel");
    }
  }, [bmAttributes]);

  const handleChange =
    (panel: string) => (_event: React.SyntheticEvent, isExpanded: boolean) => {
      setExpanded(isExpanded ? panel : false);
      if (expanded) {
        setAddLabel(true);
      }
    };

  let bmAttributesValues = useWatch({
    name: "businessMetadata",
    control
  });

  const onSubmit = async (values: { businessMetadata: any }) => {
    let formData = { ...values };
    const { businessMetadata } = formData;
    let data: Record<string, any> = {};
    for (let dataObj of businessMetadata) {
      if (isEmpty(dataObj.key)) {
        return;
      }
      const { label, group, obj } = dataObj.key;
      const { typeName } = obj;

      let atrributeType: string[] = [
        "array<string>",
        "array<int>",
        "array<short>",
        "array<float>",
        "array<double",
        "array<long>"
      ];

      if (atrributeType.includes(typeName)) {
        data[group] = {
          ...data[group],
          ...{
            [label]: !isEmpty(dataObj)
              ? dataObj?.value?.map(
                  (input: { inputValue: any }) => input.inputValue || input
                )
              : []
          }
        };
      } else if (
        typeName.indexOf("array") > -1 &&
        !atrributeType.includes(typeName)
      ) {
        data[group] = {
          ...data[group],
          ...{
            [label]: dataObj?.value?.map((val: any) =>
              typeName === "array<date>" ? val : val.label
            )
          }
        };
      } else if (typeName == "boolean") {
        data[group] = {
          ...data[group],
          ...{ [label]: dataObj.value == "true" ? true : false }
        };
      } else if (typeName == "date") {
        data[group] = {
          ...data[group],
          ...{ [label]: moment(dataObj.value).valueOf() }
        };
      } else {
        data[group] = { ...data[group], ...{ [label]: dataObj.value } };
      }
    }

    if (isEmpty(data) && !isEmpty(bmAttributes)) {
      for (let attr in bmAttributes) {
        data[attr] = {};
      }
    }

    try {
      await getEntityBusinessMetadata(guid, data);
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        "One or more Business Metadata attributes were updated successfully"
      );
      if (!isEmpty(guid)) {
        dispatchApi(fetchDetailPageData(guid as string));
      }

      setAddLabel(true);
    } catch (error) {
      console.error(
        "Error while adding or updating business metadta attribute",
        error
      );
      toast.dismiss(toastId.current);
      serverError(error, toastId);
    }
  };
  const handleSave = (e: React.MouseEvent) => {
    console.log(errors);
    e.stopPropagation();
    handleSubmit(onSubmit)();
  };

  const renderValues = (values: any) => {
    const { value, typeName } = values;

    if (
      typeName.indexOf("array<date>") == -1 &&
      typeName.indexOf("array") > -1
    ) {
      return value.map((obj: number) => obj).join(", ");
    }
    if (value.length > 0 && typeName.indexOf("array<date>") > -1) {
      return value
        .map((obj: number) =>
          formatedDate({
            date: obj,
            zone: false,
            dateFormat: dateFormat
          })
        )
        .join(", ");
    }
    if (typeName == "boolean") {
      return value == true ? "true" : "false";
    }
    if (typeName === "date") {
      return formatedDate({
        date: value,
        zone: false,
        dateFormat: dateFormat
      });
    }
    return value;
  };

  const selectedOptions = watch("businessMetadata");

  const getAvailableOptions = (index) => {
    return bmOptions.filter((option) => {
      return !selectedOptions.some(
        (selected, selectedIndex) =>
          selected?.key?.label === option.label &&
          selected?.key?.group === option.group &&
          selectedIndex !== index
      );
    });
  };

  return (
    <>
      <>
        <form onSubmit={handleSubmit(onSubmit)}>
          <Accordion
            variant="outlined"
            expanded={expanded === "bmDataPanel"}
            onChange={handleChange("bmDataPanel")}
            data-cy="bmData"
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
                        <Typography
                          fontWeight="600"
                          className="text-color-green"
                        >
                          Business Metadata
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
                              setExpanded("bmDataPanel");
                              setAddLabel(false);
                              if (!isEmpty(defaultFieldValues)) {
                                reset({ businessMetadata: defaultFieldValues });
                              }
                            }}
                          >
                            {!isEmpty(bmAttributes) ? "Edit" : "Add"}
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
              <Stack gap={"1rem"}>
                {addLabel ? (
                  <>
                    {loading == undefined || loading ? (
                      <SkeletonLoader count={3} animation="wave" />
                    ) : !isEmpty(bmAttributesData) ? (
                      bmAttributesData.map((obj) => {
                        return (
                          <Stack direction="column">
                            <Accordion defaultExpanded className="m-b-1">
                              <AccordionSummary
                                className="custom-accordion-summary"
                                aria-controls="User-defined properties-content"
                                id="User-defined properties-header"
                              >
                                <Stack
                                  direction="row"
                                  alignItems="center"
                                  flex="1"
                                >
                                  <div className="properties-panel-name">
                                    <Typography
                                      fontWeight="600"
                                      className="text-color-green"
                                    >
                                      <Stack
                                        direction="row"
                                        alignItems="center"
                                        flex="1"
                                      >
                                        <div className="properties-panel-name">
                                          <Typography
                                            fontWeight="600"
                                            className="text-color-green"
                                          >
                                            {
                                              obj[
                                                "__internal_UI_businessMetadataName"
                                              ]
                                            }
                                          </Typography>
                                        </div>
                                      </Stack>{" "}
                                    </Typography>{" "}
                                  </div>
                                </Stack>
                              </AccordionSummary>

                              {Object.entries(obj).map(([key, value]: any) => {
                                return (
                                  <>
                                    {key !=
                                      "__internal_UI_businessMetadataName" && (
                                      <AccordionDetails
                                        sx={{ padding: "4px 16px" }}
                                      >
                                        <Stack
                                          direction="row"
                                          spacing={4}
                                          marginBottom={1}
                                          marginTop={1}
                                        >
                                          <div
                                            style={{
                                              flex: "0 0 8em",
                                              wordBreak: "break-all",
                                              textAlign: "left",
                                              fontWeight: "400"
                                            }}
                                          >
                                            <Typography fontWeight="600">{`${key} (${value.typeName})`}</Typography>
                                          </div>
                                          <div
                                            style={{
                                              flex: 1,
                                              wordBreak: "break-all",
                                              textAlign: "left"
                                            }}
                                          >
                                            {value.typeName == "string" ? (
                                              <HtmlRenderer
                                                htmlString={value.value}
                                              />
                                            ) : (
                                              renderValues(value)
                                            )}
                                          </div>
                                        </Stack>
                                      </AccordionDetails>
                                    )}
                                  </>
                                );
                              })}
                            </Accordion>
                          </Stack>
                        );
                      })
                    ) : (
                      <Stack
                        direction="row"
                        alignItems="center"
                        flexWrap="wrap"
                        gap="0.5rem"
                        spacing={1}
                        justifyContent="center"
                      >
                        <span>
                          No properties have been created yet. To add a
                          property, click{" "}
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
                      </Stack>
                    )}
                  </>
                ) : (
                  <>
                    <LightTooltip title={"Add New Attribute"}>
                      <CustomButton
                        sx={{
                          alignSelf: "flex-start"
                        }}
                        variant="outlined"
                        size="small"
                        onClick={(e: any) => {
                          e.stopPropagation();
                          append(defaultField);
                        }}
                        startIcon={<AddIcon fontSize="small" />}
                      >
                        Add New Attributes
                      </CustomButton>
                    </LightTooltip>
                    {fields.map((field, index) => {
                      return (
                        <Stack
                          gap="0.5rem"
                          direction="row"
                          key={field?.id}
                          alignItems="center"
                        >
                          <Controller
                            control={control}
                            name={`businessMetadata.${index}.key` as const}
                            key={`businessMetadata.${index}.key` as const}
                            defaultValue={field.key}
                            render={({ field: { onChange, value } }) => (
                              <>
                                {field.key == null || isEmpty(bmAttributes) ? (
                                  <Autocomplete
                                    disableClearable={true}
                                    freeSolo
                                    size="small"
                                    value={isEmpty(value) ? null : value}
                                    options={getAvailableOptions(index)}
                                    onChange={(_, selectedOption) => {
                                      onChange(selectedOption);
                                    }}
                                    sx={{ flexBasis: "35%" }}
                                    getOptionLabel={(option) => option.label}
                                    groupBy={(option) => option.group}
                                    noOptionsText="No results found"
                                    renderOption={(props, option, _state) => (
                                      <MenuItem {...props} key={option.label}>
                                        {`${option.label} (${option.typeName})`}
                                      </MenuItem>
                                    )}
                                    // className="advanced-search-autocomplete"
                                    className="bmattributes-key"
                                    renderInput={(params) => {
                                      return (
                                        <TextField
                                          {...params}
                                          placeholder="Select Attribute"
                                          InputProps={{
                                            ...params.InputProps,
                                            type: "search"
                                          }}
                                        />
                                      );
                                    }}
                                  />
                                ) : (
                                  <InputLabel className="form-textfield-label">
                                    {`${value.label} (${
                                      value.value || value.typeName
                                    })`}
                                  </InputLabel>
                                )}
                              </>
                            )}
                          />
                          <span style={{ margin: "12px 0" }}>:</span>
                          {isEmpty(bmAttributesValues?.[index]?.key) ? (
                            <Controller
                              control={control}
                              name={`businessMetadata.${index}.value` as const}
                              rules={{
                                required: true
                              }}
                              key={`businessMetadata.${index}.value` as const}
                              render={({
                                field: { onChange, value },
                                fieldState: { error }
                              }) => (
                                <>
                                  <div style={{ flex: "1" }}>
                                    <TextField
                                      margin="normal"
                                      error={!!error}
                                      className="form-textfield"
                                      onChange={onChange}
                                      value={value}
                                      variant="outlined"
                                      size="small"
                                      disabled
                                      sx={{
                                        "& .MuiInputBase-root.Mui-disabled": {
                                          backgroundColor: "#eee",
                                          cursor: "not-allowed"
                                        },
                                        "& .MuiOutlinedInput-root.Mui-error": {
                                          "& fieldset": {
                                            borderColor: "red"
                                          }
                                        },
                                        marginTop: "8px !important",
                                        marginBottom: "8px !important",
                                        width: "100%"
                                      }}
                                      type={"string"}
                                    />
                                  </div>
                                </>
                              )}
                            />
                          ) : (
                            <div style={{ flex: "1" }}>
                              <Stack direction="row">
                                <BMAttributesFields
                                  obj={bmAttributesValues[index].key.obj}
                                  control={control}
                                  index={index}
                                />
                              </Stack>
                            </div>
                          )}

                          <Stack direction="row" gap="0.5rem">
                            {field.key === null ? (
                              <>
                                <IconButton
                                  size="small"
                                  color="error"
                                  onClick={() => remove(index)}
                                >
                                  <RemoveOutlinedIcon fontSize="small" />
                                </IconButton>
                                <IconButton
                                  size="small"
                                  onClick={() =>
                                    append({ key: null, value: "" })
                                  }
                                >
                                  <AddOutlinedIcon fontSize="small" />
                                </IconButton>
                              </>
                            ) : (
                              <IconButton
                                size="small"
                                color="error"
                                onClick={() => remove(index)}
                              >
                                <RemoveOutlinedIcon fontSize="small" />
                              </IconButton>
                            )}
                          </Stack>
                        </Stack>
                      );
                    })}
                  </>
                )}
              </Stack>
            </AccordionDetails>
          </Accordion>
        </form>
      </>
    </>
  );
};

export default BMAttributes;
