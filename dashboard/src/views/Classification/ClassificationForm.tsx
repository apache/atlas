//@ts-nocheck

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
 * limitations under the License.useForm
 */

import CustomModal from "@components/Modal";
import { CustomButton, LightTooltip } from "@components/muiComponents";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import {
  Autocomplete,
  CircularProgress,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  SelectChangeEvent,
  Stack,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Typography
} from "@mui/material";
import { Controller, useFieldArray, useForm } from "react-hook-form";
import AddIcon from "@mui/icons-material/Add";
import ClearOutlinedIcon from "@mui/icons-material/ClearOutlined";
import ReactQuill from "react-quill-new";
import { useEffect, useRef, useState } from "react";
import { isEmpty, sanitizeHtmlContent, serverError } from "@utils/Utils";
import { defaultDataType } from "@utils/Enum";
import { createOrUpdateTag } from "@api/apiMethods/typeDefApiMethods";
import { toast } from "react-toastify";
import { useParams } from "react-router-dom";
import { paramsType } from "@models/detailPageType";
import TagAtrributes from "./TagAttributes";
import { fetchClassificationData } from "@redux/slice/typeDefSlices/typedefClassificationSlice";
import { AntSwitch } from "@utils/Muiutils";

const ClassificationForm = ({
  open,
  onClose,
  setTagModal,
  isAdd,
  subAdd,
  node
}: any) => {
  const dispatchApi = useAppDispatch();
  const { tagName } = useParams<paramsType>();
  const currentTagName = node?.text || tagName;
  const { classificationData }: any = useAppSelector(
    (state: any) => state.classification
  );
  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const [alignment, setAlignment] = useState<string>("formatted");
  const toastId: any = useRef(null);
  const { enumDefs } = enumObj.data;
  const classificationObj = !isEmpty(currentTagName)
    ? classificationData.classificationDefs.find(
        (obj: { name: string }) => obj.name == currentTagName
      )
    : {};

  const { name, description } = classificationObj;

  let defaultValue: Record<string, string> = {};

  defaultValue["name"] = isAdd ? "" : name;
  defaultValue["description"] = isAdd ? "" : description;

  let defaultAddValues = {};
  if (isAdd && !isEmpty(node)) {
    defaultAddValues["classifications"] = !isEmpty(node)
      ? [{ label: currentTagName, value: currentTagName }]
      : [];
  }

  const {
    control,
    handleSubmit,
    watch,
    reset,
    setValue,
    register,
    isDirty,
    formState: { isSubmitting }
  } = useForm({
    defaultValues: isAdd ? defaultAddValues : defaultValue,
    mode: "onChange",
    shouldUnregister: true
  });

  useEffect(() => {
    reset(isAdd ? defaultAddValues : defaultValue);
  }, []);

  const handleChange = (
    event: React.MouseEvent<HTMLElement>,
    newAlignment: string
  ) => {
    event?.stopPropagation();
    setAlignment(newAlignment);
  };

  const nameValue = watch("name", "");
  const descriptionValue = watch("description", "");

  const { fields, append, remove } = useFieldArray({
    control,
    name: "attributes"
  });
  const watched = watch("attributes" as any);

  const options = !isEmpty(classificationData)
    ? classificationData.classificationDefs.map((obj: { name: any }) => ({
        label: obj.name,
        value: obj.name
      }))
    : [];

  const enumType = !isEmpty(enumDefs)
    ? enumDefs.map((obj: { name: string; guid: string }) => obj.name)
    : [];

  const dataTypeSelector = [...defaultDataType, ...enumType];

  const fetchInitialData = async () => {
    await dispatchApi(fetchClassificationData());
  };

  const onSubmit = async (data: any) => {
    const formValues: any = data;
    const { name, description, classifications, attributes } = formValues;
    let formData: Record<string, any> = {
      classificationDefs: [],
      entityDefs: [],
      enumDefs: [],
      structDefs: []
    };
    let attributesObj = {
      name: "",
      typeName: "string",
      isOptional: true,
      cardinality: "SINGLE",
      valuesMinCount: 0,
      valuesMaxCount: 1,
      isUnique: false,
      isIndexable: true
    };
    let classification = { ...classificationObj };
    let val: Record<string, string> = {};
    if (isAdd) {
      val["name"] = name;
      val["description"] = description;
      val["superTypes"] = !isEmpty(classifications)
        ? classifications?.map((obj: { label: string }) => obj.label)
        : [];

      val["attributeDefs"] = attributes?.map((obj) => {
        const { attributeName, typeName, toggleDuplicates } = obj;

        const newObj = {
          ...attributesObj,
          ...{
            cardinality: !isEmpty(toggleDuplicates)
              ? toggleDuplicates
                ? "LIST"
                : "SET"
              : "SINGLE"
          }
        };

        newObj.name = attributeName;
        newObj.typeName = !isEmpty(typeName) ? typeName : "string";

        return newObj;
      });
    } else {
      classification["description"] = description;
    }

    formData.classificationDefs.push(isAdd ? val : classification);

    try {
      await createOrUpdateTag("classification", isAdd, formData);
      onClose();
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        `Classification ${name} was ${
          isAdd ? "created" : "updated"
        } successfully`
      );
      fetchInitialData();
    } catch (error) {
      console.error(
        `Error while ${isAdd ? "creating" : "updating"} classification`,
        error
      );
      toast.dismiss(toastId.current);
      serverError(error, toastId);
    }
  };

  return (
    <>
      <CustomModal
        open={open}
        onClose={onClose}
        title={isAdd ? "Create a new classification" : "Edit Classification"}
        button1Label="Cancel"
        button1Handler={onClose}
        button2Label={isAdd ? "Create" : "Save"}
        disableButton2={isSubmitting}
        isDirty={isDirty}
        maxWidth="sm"
        button2Handler={handleSubmit(onSubmit)}
      >
        {
          <Stack>
            <form onSubmit={handleSubmit(onSubmit)}>
              <Stack marginBottom="2.5rem">
                <Controller
                  control={control}
                  name={"name"}
                  rules={{
                    required: true
                  }}
                  defaultValue={null}
                  render={({ field: { onChange }, fieldState: { error } }) => (
                    <>
                      <InputLabel required>Name</InputLabel>

                      <TextField
                        margin="normal"
                        error={!!error}
                        fullWidth
                        onChange={(e) => {
                          e.stopPropagation();
                          const value = e.target.value;
                          onChange(value);
                          setValue("description", value);
                        }}
                        sx={{
                          "&.Mui-disabled": {
                            backgroundColor: "#eee !important",
                            cursor: "not-allowed"
                          }
                        }}
                        disabled={isAdd ? false : true}
                        value={nameValue}
                        variant="outlined"
                        size="small"
                        placeholder={"Name required"}
                        className="form-textfield"
                      />
                    </>
                  )}
                />

                <Controller
                  control={control}
                  name={"description"}
                  rules={{
                    required: true
                  }}
                  defaultValue={null}
                  render={({ field }) => (
                    <Stack gap="0.5rem">
                      <Stack
                        direction="row"
                        justifyContent="space-between"
                        alignItems="center"
                      >
                        <InputLabel required>Description</InputLabel>
                        <ToggleButtonGroup
                          size="small"
                          color="primary"
                          value={alignment}
                          exclusive
                          onChange={(e, newValue) => {
                            e.stopPropagation();
                            handleChange(e, newValue);
                          }}
                          aria-label="Platform"
                        >
                          <ToggleButton
                            className="entity-form-toggle-btn"
                            value="formatted"
                            data-cy="formatted"
                          >
                            Formatted Text
                          </ToggleButton>
                          <ToggleButton
                            value="plain"
                            className="entity-form-toggle-btn"
                            data-cy="plain"
                          >
                            Plain text
                          </ToggleButton>
                        </ToggleButtonGroup>
                      </Stack>
                      {alignment == "formatted" ? (
                        <div style={{ position: "relative" }}>
                          <ReactQuill
                            {...field}
                            theme="snow"
                            placeholder={"Description required"}
                            value={descriptionValue}
                            onChange={(text) => {
                              field.onChange(text);
                              setValue("description", text);
                            }}
                            className="classification-form-editor"
                          />
                        </div>
                      ) : (
                        <textarea
                          {...field}
                          className="form-textarea-field"
                          placeholder={"Long Description"}
                          value={sanitizeHtmlContent(descriptionValue)}
                          onChange={(e) => {
                            e.stopPropagation();
                            const value = e.target.value;
                            field.onChange(value);
                            setValue("description", value);
                          }}
                          style={{ width: "100%" }}
                        />
                      )}
                    </Stack>
                  )}
                />
              </Stack>
              {isAdd && (
                <Stack marginBottom="1rem">
                  <Controller
                    control={control}
                    name={"classifications"}
                    render={({
                      field: { onChange, value },
                      fieldState: { error }
                    }) => {
                      return (
                        <>
                          <div>
                            <Typography fontWeight={600} className="pb-1">
                              Select classification to inherit
                              attributes(optional)
                            </Typography>
                            <InputLabel>
                              Attributes define additional properties for the
                              classification
                            </InputLabel>
                          </div>
                          <Autocomplete
                            multiple
                            size="small"
                            onChange={(e, item) => {
                              e.stopPropagation();
                              onChange(item);
                            }}
                            value={value}
                            filterSelectedOptions
                            getOptionLabel={(option: { label: string }) =>
                              option.label
                            }
                            isOptionEqualToValue={(option, value) =>
                              option.label === value.label
                            }
                            options={options}
                            className="form-autocomplete-field"
                            renderInput={(params) => (
                              <TextField
                                {...params}
                                className="form-textfield"
                                size="small"
                                InputProps={{
                                  ...params.InputProps
                                }}
                                placeholder={`Search Classification`}
                              />
                            )}
                          />
                        </>
                      );
                    }}
                  />
                </Stack>
              )}
              {isAdd && (
                <Stack marginBottom="1rem">
                  <Typography fontWeight={600} className="pb-1">
                    Attributes (optional)
                  </Typography>

                  <CustomButton
                    sx={{
                      alignSelf: "flex-start",
                      marginBottom: "1rem",
                      height: "32px"
                    }}
                    variant="outlined"
                    size="small"
                    onClick={(e: any) => {
                      e.stopPropagation();
                      append({ attributeName: "", typeName: "" });
                    }}
                    startIcon={<AddIcon />}
                  >
                    Add New Attributes
                  </CustomButton>

                  {fields.map((field, index) => (
                    <Stack gap="1rem" key={field.id} direction="row">
                      <TextField
                        margin="normal"
                        fullWidth
                        {...register(`attributes.${index}.attributeName`)}
                        defaultValue={field.attributeName}
                        variant="outlined"
                        size="small"
                        placeholder={"Attribute Name"}
                        className="form-textfield"
                        sx={{
                          marginTop: "8px !important",
                          marginBottom: "8px !important"
                        }}
                      />

                      <div
                        style={{
                          width: "100%",
                          display: "flex",
                          alignItems: "center",
                          gap: "0.5rem"
                        }}
                      >
                        <Select
                          fullWidth
                          size="small"
                          defaultValue={"string"}
                          id="demo-select-small"
                          {...register(`attributes.${index}.typeName`)}
                          className="form-textfield"
                          sx={{
                            marginTop: "8px !important",
                            marginBottom: "8px !important"
                          }}
                        >
                          {dataTypeSelector.map((type) => (
                            <MenuItem key={type} value={type}>
                              {type}
                            </MenuItem>
                          ))}
                        </Select>
                        {watched?.[index] &&
                          watched?.[index]?.typeName == "array<string>" && (
                            <Controller
                              control={control}
                              name={
                                `attributes.${index}.toggleDuplicates` as const
                              }
                              data-cy={`attributes.${index}.toggleDuplicates`}
                              defaultValue={field?.toggleDuplicates}
                              render={({ field: { value, onChange } }: any) => (
                                <>
                                  <LightTooltip
                                    title={
                                      value == false ? "Make LIST" : "Make SET"
                                    }
                                  >
                                    <AntSwitch
                                      size="small"
                                      {...register(
                                        `attributes.${index}.toggleDuplicates`
                                      )}
                                      checked={value}
                                      onChange={onChange}
                                      sx={{ marginRight: "4px" }}
                                      inputProps={{
                                        "aria-label": "controlled"
                                      }}
                                    />
                                  </LightTooltip>
                                </>
                              )}
                            />
                          )}
                      </div>

                      <IconButton
                        aria-label="back"
                        color="error"
                        sx={{
                          display: "inline-flex",
                          position: "relative",
                          padding: "4px",
                          marginLeft: "4px"
                        }}
                        onClick={(e) => {
                          e.stopPropagation();
                          remove(index);
                        }}
                      >
                        <ClearOutlinedIcon sx={{ fontSize: "1.25rem" }} />
                      </IconButton>
                    </Stack>
                  ))}
                  {/* <TagAtrributes control={control} /> */}
                </Stack>
              )}
            </form>
          </Stack>
        }
      </CustomModal>
    </>
  );
};

export default ClassificationForm;
