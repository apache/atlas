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
import { Controller, useFieldArray, useForm } from "react-hook-form";
import { createOrUpdateTag } from "@api/apiMethods/typeDefApiMethods";
import { isEmpty, serverError } from "@utils/Utils";
import { toast } from "react-toastify";
import { useRef } from "react";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { CustomButton, LightTooltip } from "@components/muiComponents";
import { Stack, TextField, Select, MenuItem, IconButton } from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import ClearOutlinedIcon from "@mui/icons-material/ClearOutlined";
import { defaultDataType } from "@utils/Enum";
import { useParams } from "react-router-dom";
import { paramsType } from "@models/detailPageType";
import { fetchClassificationData } from "@redux/slice/typeDefSlices/typedefClassificationSlice";
import { AntSwitch } from "@utils/Muiutils";

const AddTagAttributes = ({ open, onClose }: any) => {
  const toastId: any = useRef(null);
  const { tagName } = useParams<paramsType>();
  const dispatchApi = useAppDispatch();
  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const { enumDefs } = enumObj.data;
  const enumType = !isEmpty(enumDefs)
    ? enumDefs.map((obj: { name: string; guid: string }) => obj.name)
    : [];

  const { classificationData }: any = useAppSelector(
    (state: any) => state.classification
  );
  const { classificationDefs } = classificationData;
  const classificationObj = !isEmpty(tagName)
    ? classificationDefs.find((obj: { name: string }) => obj.name == tagName)
    : {};

  const dataTypeSelector = [...defaultDataType, ...enumType];
  const {
    watch,
    control,
    handleSubmit,
    register,
    formState: { isSubmitting }
  } = useForm();

  const { fields, append, remove } = useFieldArray({
    control,
    name: "attributes"
  });

  const watched = watch("attributes" as any);

  const fetchInitialData = async () => {
    await dispatchApi(fetchClassificationData());
  };

  const onSubmit = async (values: any) => {
    let formValues = { ...values };
    const { attributes } = formValues;
    let formData: Record<string, any> = {
      classificationDefs: [{ ...classificationObj }],
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
    let val: Record<string, string> = {};

    val["attributeDefs"] = attributes.map(
      (obj: {
        attributeName: string;
        typeName: string;
        toggleDuplicates: boolean;
      }) => {
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
        const updatedAttributeDefs = [
          ...formData.classificationDefs[0].attributeDefs,
          newObj
        ];
        formData.classificationDefs[0].attributeDefs = updatedAttributeDefs;
        return newObj;
      }
    );

    try {
      await createOrUpdateTag("classification", false, formData);
      onClose();
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        `Classification attribute is added successfully`
      );
      fetchInitialData();
    } catch (error) {
      console.error(`Error while adding classification  attributes`, error);
      toast.dismiss(toastId.current);
      serverError(error, toastId);
    }
  };
  return (
    <>
      <CustomModal
        open={open}
        onClose={onClose}
        title={"Add Attribute"}
        button1Label="Cancel"
        button1Handler={onClose}
        button2Label="Add"
        button2Handler={handleSubmit(onSubmit)}
        disableButton2={isSubmitting}
      >
        <form onSubmit={handleSubmit(onSubmit)}>
          {/* <TagAtrributes control={control} /> */}
          <Stack marginBottom="1rem">
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

            {fields.map((field: any, index) => (
              <Stack gap="1rem" key={field.id} direction="row">
                <TextField
                  margin="normal"
                  fullWidth
                  {...register(`attributes.${index}.attributeName`)}
                  // defaultValue={field.attributeName}
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
                        name={`attributes.${index}.toggleDuplicates` as const}
                        key={`attributes.${index}.toggleDuplicates`}
                        data-cy={`attributes.${index}.toggleDuplicates`}
                        defaultValue={field?.multiValueSelect}
                        render={({ field: { value, onChange } }: any) => (
                          <>
                            <LightTooltip
                              title={value == false ? "Make LIST" : "Make SET"}
                            >
                              <AntSwitch
                                size="small"
                                {...register(
                                  `attributes.${index}.toggleDuplicates`
                                )}
                                checked={value}
                                onChange={onChange}
                                sx={{ marginRight: "4px" }}
                                inputProps={{ "aria-label": "controlled" }}
                              />
                            </LightTooltip>
                          </>
                        )}
                      />
                    )}
                </div>

                <IconButton
                  aria-label="back"
                  size="small"
                  color="error"
                  disableRipple
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
                  <ClearOutlinedIcon fontSize="small" />
                </IconButton>
              </Stack>
            ))}
            {/* <TagAtrributes control={control} /> */}
          </Stack>
        </form>
      </CustomModal>
    </>
  );
};

export default AddTagAttributes;
