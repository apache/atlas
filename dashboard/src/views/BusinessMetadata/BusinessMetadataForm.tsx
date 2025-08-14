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
 * limitations under the License
 */

import { createEditBusinessMetadata } from "@api/apiMethods/typeDefApiMethods";
import { CustomButton, LightTooltip } from "@components/muiComponents";
import Button from "@mui/material/Button";
import CardActions from "@mui/material/CardActions";
import CardHeader from "@mui/material/CardHeader";
import Card from "@mui/material/Card";
import InputLabel from "@mui/material/InputLabel";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Typography from "@mui/material/Typography";
import Grid from "@mui/material/Grid";
import Divider from "@mui/material/Divider";
import CircularProgress from "@mui/material/CircularProgress";
import CardContent from "@mui/material/CardContent";
import { isEmpty, serverError } from "@utils/Utils";
import { useEffect, useRef, useState } from "react";
import { Controller, useFieldArray, useForm } from "react-hook-form";
import ReactQuill from "react-quill-new";
import { toast } from "react-toastify";
import AddIcon from "@mui/icons-material/Add";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import BusinessMetadataAttributeForm from "./BusinessMetadataAtrributeForm";
import { setEditBMAttribute } from "@redux/slice/createBMSlice";
import { cloneDeep } from "@utils/Helper";
import { fetchBusinessMetaData } from "@redux/slice/typeDefSlices/typedefBusinessMetadataSlice";
import { defaultType } from "@utils/Enum";

const BusinessMetaDataForm = ({
  setForm,
  setBMAttribute,
  bmAttribute
}: any) => {
  const { editbmAttribute }: any = useAppSelector((state) => state.createBM);
  const toastId: any = useRef(null);
  const [alignment, setAlignment] = useState<string>("formatted");
  const { typeHeaderData } = useAppSelector((state: any) => state.typeHeader);
  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const { enumDefs } = enumObj.data || {};
  let enumTypes = !isEmpty(enumDefs)
    ? enumDefs.map((obj: { name: any }) => {
        return obj.name;
      })
    : [];
  const { name }: any = bmAttribute;
  const { name: editAttrName, typeName, options }: any = editbmAttribute;

  const str = !isEmpty(typeName) ? typeName : "";
  const start = !isEmpty(str) ? str.indexOf("<") + 1 : "";
  const end = !isEmpty(str) ? str.indexOf(">") : "";
  const extracted = !isEmpty(str) ? str.slice(start, end) : "";
  let currentTypeName =
    str.indexOf("<") != -1
      ? defaultType.includes(typeName)
        ? extracted
        : "enumeration"
      : defaultType.includes(typeName)
      ? typeName
      : "enumeration";
  let selectedEnumObj = !isEmpty(enumDefs)
    ? enumDefs.find((obj: { name: any }) => {
        return obj.name == typeName;
      })
    : {};
  let selectedEnumValues = !isEmpty(selectedEnumObj)
    ? selectedEnumObj?.elementDefs
    : [];

  let enumTypeOptions = [...selectedEnumValues];
  const editDefaultObj = [
    {
      ...(editbmAttribute || {}),
      options: {
        ...(options || {}),
        applicableEntityTypes: !isEmpty(options)
          ? (function () {
              try {
                return JSON.parse(options?.applicableEntityTypes);
              } catch (e) {
                return options?.applicableEntityTypes;
              }
            })()
          : []
      },
      typeName:
        str.indexOf("<") != -1
          ? defaultType.includes(typeName)
            ? extracted
            : "enumeration"
          : defaultType.includes(typeName)
          ? typeName
          : "enumeration",
      ...(currentTypeName == "enumeration" && { enumType: typeName }),
      ...(currentTypeName == "enumeration" && {
        enumValues: enumTypeOptions
      }),
      multiValueSelect: str.indexOf("<") != -1 ? true : false
    }
  ];
  const dispatchState = useAppDispatch();
  const {
    control,
    handleSubmit,
    setValue,
    watch,
    reset,
    formState: { isSubmitting }
  } = useForm<any>({
    ...((!isEmpty(bmAttribute) || !isEmpty(editbmAttribute)) && {
      defaultValues: {
        attributeDefs: [
          !isEmpty(bmAttribute) && isEmpty(editbmAttribute)
            ? {
                name: "",
                typeName: "string",
                searchWeight: 5,
                multiValueSelect: false,
                options: {
                  maxStrLength: 50,
                  applicableEntityTypes: null
                },
                isOptional: true,
                cardinality: "SINGLE",
                valuesMinCount: 0,
                valuesMaxCount: 1,
                isUnique: false,
                isIndexable: true
              }
            : editDefaultObj
        ]
      }
    })
  });

  const { fields, append, remove } = useFieldArray({
    control,
    name: "attributeDefs"
  });

  useEffect(() => {
    let currentTypeName =
      str.indexOf("<") != -1
        ? defaultType.includes(typeName)
          ? extracted
          : "enumeration"
        : defaultType.includes(typeName)
        ? typeName
        : "enumeration";
    let selectedEnumObj = enumDefs.find((obj: { name: any }) => {
      return obj.name == typeName;
    });
    let selectedEnumValues = !isEmpty(selectedEnumObj)
      ? selectedEnumObj?.elementDefs
      : [];

    let enumTypeOptions = [...selectedEnumValues];

    const editObj = [
      {
        ...(editbmAttribute || {}),
        options: {
          ...(options || {}),
          applicableEntityTypes: !isEmpty(options)
            ? (function () {
                try {
                  return JSON.parse(options?.applicableEntityTypes);
                } catch (e) {
                  return options?.applicableEntityTypes;
                }
              })()
            : []
        },
        typeName: currentTypeName,
        ...(currentTypeName == "enumeration" && { enumType: typeName }),
        ...(currentTypeName == "enumeration" && {
          enumValues: enumTypeOptions
        }),
        multiValueSelect: str.indexOf("<") != -1 ? true : false
      }
    ];
    if (!isEmpty(editbmAttribute)) {
      reset({
        attributeDefs: editObj || []
      });
    }
  }, [editbmAttribute]);

  const dataTypeOptions = !isEmpty(typeHeaderData)
    ? typeHeaderData
        .map((obj: { category: string; name: any }) => {
          if (obj.category == "ENTITY") {
            return obj.name;
          }
        })
        .filter(Boolean)
    : [];
  const watched = watch("attributeDefs" as any);
  const handleChange = (
    event: React.MouseEvent<HTMLElement>,
    newAlignment: string
  ) => {
    event?.stopPropagation();
    setAlignment(newAlignment);
  };

  const toastMssg = (bmName: string) => {
    if (isEmpty(bmAttribute && isEmpty(editbmAttribute))) {
      toast.success(`Business Metadata ${bmName} was created successfully`);
    } else {
      toast.success(
        "One or more Business Metadata attributes were updated successfully"
      );
    }
  };

  const onSubmit = async (values: any) => {
    let formData = { ...values };
    let bmData = cloneDeep(bmAttribute);

    const { name, description, attributeDefs } = formData;
    if (isEmpty(bmAttribute) && isEmpty(editAttrName) && isEmpty(name)) {
      toast.info("Please enter the Enumeration name");
      return;
    }
    let attributeDefsData = !isEmpty(attributeDefs) ? [...attributeDefs] : [];

    let attributes = !isEmpty(attributeDefsData)
      ? attributeDefsData.map((item) => ({
          ...item,
          ...{
            options: {
              applicableEntityTypes: JSON.stringify(
                item.options.applicableEntityTypes
              ),
              maxStrLength: item.options.maxStrLength
            },
            typeName: item.multiValueSelect
              ? item.typeName == "enumeration"
                ? item.enumType
                : `array<${item.typeName}>`
              : item.typeName == "enumeration"
              ? item.enumType
              : `array<${item.typeName}>`
          }
        }))
      : [];

    let data = {
      structDefs: [],
      enumDefs: [],
      classificationDefs: [],
      entityDefs: [],
      businessMetadataDefs: [
        isEmpty(bmAttribute) && isEmpty(editbmAttribute)
          ? {
              category: "BUSINESS_METADATA",
              createdBy: "admin",
              updatedBy: "admin",
              version: 1,
              typeVersion: "1.1",
              name: name.trim(),
              description: description ? description.trim() : "",
              attributeDefs: attributes
            }
          : !isEmpty(bmAttribute) && isEmpty(editbmAttribute)
          ? {
              ...bmAttribute,
              attributeDefs: [...bmAttribute.attributeDefs, ...attributes]
            }
          : {
              ...bmData,
              attributeDefs: bmData?.attributeDefs?.map((obj: any) => {
                const matchingAttribute = attributes.find(
                  (attr: any) => attr.name === obj.name
                );
                return matchingAttribute ? matchingAttribute : obj;
              })
            }
      ]
    };

    try {
      const response = await createEditBusinessMetadata(
        "business_metadata",
        isEmpty(bmAttribute) && isEmpty(editAttrName) ? "POST" : "PUT",
        data
      );
      let bmName = response?.data?.businessMetadataDefs?.[0]?.name;
      toastMssg(bmName);
      dispatchState(fetchBusinessMetaData());
      setBMAttribute({});
      setForm(false);
    } catch (error) {
      console.log(
        `Error occur while creating or updating BusinessMetadata`,
        error
      );
      serverError(error, toastId);
    }
  };

  const bmTitle = () => {
    if (isEmpty(bmAttribute) && isEmpty(editbmAttribute)) {
      return "Create Business Metadata";
    } else if (!isEmpty(bmAttribute) && isEmpty(editbmAttribute)) {
      return `Add Business Metadata Attribute for:
               ${name}`;
    } else if (!isEmpty(editbmAttribute)) {
      return `Update Attribute of: ${editAttrName}`;
    }
  };

  return (
    <>
      <Card sx={{ border: 0, height: "100%" }} variant="outlined">
        <CardHeader
          title={
            <>
              <Typography textAlign="left" fontSize={18} fontWeight={600}>
                {bmTitle()}
              </Typography>
              <Divider />
            </>
          }
        />
        <CardContent sx={{ textAlign: "left" }}>
          <form onSubmit={handleSubmit(onSubmit)}>
            <Grid container spacing={2}>
              {isEmpty(bmAttribute) && isEmpty(editbmAttribute) && (
                <>
                  <Controller
                    control={control}
                    name={"name"}
                    rules={{
                      required: true
                    }}
                    render={({
                      field: { onChange, value },
                      fieldState: { error }
                    }) => (
                      <>
                        <Grid container spacing={2}>
                          <Grid
                            textAlign="right"
                            alignItems="center"
                            item
                            md={3}
                          >
                            <InputLabel className="form-textfield" required>
                              Name
                            </InputLabel>
                          </Grid>
                          <Grid item md={6}>
                            <TextField
                              margin="normal"
                              error={!!error}
                              fullWidth
                              value={value}
                              onChange={(e) => {
                                const value = e.target.value;
                                onChange(value);
                              }}
                              variant="outlined"
                              size="small"
                              placeholder={"Name required"}
                              className="form-textfield"
                            />
                          </Grid>
                        </Grid>
                      </>
                    )}
                  />
                  <Controller
                    control={control}
                    name={"description"}
                    render={({ field }) => (
                      <>
                        <Grid container spacing={2}>
                          <Grid item textAlign="right" md={3}>
                            <InputLabel>Description</InputLabel>
                          </Grid>
                          <Grid item md={6}>
                            <Stack gap={1} marginBottom="1.5rem">
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
                              {alignment == "formatted" ? (
                                <div style={{ position: "relative" }}>
                                  <ReactQuill
                                    {...field}
                                    theme="snow"
                                    placeholder={"Description required"}
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
                                  placeholder={"Long Description"}
                                  onChange={(e) => {
                                    e.stopPropagation();
                                    const value = e.target.value;
                                    field.onChange(value);
                                    setValue("description", value);
                                  }}
                                  style={{ width: "100%" }}
                                  className="form-textfield form-textarea-field"
                                />
                              )}
                            </Stack>
                          </Grid>
                        </Grid>
                      </>
                    )}
                  />
                </>
              )}
              <Grid container spacing={2}>
                <Grid item md={3}></Grid>
                {isEmpty(editAttrName) && (
                  <Grid item md={6}>
                    <Stack marginBottom="1rem">
                      <LightTooltip title={"Add Business Metadata Attribute"}>
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

                            append({
                              name: "",
                              typeName: "string",
                              searchWeight: 5,
                              multiValueSelect: false,
                              options: {
                                maxStrLength: 50,
                                applicableEntityTypes: null
                              },
                              isOptional: true,
                              cardinality: "SINGLE",
                              valuesMinCount: 0,
                              valuesMaxCount: 1,
                              isUnique: false,
                              isIndexable: true
                            });
                          }}
                          startIcon={<AddIcon />}
                        >
                          Add Business Metadata Attribute
                        </CustomButton>
                      </LightTooltip>
                    </Stack>
                  </Grid>
                )}
              </Grid>
              <Grid container gap={2} justifyContent="center">
                <Grid item md={9}>
                  <Stack>
                    <BusinessMetadataAttributeForm
                      fields={fields}
                      control={control}
                      remove={remove}
                      watched={watched}
                      dataTypeOptions={dataTypeOptions}
                      enumTypes={enumTypes}
                      watch={watch}
                      setValue={setValue}
                    />
                  </Stack>
                </Grid>
              </Grid>
            </Grid>
            <Divider />
            <Stack
              direction="row"
              gap={2}
              justifyContent="center"
              paddingTop="1rem"
            >
              <CustomButton
                variant="outlined"
                color="primary"
                onClick={(_e: Event) => {
                  setForm(false);
                  setBMAttribute({});
                  dispatchState(setEditBMAttribute({}));
                }}
              >
                Cancel
              </CustomButton>
              <Button
                startIcon={
                  isSubmitting && (
                    <CircularProgress
                      color="success"
                      sx={{ fontWeight: "600" }}
                      size="20px"
                    />
                  )
                }
                type="submit"
                variant="contained"
                color="primary"
                disabled={isSubmitting ? true : false}
              >
                {isEmpty(bmAttribute) && isEmpty(editAttrName)
                  ? "Create"
                  : "Save"}
              </Button>
            </Stack>
          </form>
        </CardContent>
        <CardActions disableSpacing></CardActions>
      </Card>
    </>
  );
};

export default BusinessMetaDataForm;
