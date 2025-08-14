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
  Button,
  Card,
  CardActions,
  CardContent,
  CardHeader,
  CircularProgress,
  Divider,
  Grid,
  Stack,
  Typography
} from "@mui/material";
import DetailPageAttribute from "../DetailPageAttributes";
import { useParams } from "react-router-dom";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { CustomButton, LightTooltip } from "@components/muiComponents";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";
import { isEmpty, serverError } from "@utils/Utils";
import BusinessMetadataAtrribute from "./BusinessMetadataAtrribute";
import { useRef, useState } from "react";
import BusinessMetadataAttributeForm from "@views/BusinessMetadata/BusinessMetadataAtrributeForm";
import { useFieldArray, useForm } from "react-hook-form";
import AddIcon from "@mui/icons-material/Add";
import { setEditBMAttribute } from "@redux/slice/createBMSlice";
// import { fetchBusinessMetaData } from "@redux/actions/typedefActions/typedefBusinessmetadataAction";
import { toast } from "react-toastify";
import { createEditBusinessMetadata } from "@api/apiMethods/typeDefApiMethods";
import { cloneDeep } from "@utils/Helper";
import { defaultAttrObj, defaultType } from "@utils/Enum";
import { fetchBusinessMetaData } from "@redux/slice/typeDefSlices/typedefBusinessMetadataSlice";

const BusinessMetadataDetailsLayout = () => {
  const { bmguid } = useParams();
  const dispatchState = useAppDispatch();
  const { businessMetaData, loading }: any = useAppSelector(
    (state: any) => state.businessMetaData
  );

  const [form, setForm] = useState<boolean>(false);
  const [bmAttribute, setBMAttribute] = useState({});
  const { businessMetadataDefs } = businessMetaData || {};

  const businessmetaDataObj = !isEmpty(businessMetadataDefs)
    ? businessMetadataDefs.find((obj: { guid: string }) => obj.guid == bmguid)
    : {};

  const { description, attributeDefs, name } = businessmetaDataObj;

  const { editbmAttribute }: any = useAppSelector((state) => state.createBM);
  const toastId: any = useRef(null);
  const { typeHeaderData } = useAppSelector((state: any) => state.typeHeader);
  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const { enumDefs } = enumObj.data || {};
  let enumTypes = !isEmpty(enumDefs)
    ? enumDefs.map((obj: { name: any }) => {
        return obj.name;
      })
    : [];
  const dataTypeOptions = !isEmpty(typeHeaderData)
    ? typeHeaderData
        .map((obj: { category: string; name: any }) => {
          if (obj.category == "ENTITY") {
            return obj.name;
          }
        })
        .filter(Boolean)
    : [];
  const { typeName, options }: any = editbmAttribute;
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

  const {
    control,
    handleSubmit,
    reset,
    watch,
    setValue,
    formState: { isSubmitting }
  } = useForm<any>({
    defaultValues: {
      attributeDefs: [!isEmpty(editbmAttribute) ? editObj : defaultAttrObj]
    }
  });
  const { fields, append, remove } = useFieldArray({
    control,
    name: "attributeDefs"
  });

  const watched = watch("attributeDefs" as any);

  const bmTitle = () => {
    if (isEmpty(bmAttribute) && isEmpty(editbmAttribute)) {
      return `Add Business Metadata Attribute for:
               ${name}`;
    } else if (!isEmpty(editbmAttribute)) {
      return `Update Attribute of: ${name}`;
    }
  };

  const onSubmit = async (values: any) => {
    let formData = { ...values };
    const { attributeDefs: formAttributes } = formData;

    let bmData = cloneDeep(businessmetaDataObj);
    const { attributeDefs } = bmData;

    let attributeDefsData = [...formAttributes];

    let attributes = attributeDefsData.map((item) => {
      const { multiValueSelect, ...rest } = item;

      return {
        ...rest,
        ...{
          options: {
            applicableEntityTypes: JSON.stringify(
              rest.options.applicableEntityTypes
            ),
            maxStrLength: rest.options.maxStrLength
          },
          typeName: rest.multiValueSelect
            ? rest.typeName == "enumeration"
              ? rest.enumType
              : `array<${rest.typeName}>`
            : rest.typeName == "enumeration"
            ? rest.enumType
            : `array<${rest.typeName}>`
        }
      };
    });

    const formDataAttributes = isEmpty(editbmAttribute)
      ? [...attributeDefs, ...attributes]
      : attributeDefs.map((obj: any) => {
          if (obj.name == attributes?.[0].name) {
            const currentObj = attributes?.[0];
            return currentObj;
          }
          return obj;
        });

    const data = {
      structDefs: [],
      enumDefs: [],
      classificationDefs: [],
      entityDefs: [],
      businessMetadataDefs: [
        {
          ...bmData,
          attributeDefs: formDataAttributes
        }
      ]
    };

    try {
      await createEditBusinessMetadata("business_metadata", "PUT", data);
      dispatchState(fetchBusinessMetaData());
      toast.success(
        "One or more Business Metadata attributes were updated successfully"
      );
      setForm(false);
      setBMAttribute({});
      dispatchState(setEditBMAttribute({}));
    } catch (error) {
      console.log(
        `Error occur while creating or updating BusinessMetadata attributes`,
        error
      );
      serverError(error, toastId);
    }
  };

  return (
    <Stack direction="column">
      <DetailPageAttribute
        paramsAttribute={bmguid}
        data={businessmetaDataObj}
        description={description}
        loading={loading}
      />
      <Stack gap={2} marginTop={2}>
        {!form && (
          <Stack>
            <div style={{ height: "24px" }}>
              <LightTooltip title={"Attributes"}>
                <CustomButton
                  variant="contained"
                  sx={{ position: "relative" }}
                  className="table-filter-btn  align-self-end"
                  size="small"
                  onClick={(_e: any) => {
                    setForm(true);
                    reset({ attributeDefs: [defaultAttrObj] });
                    setBMAttribute({});
                    dispatchState(setEditBMAttribute({}));
                  }}
                  startIcon={<AddOutlinedIcon />}
                  data-cy="bm-attributes"
                >
                  Attributes
                </CustomButton>
              </LightTooltip>
            </div>
          </Stack>
        )}
        {form ? (
          <Card>
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
                  <Grid container spacing={2}>
                    <Grid item md={3}></Grid>
                    {isEmpty(bmAttribute) && (
                      <Grid item md={6}>
                        <Stack marginBottom="1rem">
                          <LightTooltip
                            title={"Add Business Metadata Attribute"}
                          >
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
                                append(defaultAttrObj);
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
                      reset({ attributeDefs: [defaultAttrObj] });
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
                    {"Save"}
                  </Button>
                </Stack>
              </form>
            </CardContent>
            <CardActions disableSpacing></CardActions>
          </Card>
        ) : (
          <BusinessMetadataAtrribute
            componentProps={{
              attributeDefs: attributeDefs,
              loading: loading,
              setForm: setForm,
              setBMAttribute: setBMAttribute,
              reset: reset
            }}
          />
        )}
      </Stack>
    </Stack>
  );
};

export default BusinessMetadataDetailsLayout;
