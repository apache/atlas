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

import { getEntitiesType } from "@api/apiMethods/entitiesApiMethods";
import {
  createEntity,
  getEntity,
  getTypedef
} from "@api/apiMethods/entityFormApiMethod";
import FormAutocomplete from "@components/Forms/FormAutocomplete";
import FormCreatableSelect from "@components/Forms/FormCreatableSelect";
import FormDatepicker from "@components/Forms/FormDatepicker";
import FormInputText from "@components/Forms/FormInputText";
import FormSelectBoolean from "@components/Forms/FormSelectBoolean";
import FormTextArea from "@components/Forms/FormTextArea";
import CustomModal from "@components/Modal";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { Action, DynamicObject, State } from "@models/entityFormType";
import {
  Autocomplete,
  CircularProgress,
  FormControl,
  Stack,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Typography
} from "@mui/material";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";
import { fetchMetricEntity } from "@redux/slice/metricsSlice";
import {
  extractKeyValueFromEntity,
  getNestedSuperTypeObj,
  isArray,
  isEmpty,
  isObject,
  isString,
  serverError
} from "@utils/Utils";
import moment from "moment";
import { useEffect, useReducer, useRef, useState } from "react";
import { useForm } from "react-hook-form";
import { useNavigate, useParams, useSearchParams } from "react-router-dom";
import { toast } from "react-toastify";

export const initialState: State = {
  entityTypeObj: null,

  error: null
};

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case "FETCH_REQUEST":
      return { ...state, error: null };
    case "FETCH_SUCCESS":
      return { ...state, entityTypeObj: action.payload };
    case "FETCH_FAILURE":
      return { ...state, error: action.payload };
    default:
      return state;
  }
}

const EntityForm = ({
  open,
  onClose
}: {
  open: boolean;
  onClose: () => void;
}) => {
  const key = "atlas.ui.editable.entity.types";
  const dispatchApi = useAppDispatch();
  const navigate = useNavigate();
  const { guid }: any = useParams();
  const [_searchParams, setSearchParams] = useSearchParams();

  const { entityData = {} }: any = useAppSelector((state: any) => state.entity);
  const { sessionObj = {} }: any = useAppSelector(
    (state: any) => state.session
  );
  const { data = {} } = sessionObj || {};
  const entityTypes = data?.[key] || {};
  const { typeHeaderData }: any = useAppSelector(
    (state: any) => state.typeHeader
  );
  const toastId: any = useRef(null);
  const [state, dispatch] = useReducer(reducer, initialState);
  const [typeValue, setTypeValue] = useState<any>("");
  const [view, setView] = useState<string>("required");
  const [entity, setEntity] = useState<any>({});
  const [loader, setLoader] = useState<boolean>(false);

  useEffect(() => {
    if (!isEmpty(guid)) {
      fetchEntity();
    }
  }, [guid]);

  useEffect(() => {
    if (!isEmpty(typeValue?.name)) {
      fetchEntityType(typeValue.name);
    }
  }, [typeValue.name]);

  const fetchEntity = async () => {
    dispatch({ type: "FETCH_REQUEST" });
    try {
      setLoader(true);
      let entityResp = await getEntity(guid, "GET", {});
      const { entity = {} } = entityResp?.data || {};

      const { typeName }: { typeName: string } = entity;
      let typedefResp = await getTypedef(typeName, { type: "entity" });
      let attributeDefList = !isEmpty(typedefResp.data)
        ? getNestedSuperTypeObj({
            seperateRelatioshipAttr: true,
            attrMerge: true,
            data: typedefResp.data,
            collection: entityData.entityDefs
          })
        : null;
      if (
        attributeDefList &&
        attributeDefList?.relationshipAttributeDefs?.length
      ) {
        attributeDefList.attributeDefs = !isEmpty(
          attributeDefList.attributeDefs
        )
          ? attributeDefList.attributeDefs.filter(
              (obj: { name: string }) =>
                !attributeDefList.relationshipAttributeDefs.find(
                  (attr: { name: string }) => attr.name === obj.name
                )
            )
          : [];
      }
      setEntity(entity);
      dispatch({ type: "FETCH_SUCCESS", payload: attributeDefList });
      setLoader(false);

      //Edit//
      let values = { ...entityResp.data.entity };
      let attributesValues = { ...attributeDefList };
      let entityAttribute: Record<string, string> = {};
      let relationshipAttribute: Record<string, string> = {};

      const extractValue = (value: any, typeName: string) => {
        let parseData: any = typeName.indexOf("array") > -1 ? [] : null;
        if (!value) {
          return value;
        }
        if (isArray(value) && !isEmpty(value)) {
          value.map(
            (val: {
              uniqueAttributes: any;
              guid: string;
              typeName: string;
              qualifiedName: string;
              inputValue: string;
            }) => {
              return parseData.push(
                !isEmpty(val.typeName)
                  ? {
                      guid: val.guid,
                      typeName:
                        val.uniqueAttributes.qualifiedName || val.qualifiedName,
                      label:
                        val.uniqueAttributes.qualifiedName || val.qualifiedName
                    }
                  : val.inputValue || val
              );
            }
          );
        } else if (!isArray(value) && !isEmpty(value)) {
          return (parseData = {
            guid: value.guid,
            typeName:
              value.uniqueAttributes.qualifiedName || value.qualifiedName,
            label: value.uniqueAttributes.qualifiedName || value.qualifiedName
          });
        }
        return parseData;
      };

      for (let attributes in attributesValues) {
        for (let obj of attributesValues[attributes]) {
          let type: string = obj.typeName;
          let value = isEmpty(values.attributes[obj.name])
            ? values[obj.name]
            : values.attributes[obj.name];
          let typeNameCategory = typeHeaderData.find(
            (entitydef: { name: string }) => entitydef.name == type
          );
          let val = null;

          if (
            entityDefs.find(
              (entitydef: { name: string }) => entitydef.name == type
            )
          ) {
            val = extractValue(value, type);
          } else if (type === "date" || type === "time") {
            val = moment(value);
          } else if (
            type.indexOf("map") > -1 ||
            (typeNameCategory && typeNameCategory.category === "STRUCT")
          ) {
            try {
              if (value && value.length) {
                let parseData = JSON.parse(value);
                val = parseData;
              }
            } catch (err) {
              console.error(err);
            }
          } else if (
            type.indexOf("array") > -1 &&
            type.indexOf("string") === -1
          ) {
            val = extractValue(value, type);
          } else {
            if (isString(value)) {
              if (value.length) {
                val = value;
              } else {
                val = null;
              }
            } else {
              val = value;
            }
          }

          if (attributes == "attributeDefs") {
            entityAttribute[obj.name] = val;
          } else {
            relationshipAttribute[obj.name] = val;
          }
        }
      }
      //Edit//
      reset(
        { ...entityAttribute, ...relationshipAttribute },
        {
          keepDefaultValues: false,
          keepDirty: false,
          keepTouched: false,
          keepErrors: false
        }
      );
    } catch (error) {
      dispatch({
        type: "FETCH_FAILURE",
        payload: (error as Error).message
      });
      setLoader(false);
      console.log("Error while creating entity", error);
      toast.dismiss(toastId.current);
      serverError(error, toastId);
    }
  };

  const fetchEntityType = async (name: string) => {
    let params: { type: string } = { type: "entity" };
    dispatch({ type: "FETCH_REQUEST" });
    try {
      setLoader(true);
      const entityTypeResp: DynamicObject = await getEntitiesType(name, params);
      let attributeDefList = !isEmpty(entityTypeResp.data)
        ? getNestedSuperTypeObj({
            seperateRelatioshipAttr: true,
            attrMerge: true,
            data: entityTypeResp.data,
            collection: entityData.entityDefs
          })
        : null;
      if (
        attributeDefList &&
        attributeDefList.relationshipAttributeDefs.length
      ) {
        attributeDefList.attributeDefs = attributeDefList.attributeDefs.filter(
          (obj: { name: string }) =>
            !attributeDefList.relationshipAttributeDefs.find(
              (attr: { name: string }) => attr.name === obj.name
            )
        );
      }

      dispatch({ type: "FETCH_SUCCESS", payload: attributeDefList });
      setLoader(false);
    } catch (error) {
      setLoader(false);
      dispatch({ type: "FETCH_FAILURE", payload: (error as Error).message });
      console.log("Error while fetching entity type data", error);
      toast.dismiss(toastId.current);
      serverError(error, toastId);
    }
  };
  const { entityDefs = {} } = entityData || {};

  const optionsList = !isEmpty(entityDefs)
    ? entityDefs.map((entity: { name: string }) => {
        return entity.name;
      })
    : [];

  const { entityTypeObj }: State = state;

  const handleViewChange = (
    _event: React.MouseEvent<HTMLElement>,
    newView: string
  ) => {
    if (newView !== null) {
      setView(newView);
    }
  };

  const {
    control,
    reset,
    handleSubmit,
    getValues,
    formState: { isSubmitting, isDirty }
  } = useForm({
    mode: "onChange",
    shouldUnregister: false
  });

  const fetchInitialData = async () => {
    await dispatchApi(fetchMetricEntity());
  };

  const onSubmit = async () => {
    let attributesValues = { ...entityTypeObj };
    const formValues = { ...getValues() };
    let entityAttribute: Record<string, string> = {};
    let relationshipAttribute: Record<string, string> = {};
    let referredEntities: Record<string, string> = {};

    const extractValue = (value: any, typeName: string) => {
      let parseData: any = [];
      if (!value) {
        return value;
      }
      if (isArray(value) && !isEmpty(value)) {
        value.map(
          (val: { guid: string; typeName: string; inputValue: string }) =>
            parseData.push(
              !isEmpty(val?.typeName)
                ? { guid: val.guid, typeName: val.typeName }
                : val.inputValue || val
            )
        );
      } else if (isObject(value) && !isEmpty(value)) {
        parseData = { guid: value.guid, typeName: typeName };
      } else {
        return value;
      }
      return parseData;
    };

    for (let attributes in attributesValues) {
      for (let obj of attributesValues[attributes]) {
        let type: string = obj.typeName;
        let value = formValues[obj.name];
        let typeNameCategory = typeHeaderData.find(
          (entitydef: { name: string }) => entitydef.name == type
        );
        let val = null;

        if (
          entityDefs.find(
            (entitydef: { name: string }) => entitydef.name == type
          )
        ) {
          val = extractValue(value, type);
        } else if (type === "date" || type === "time") {
          val = value?.valueOf();
        } else if (
          type.indexOf("map") > -1 ||
          (typeNameCategory && typeNameCategory.category === "STRUCT")
        ) {
          try {
            if (value && value.length) {
              let parseData = JSON.parse(value);
              val = parseData;
            }
          } catch (err) {
            console.error(err);
          }
        } else if (
          type.indexOf("array") > -1 ||
          type.indexOf("string") === -1
        ) {
          val = extractValue(value, type);
        } else {
          if (isString(value)) {
            if (value.length) {
              val = value;
            } else {
              val = null;
            }
          } else {
            val = value;
          }
        }

        if (attributes == "attributeDefs") {
          entityAttribute[obj.name] = val;
        } else {
          relationshipAttribute[obj.name] = val;
        }
      }
    }

    let formData = {
      entity: {
        typeName: isEmpty(guid) ? typeValue.name : entity.typeName,
        attributes: entityAttribute,
        guid: isEmpty(guid) ? -1 : guid,
        relationshipAttributes: relationshipAttribute
      },
      referredEntities: referredEntities
    };
    try {
      let entityResp = await createEntity(formData);
      onClose();
      let entityRespData = entityResp.data;
      const { guidAssignments } = entityRespData;
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        `Entity was ${isEmpty(guid) ? "created" : "udpated"} successfully`
      );
      const newSearchParams = new URLSearchParams();
      newSearchParams.set("tabActive", "properties");
      setSearchParams(newSearchParams);
      navigate(
        {
          pathname: `/detailPage/${
            isEmpty(guid)
              ? guidAssignments[formData.entity.guid]
              : formData.entity.guid
          }`,
          search: newSearchParams.toString()
        },
        { replace: true }
      );
      if (!isEmpty(guid)) {
        dispatchApi(fetchDetailPageData(guid as string));
      }
      fetchInitialData();
    } catch (error) {
      console.error("Error while creating entity", error);
      toast.dismiss(toastId.current);
      serverError(error, toastId);
    }
  };

  const renderFormControl: any = (obj: any) => {
    let attributeObj = entityDefs.find(
      (typedef: any) => typedef.name == obj.typeName
    );
    let typeHeaderObj = typeHeaderData.find(
      (typeheader: { name: string }) => typeheader.name == obj.typeName
    );
    switch (obj.typeName) {
      case (obj.typeName.indexOf("map") > -1 ||
        typeHeaderObj?.category === "STRUCT") &&
        obj.typeName:
        return <FormTextArea data={obj} control={control} />;
      case (attributeObj && obj.typeName) ||
        (obj.typeName.indexOf("array") > -1 &&
          obj.cardinality == "SET" &&
          obj.typeName):
        return <FormAutocomplete data={obj} control={control} />;
      case (attributeObj && obj.typeName) ||
        (obj.typeName.indexOf("array") > -1 &&
          obj.cardinality == "SINGLE" &&
          obj.typeName):
        return <FormCreatableSelect data={obj} control={control} />;
      case "boolean":
        return <FormSelectBoolean data={obj} control={control} />;
      case "date":
      case "time":
        return <FormDatepicker data={obj} control={control} />;

      default:
        return <FormInputText data={obj} control={control} />;
    }
  };
  const { name } = extractKeyValueFromEntity(entity);

  let requiredFieldList = !isEmpty(entityTypeObj)
    ? Object.keys(entityTypeObj).reduce((acc: any, key: string) => {
        acc[key] = entityTypeObj[key].filter(
          (value: { isOptional: string }) => !value.isOptional
        );
        return acc;
      }, {})
    : [];

  return (
    <CustomModal
      open={open}
      onClose={onClose}
      title={`${isEmpty(guid) ? "Create" : "Edit"} entity`}
      button1Label="Cancel"
      button1Handler={onClose}
      button2Label={guid ? "Update" : "Create"}
      button2Handler={handleSubmit(onSubmit)}
      disableButton2={isSubmitting}
      isDirty={isDirty}
      maxWidth="md"
    >
      <Stack gap="1.5rem" minHeight="96px">
        <Stack direction="row" justifyContent="space-between" gap="1rem">
          {!isEmpty(guid) ? (
            <Typography overflow={"hidden"} textOverflow={"ellipsis"}>
              {name}
            </Typography>
          ) : (
            <FormControl size="small" className="advanced-search-formcontrol">
              <Autocomplete
                size="small"
                fullWidth
                value={typeValue.name}
                onChange={(_event: any, newValue: string | null) => {
                  let selectedEntity = entityDefs.find(
                    (obj: { name: string }) => obj.name == newValue
                  );

                  reset();
                  setTypeValue(selectedEntity);
                }}
                disableClearable={true}
                id="search-by-type"
                options={optionsList
                  .map((obj: any) => {
                    if (entityTypes != "*") {
                      if (obj == entityTypes) {
                        return obj;
                      }
                    } else {
                      return obj;
                    }
                  })
                  .filter(Boolean)
                  .sort()}
                className="entity-form-type-select"
                renderInput={(params) => {
                  return (
                    <TextField
                      {...params}
                      fullWidth
                      onChange={(event: any) => {
                        setTypeValue(event.target.value);
                      }}
                      label="Search-entity-type"
                      InputLabelProps={{
                        style: {
                          top: "unset",
                          bottom: "14px"
                        }
                      }}
                      InputProps={{
                        style: {
                          padding: "0px 32px 0px 4px",
                          height: "32px",
                          lineHeight: "1.2"
                        },
                        ...params.InputProps
                      }}
                    />
                  );
                }}
              />
            </FormControl>
          )}

          {!isEmpty(entityTypeObj) && (
            <ToggleButtonGroup
              color="primary"
              value={view}
              exclusive
              className="entity-form-toggle"
              size="small"
              onChange={handleViewChange}
              aria-label="View options"
            >
              <ToggleButton
                value="required"
                className="entity-form-toggle-btn"
                aria-label="Required Fields"
              >
                Required
              </ToggleButton>
              <ToggleButton
                className="entity-form-toggle-btn"
                value="all"
                aria-label="All Fields"
              >
                All
              </ToggleButton>
            </ToggleButtonGroup>
          )}
        </Stack>
        {loader ? (
          <Stack
            direction="row"
            alignItems={"center"}
            justifyContent={"center"}
          >
            <CircularProgress />
          </Stack>
        ) : (
          !isEmpty(entityTypeObj) && (
            <Stack>
              <form onSubmit={handleSubmit(onSubmit)} onReset={reset}>
                {Object.entries(
                  view == "required" ? requiredFieldList : entityTypeObj
                ).map(([key, field]: any) => {
                  if (!isEmpty(field)) {
                    return (
                      <>
                        <fieldset className="entity-form-fieldset">
                          <legend>
                            <Typography fontSize={"14px"}>
                              {key == "attributeDefs"
                                ? "Attributes"
                                : "Relationships"}
                            </Typography>
                          </legend>

                          <Stack className="entity-form-fields">
                            {field.map((obj: any) => {
                              return (
                                <Stack direction="row">
                                  {renderFormControl(obj)}
                                </Stack>
                              );
                            })}
                          </Stack>
                        </fieldset>
                      </>
                    );
                  }
                })}
              </form>
            </Stack>
          )
        )}
      </Stack>
    </CustomModal>
  );
};

export default EntityForm;
