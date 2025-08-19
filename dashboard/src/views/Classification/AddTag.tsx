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
 * limitations under the License.useForm
 */

import CustomModal from "@components/Modal";
import {
  Autocomplete,
  Checkbox,
  FormControlLabel,
  InputLabel,
  Stack,
  TextField,
  Typography
} from "@mui/material";
import { Controller, useForm } from "react-hook-form";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import {
  customSortBy,
  extractKeyValueFromEntity,
  getNestedSuperTypeObj,
  isArray,
  isEmpty,
  serverError
} from "@utils/Utils";
import AddValidityPeriod from "./AddValidityPeriod";
import FormCreatableSelect from "@components/Forms/FormCreatableSelect";
import FormSelectBoolean from "@components/Forms/FormSelectBoolean";
import FormDatepicker from "@components/Forms/FormDatepicker";
import FormInputText from "@components/Forms/FormInputText";
import FormSingleSelect from "@components/Forms/FormSingleSelect";
import { addTag, editAssignTag } from "@api/apiMethods/classificationApiMethod";
import { useRef } from "react";
import { toast } from "react-toastify";
import moment from "moment-timezone";
import { useLocation, useParams } from "react-router-dom";
import { fetchGlossaryDetails } from "@redux/slice/glossaryDetailsSlice";
import Card from "@mui/material/Card";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";
import { fetchGlossaryData } from "@redux/slice/glossarySlice";

const AddTag = (props: {
  open: boolean;
  isAdd: boolean;
  onClose: () => void;
  entityData: any;
  setUpdateTable: any;
  setRowSelection: any;
}) => {
  const { open, onClose, entityData, setUpdateTable, isAdd, setRowSelection } =
    props;
  const { guid }: any = useParams();
  const location = useLocation();
  const dispatchApi = useAppDispatch();
  const { guid: entityGuid = "", classifications = [] }: { guid: string } =
    entityData || [];
  const toastId: any = useRef(null);
  const searchParams = new URLSearchParams(location.search);
  const gType = searchParams.get("gtype");

  const { classificationData }: any = useAppSelector(
    (state: any) => state.classification
  );
  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const { classificationDefs = {} } = classificationData || [];

  const editFormValues = () => {
    const editFormData = { ...entityData };
    const {
      typeName,
      propagate,
      validityPeriods,
      attributes,
      removePropagationsOnEntityDelete
    } = editFormData;
    let defaultValue: Record<string, any> = {};
    defaultValue["checkModalTagProperty"] = propagate;
    defaultValue["removePropagationsOnEntityDelete"] =
      removePropagationsOnEntityDelete;

    if (!isEmpty(validityPeriods)) {
      defaultValue["checkTimezoneProperty"] = validityPeriods.length > 0;
    }

    if (!isEmpty(validityPeriods)) {
      let timeZone = validityPeriods.map(
        (obj: { startTime: string; endTime: string; timeZone: string }) => {
          return {
            ...obj,
            timeZone: { label: obj.timeZone, value: obj.timeZone }
          };
        }
      );
      defaultValue["validityPeriod"] = timeZone;
    }
    let data = { ...defaultValue, ...{ ["attributes"]: attributes } };
    return data;
  };
  const {
    control,
    watch,
    handleSubmit,
    formState: { isSubmitting, isDirty }
  } = useForm({
    defaultValues: isAdd ? undefined : editFormValues(),
    mode: "onChange",
    shouldUnregister: true
  });
  const { enumDefs = {} } = enumObj?.data || {};

  const tagName = isAdd ? watch("classification") : entityData.typeName;
  const checkModalTagProperty = watch("checkModalTagProperty");
  const checkTimezoneProperty = watch("checkTimezoneProperty");

  const classificationNames = !isEmpty(classifications)
    ? classifications.map((obj: { typeName: any }) => {
        return obj.typeName;
      })
    : [];

  const nonAssignTag = classificationDefs.filter((obj: { name: string }) => {
    return !classificationNames.includes(obj.name);
  });

  const options = !isEmpty(nonAssignTag)
    ? nonAssignTag.map((obj: { name: any }) => ({
        label: obj.name,
        value: obj.name
      }))
    : [];

  const classificationObj = !isEmpty(tagName)
    ? classificationDefs.find((obj: { name: string }) => {
        return obj.name == (isAdd ? tagName.label : tagName);
      })
    : null;

  let attributeDefList = !isEmpty(classificationObj)
    ? getNestedSuperTypeObj({
        data: classificationObj,
        collection: classificationDefs,
        attrMerge: true
      })
    : null;

  const renderFormControl: any = (obj: any) => {
    const { name: typeName } = extractKeyValueFromEntity(obj, "typeName");

    const typeNameValue = !isEmpty(enumDefs)
      ? enumDefs.find((obj: { name: string }) => obj.name == typeName)
      : [];
    const { elementDefs } = typeNameValue || {};
    switch (obj.typeName) {
      case !isEmpty(typeNameValue) && obj.typeName:
        return (
          <FormSingleSelect
            data={obj}
            control={control}
            optionsList={elementDefs}
            typeName={typeName}
            fieldName="attributes"
          />
        );
      case obj.typeName.indexOf("array") == 0 && obj.typeName:
        return (
          <FormCreatableSelect
            data={obj}
            control={control}
            fieldName="attributes"
          />
        );
      case "boolean":
        return (
          <FormSelectBoolean
            data={obj}
            control={control}
            fieldName="attributes"
          />
        );
      case "date":
      case "time":
        return (
          <FormDatepicker data={obj} control={control} fieldName="attributes" />
        );

      default:
        return (
          <FormInputText
            data={obj}
            control={control}
            fieldName="attributes"
            isLabelCapitalized={false}
          />
        );
    }
  };

  const onSubmit = async (formData: any) => {
    if (isAdd && isEmpty(classificationObj)) {
      toast.dismiss(toastId.current);
      toast.warning("Please select a classification first");
      return;
    }

    let formValues = { ...formData };
    const {
      checkModalTagProperty,
      classification,
      removePropagationsOnEntityDelete,
      validityPeriod,
      attributes
    } = formValues;
    let data: any = {};
    const attributeObj: any = {};

    for (const key in attributes) {
      if (isArray(attributes[key])) {
        attributeObj[key] = attributes[key].map((obj: { inputValue: any }) => {
          if (!isEmpty(obj.inputValue)) {
            return obj.inputValue;
          }
          return obj;
        });
      } else {
        attributeObj[key] = attributes[key];
      }
    }
    let classificationDataObj: any = {
      attributes: !isEmpty(attributes) ? attributeObj : {},
      propagate: checkModalTagProperty,
      removePropagationsOnEntityDelete: removePropagationsOnEntityDelete,
      typeName: isAdd ? tagName.label : tagName
    };
    if (!isEmpty(validityPeriod)) {
      let timeZones = validityPeriod.map(
        (obj: {
          validityPeriod: any;
          startTime: moment.MomentInput;
          endTime: moment.MomentInput;
          timeZone: { label: any };
        }) => {
          delete obj.validityPeriod;
          return {
            ...obj,
            ...{
              startTime: moment(obj.startTime)
                .utc()
                .format("YYYY/MM/DD HH:mm:ss"),
              endTime: moment(obj.endTime).utc().format("YYYY/MM/DD HH:mm:ss"),
              timeZone: obj.timeZone.label
            }
          };
        }
      );
      classificationDataObj["validityPeriods"] = timeZones;
    } else {
      classificationDataObj["validityPeriods"] = [];
    }
    if (isAdd) {
      data["classification"] = classificationDataObj;
    } else {
      data = [classificationDataObj];
    }
    if (isAdd) {
      let bulkGuids = isArray(entityData)
        ? entityData.map((obj: { guid: any }) => {
            return obj.guid;
          })
        : [entityGuid];

      data["entityGuids"] = bulkGuids;
    }
    if (isAdd) {
      try {
        await addTag(data);

        toast.success(
          `Classification ${classificationObj.name}  has been added to entity`
        );
        onClose();
        if (!isEmpty(setUpdateTable)) {
          setUpdateTable(moment.now());
        }

        if (!isEmpty(guid)) {
          if (!isEmpty(gType)) {
            let params: any = { gtype: gType, guid: guid };
            dispatchApi(fetchGlossaryData());
            dispatchApi(fetchGlossaryDetails(params));
          }
          dispatchApi(fetchDetailPageData(guid as string));
        }

        if (!isEmpty(setRowSelection)) {
          setRowSelection({});
        }
      } catch (error) {
        console.error(`Error while Adding Classification`, error);
        serverError(error, toastId);
      }
    } else {
      try {
        await editAssignTag(guid, data);
        toast.success(
          `Classification ${classificationObj.name}  has been updated successfully to entity`
        );

        onClose();
        if (!isEmpty(setUpdateTable)) {
          setUpdateTable(moment.now());
        }

        if (!isEmpty(guid)) {
          if (!isEmpty(gType)) {
            let params: any = { gtype: gType, guid: guid };
            dispatchApi(fetchGlossaryData());
            dispatchApi(fetchGlossaryDetails(params));
          }
          dispatchApi(fetchDetailPageData(guid as string));
        }

        if (!isEmpty(setRowSelection)) {
          setRowSelection({});
        }
      } catch (error) {
        console.error(`Error while Editing Classification`, error);
        serverError(error, toastId);
      }
    }
  };

  return (
    <CustomModal
      open={open}
      onClose={onClose}
      title={`${isAdd ? "Add" : "Edit"} Classification`}
      button1Label="Cancel"
      button1Handler={onClose}
      button2Label={`${isAdd ? "Add" : "Update"}`}
      maxWidth="md"
      button2Handler={handleSubmit(onSubmit)}
      disableButton2={isEmpty(classificationData) ? true : isSubmitting}
      isDirty={isEmpty(classificationData) ? true : isDirty}
    >
      <form onSubmit={handleSubmit(onSubmit)}>
        <Stack>
          <Controller
            control={control}
            name="classification"
            data-cy="addTagOptions"
            defaultValue={
              isAdd
                ? null
                : {
                    label: entityData.typeName,
                    value: entityData.typeName
                  }
            }
            render={({ field: { onChange, value } }) => {
              return (
                <>
                  <Autocomplete
                    size="small"
                    onChange={(_event, item) => {
                      onChange(item);
                    }}
                    value={value}
                    disabled={isAdd ? false : true}
                    filterSelectedOptions
                    getOptionLabel={(option: { label: string }) => option.label}
                    isOptionEqualToValue={(option, value) =>
                      option.label === value.label
                    }
                    options={customSortBy(options, ["label"])}
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

          <Stack
            direction="row"
            marginBottom="1.5rem"
            justifyContent="space-between"
            alignItems="flex-start"
            gap="1rem"
          >
            <Card
              variant="outlined"
              size="small"
              sx={{ width: "50%", padding: "0.375rem 0.875rem" }}
            >
              <Stack>
                {" "}
                <Controller
                  control={control}
                  name={"checkModalTagProperty"}
                  data-cy="checkModalTagProperty"
                  defaultValue={isAdd ? true : entityData.propagate}
                  render={({ field: { onChange, value } }) => (
                    <FormControlLabel
                      control={
                        <Checkbox
                          size="small"
                          checked={value}
                          onChange={onChange}
                        />
                      }
                      label="Propagate"
                    />
                  )}
                />
                {(checkModalTagProperty == undefined ||
                  checkModalTagProperty) && (
                  <Controller
                    control={control}
                    name={"removePropagationsOnEntityDelete"}
                    data-cy="removePropagationsOnEntityDelete"
                    render={({ field: { onChange, value } }) => (
                      <FormControlLabel
                        control={
                          <Checkbox
                            size="small"
                            checked={value}
                            onChange={onChange}
                          />
                        }
                        label=" Remove propagation on entity delete"
                      />
                    )}
                  />
                )}{" "}
              </Stack>
            </Card>
            <Card
              variant="none"
              size="small"
              sx={{ width: "50%", padding: "0.375rem 0.875rem" }}
            >
              <Controller
                control={control}
                name={"checkTimezoneProperty"}
                data-cy="checkTimezoneProperty"
                render={({ field: { onChange, value } }) => (
                  <FormControlLabel
                    control={
                      <Checkbox
                        size="small"
                        checked={value}
                        onChange={onChange}
                      />
                    }
                    label="Apply Validity Period"
                  />
                )}
              />
            </Card>
          </Stack>
          {(checkTimezoneProperty ||
            entityData?.validityPeriods?.length > 0) && (
            <AddValidityPeriod control={control} />
          )}
          {!isEmpty(attributeDefList) && (
            <div>
              <Typography fontWeight={600} sx={{ paddingBottom: "1rem" }}>
                Classification Attributes(optional)
              </Typography>
              <InputLabel sx={{ marginBottom: "1rem" }}>
                Add attribute values for this classification
              </InputLabel>
            </div>
          )}
          {!isEmpty(attributeDefList) &&
            attributeDefList.map((field: any) => {
              if (!isEmpty(field)) {
                return (
                  <>
                    <Stack>
                      <Stack direction="row">{renderFormControl(field)}</Stack>
                    </Stack>
                  </>
                );
              }
            })}
        </Stack>
      </form>
    </CustomModal>
  );
};

export default AddTag;
