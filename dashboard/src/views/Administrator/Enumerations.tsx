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

import { createEnum, updateEnum } from "@api/apiMethods/typeDefApiMethods";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { Stack } from "@mui/material";
import { fetchEnumData } from "@redux/slice/enumSlice";
import { isEmpty, serverError } from "@utils/Utils";
import EnumCreateUpdate from "@views/BusinessMetadata/EnumCreateUpdate";
import { useRef } from "react";
import { useForm } from "react-hook-form";
import { toast } from "react-toastify";

const Enumerations = () => {
  const dispatch = useAppDispatch();
  const { enumObj }: any = useAppSelector((state: any) => state.enum);
  const { enumDefs } = enumObj?.data || {};
  const {
    control,
    handleSubmit,
    watch,
    setValue,
    reset,
    formState: { isDirty, isSubmitting }
  } = useForm();
  const toastId: any = useRef(null);

  const onSubmit = async (values: any) => {
    let formData = { ...values };
    let isPutCall = false;
    let isPostCallEnum = false;
    const { enumType = "", enumValues = [] } = formData || {};
    const selectedEnumValues = !isEmpty(enumValues)
      ? enumValues.map((enumVal: { value: any }) => enumVal.value)
      : [];
    let newEnumDef = [];

    const enumName = !isEmpty(enumDefs)
      ? enumDefs?.find((enumDef: { name: string }) => enumDef.name === enumType)
      : {};
    const { elementDefs = [] } = enumName || {};
    if (!isEmpty(enumName)) {
      let enumDef = elementDefs || [];
      if (enumDef.length === selectedEnumValues.length) {
        enumDef.forEach((enumVal: { value: any }) => {
          if (!selectedEnumValues.includes(enumVal.value)) {
            isPutCall = true;
          }
        });
      } else {
        isPutCall = true;
      }
    } else {
      isPostCallEnum = true;
    }
    let elementValues: { ordinal: number; value: any }[] = [];
    selectedEnumValues?.forEach((inputEnumVal: any, index: number) => {
      elementValues?.push({
        ordinal: index + 1,
        value: inputEnumVal
      });
    });

    newEnumDef?.push({
      name: enumType,
      elementDefs: elementValues
    });
    let data = {
      enumDefs: newEnumDef
    };

    try {
      if (isPostCallEnum) {
        await createEnum(data);
        toast.dismiss(toastId.current);
        toastId.current = toast.success(
          `Enumeration ${enumType} 
           added
             successfully`
        );
      } else if (isPutCall) {
        await updateEnum(data);
        toast.dismiss(toastId.current);
        toastId.current = toast.success(
          `Enumeration ${enumType} updated
             successfully`
        );
      } else {
        toast.dismiss(toastId.current);
        toastId.current = toast.success("No updated values");
      }
      reset({ enumType: "", enumValues: [] });
      dispatch(fetchEnumData());
    } catch (error) {
      console.log(`Error occur while creating or updating Enum`, error);
      serverError(error, toastId);
    }
  };
  return (
    <Stack gap={2} paddingTop="2rem" paddingBottom="2rem">
      <EnumCreateUpdate
        control={control}
        handleSubmit={handleSubmit}
        setValue={setValue}
        reset={reset}
        watch={watch}
        isSubmitting={isSubmitting}
        onSubmit={onSubmit}
        isDirty={isDirty}
      />
    </Stack>
  );
};

export default Enumerations;
