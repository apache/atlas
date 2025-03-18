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

import { updateEnum } from "@api/apiMethods/typeDefApiMethods";
import { useAppDispatch } from "@hooks/reducerHook";
import { Stack } from "@mui/material";
import { fetchEnumData } from "@redux/slice/enumSlice";
import { serverError } from "@utils/Utils";
import EnumCreateUpdate from "@views/BusinessMetadata/EnumCreateUpdate";
import { useRef } from "react";
import { useForm } from "react-hook-form";
import { toast } from "react-toastify";

const Enumerations = () => {
  const dispatch = useAppDispatch();
  const {
    control,
    handleSubmit,
    watch,
    setValue,
    reset,
    formState: { isSubmitting },
  } = useForm();
  const toastId: any = useRef(null);

  const onSubmit = async (values: any) => {
    let formData = { ...values };
    const { enumType, enumValues } = formData;
    let data = {
      enumDefs: [{ name: enumType, elementDefs: enumValues }],
    };

    try {
      await updateEnum(data);
      reset({ enumType: "", enumValues: [] });
      dispatch(fetchEnumData());
      toast.success(`Enumeration ${enumType} updated successfully`);
    } catch (error) {
      console.log(`Error occur while creating Enum`, error);
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
      />
    </Stack>
  );
};

export default Enumerations;
