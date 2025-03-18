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

import CustomModal from "@components/Modal";
import GlossaryForm from "./GlossaryForm";
import { useForm } from "react-hook-form";
import {
  createGlossary,
  editGlossary
} from "@api/apiMethods/glossaryApiMethod";
import { isEmpty, serverError } from "@utils/Utils";
import { toast } from "react-toastify";
import { useRef } from "react";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { fetchGlossaryData } from "@redux/slice/glossarySlice";

const AddUpdateGlossaryForm = (props: {
  open: any;
  onClose: any;
  isAdd: any;
  node: Record<string, any> | undefined;
}) => {
  const { open, onClose, isAdd, node } = props;
  const dispatch = useAppDispatch();
  const toastId: any = useRef(null);
  const { glossaryData }: any = useAppSelector((state: any) => state.glossary);
  const { id } = node || {};
  let defaultValue: Record<string, string> = {};
  let glossaryObj: Record<string, string> = {};
  if (!isAdd) {
    glossaryObj = glossaryData.find((obj: { name: string }) => {
      return obj.name == id;
    });
    const { name, shortDescription, longDescription } = glossaryObj || {};

    defaultValue["name"] = name;
    defaultValue["shortDescription"] = shortDescription;
    defaultValue["longDescription"] = longDescription;
  }
  const {
    control,
    handleSubmit,
    setValue,
    formState: { isSubmitting }
  } = useForm({
    defaultValues: isAdd ? {} : defaultValue,
    mode: "onChange",
    shouldUnregister: true
  });

  const onSubmit = async (formValues: any) => {
    let formData = { ...formValues };
    const { guid, qualifiedName } = glossaryObj;
    const {
      name,
      shortDescription,
      longDescription
    }: { name: string; shortDescription: string; longDescription: string } =
      formData;
    let data: Record<string, string> = {};
    if (!isAdd) {
      data["guid"] = guid;
      data["qualifiedName"] = qualifiedName;
    }
    data["name"] = name;
    data["shortDescription"] = !isEmpty(shortDescription)
      ? shortDescription
      : "";
    data["longDescription"] = !isEmpty(longDescription) ? longDescription : "";

    try {
      if (isAdd) {
        await createGlossary(data);
      } else {
        await editGlossary(guid, data);
      }

      await dispatch(fetchGlossaryData());
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        `Glossary ${name} was ${isAdd ? "created" : "updated"} successfully`
      );
      onClose();
    } catch (error) {
      console.log(
        `Error occur while ${isAdd ? "created" : "updated"} Glossary`,
        error
      );
      serverError(error, toastId);
    }
  };

  return (
    <>
      <CustomModal
        open={open}
        onClose={onClose}
        title={isAdd ? "Create Glossary" : "Edit Glossary"}
        button1Label="Cancel"
        button1Handler={onClose}
        button2Label={isAdd ? "Create" : "Update"}
        maxWidth="sm"
        button2Handler={handleSubmit(onSubmit)}
        disableButton2={isSubmitting}
      >
        <GlossaryForm
          control={control}
          handleSubmit={handleSubmit(onSubmit)}
          setValue={setValue}
        />
      </CustomModal>
    </>
  );
};

export default AddUpdateGlossaryForm;
