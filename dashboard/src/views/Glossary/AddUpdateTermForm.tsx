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
  createTermorCategory,
  editTermorCatgeory
} from "@api/apiMethods/glossaryApiMethod";
import { isEmpty, serverError } from "@utils/Utils";
import { toast } from "react-toastify";
import { useRef } from "react";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { fetchGlossaryDetails } from "@redux/slice/glossaryDetailsSlice";
import { useLocation, useParams } from "react-router-dom";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";
import { fetchGlossaryData } from "@redux/slice/glossarySlice";

const AddUpdateTermForm = (props: {
  open: any;
  onClose: any;
  isAdd: any;
  node: Record<string, any> | undefined;
  dataObj: any;
}) => {
  const { open, onClose, isAdd, node, dataObj } = props;
  const { id, parent } = node || {};
  const dispatchApi = useAppDispatch();
  const location = useLocation();
  const toastId: any = useRef(null);
  const searchParams = new URLSearchParams(location.search);
  const gType = searchParams.get("gtype");
  const { guid: entityGuid } = useParams();
  const { glossaryData }: any = useAppSelector((state: any) => state.glossary);
  let defaultValue: Record<string, string> = {};
  let glossaryObj: Record<string, string> = {};

  if (isAdd) {
    glossaryObj = glossaryData.find((obj: { name: string }) => {
      return obj.name == parent;
    });
  }

  const { name = "", shortDescription, longDescription } = glossaryObj || {};

  defaultValue["name"] = !isEmpty(dataObj) ? dataObj.name : name;
  defaultValue["shortDescription"] = !isEmpty(dataObj)
    ? dataObj.shortDescription
    : shortDescription;
  defaultValue["longDescription"] = !isEmpty(dataObj)
    ? dataObj.longDescription
    : longDescription;

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
    const { guid } = isAdd ? glossaryObj : dataObj;
    const {
      name,
      shortDescription,
      longDescription
    }: { name: string; shortDescription: string; longDescription: string } =
      formData;
    let data: Record<string, any> = {};
    if (!isAdd) {
      data = { ...glossaryObj };
    }
    let glossaryDetails: Record<string, any> = {
      ["displayText"]: id,
      ["glossaryGuid"]: guid
    };

    if (isAdd) {
      data["anchor"] = glossaryDetails;
    } else {
      data = { ...dataObj };
    }
    data["name"] = name;
    data["shortDescription"] = !isEmpty(shortDescription)
      ? shortDescription
      : "";
    data["longDescription"] = !isEmpty(longDescription) ? longDescription : "";

    try {
      if (isAdd) {
        await createTermorCategory("term", data);
      } else {
        await editTermorCatgeory("term", guid, data);
      }
      if (isAdd) {
        dispatchApi(fetchGlossaryData());
      } else {
        if (!isEmpty(dataObj)) {
          let params: any = { gtype: gType, guid: entityGuid };
          dispatchApi(fetchGlossaryData());
          dispatchApi(fetchGlossaryDetails(params));
          dispatchApi(fetchDetailPageData(dataObj.guid as string));
        }
      }
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        `Term ${name} was ${isAdd ? "created" : "updated"} successfully`
      );
      onClose();
    } catch (error) {
      console.log(
        `Error occur while ${isAdd ? "creating" : "updating"} Glossary`,
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
        title={isAdd ? "Create Term" : "Edit Term"}
        button1Label="Cancel"
        button1Handler={onClose}
        button2Label={isAdd ? "Create" : "Update"}
        disableButton2={isSubmitting}
        maxWidth="sm"
        button2Handler={handleSubmit(onSubmit)}
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

export default AddUpdateTermForm;
