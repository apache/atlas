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

import React, { useRef, useState } from "react";
import { useForm } from "react-hook-form";
import { useLocation, useParams } from "react-router-dom";
import { toast } from "react-toastify";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import CustomModal from "@components/Modal";
import FormTreeView from "@components/Forms/FormTreeView";
import RelatedTermStepper from "./AssignRelatedTerm";
import {
  customSortByObjectKeys,
  isEmpty,
  noTreeData,
  serverError,
  showToastError,
  showToastSuccess
} from "@utils/Utils";
import moment from "moment-timezone";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";
import { fetchGlossaryDetails } from "@redux/slice/glossaryDetailsSlice";
import { fetchGlossaryData } from "@redux/slice/glossarySlice";
import { cloneDeep } from "@utils/Helper";
import {
  assignTermstoCategory,
  assignTermstoEntites
} from "@api/apiMethods/glossaryApiMethod";
import { getGlossaryChildrenData } from "@utils/CommonViewFunction";

const steps = ["Select Item", "Attributes"];

const AssignGlossaryItem = ({
  open,
  onClose,
  data,
  updateTable,
  relatedItem,
  columnVal,
  setRowSelection,
  itemType,
  dataKey,
  assignApiMethod,
  treeLabel
}: {
  open: boolean;
  onClose: () => void;
  data: any;
  updateTable: any;
  relatedItem?: boolean;
  columnVal?: string | undefined;
  setRowSelection?: any;
  itemType: "term" | "category";
  dataKey: string;
  assignApiMethod: Function;
  treeLabel: string;
}) => {
  const dispatchApi = useAppDispatch();
  const location = useLocation();
  const toastId: any = useRef(null);
  const searchParams = new URLSearchParams(location.search);
  const gType = searchParams.get("gtype");
  const { guid: entityGuid } = useParams();
  const { glossaryData, loader }: any = useAppSelector(
    (state: any) => state.glossary
  );

  const { guid, meanings } = data;

  const assignedNames = !isEmpty(
    meanings || data[dataKey] || data?.[columnVal!]
  )
    ? (meanings || data[dataKey] || data?.[columnVal!])?.map(
        (obj: { displayText: string }) => {
          return obj.displayText;
        }
      )
    : [];

  const [searchTerm, setSearchTerm] = useState("");
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [activeStep, setActiveStep] = useState(0);
  const [completed, setCompleted] = useState<{ [k: number]: boolean }>({});
  const {
    control,
    handleSubmit,
    formState: { isSubmitting }
  } = useForm();

  const totalSteps = () => steps?.length;
  const completedSteps = () => Object.keys(completed)?.length;
  const isLastStep = () => activeStep === totalSteps() - 1;
  const allStepsCompleted = () => completedSteps() === totalSteps();
  const handleNext = () => {
    if (isEmpty(selectedNode)) {
      toast.error(`Please select ${treeLabel} for association`);
      return;
    }
    const newActiveStep =
      isLastStep() && !allStepsCompleted()
        ? steps.findIndex((_step, i) => !(i in completed))
        : activeStep + 1;
    setActiveStep(newActiveStep);
  };
  const handleBack = () => setActiveStep((prev) => prev - 1);
  const handleStep = (step: number) => () => setActiveStep(step);
  const handleReset = () => {
    setActiveStep(0);
    setCompleted({});
  };

  const updatedGlossary = !isEmpty(glossaryData)
    ? glossaryData.map((gloss: any) => {
        if (isEmpty(gloss[dataKey])) return gloss;
        return {
          ...gloss,
          [dataKey]: gloss[dataKey].filter(
            (item: { displayText: string }) =>
              !assignedNames.includes(item.displayText)
          )
        };
      })
    : [];

  const newServiceTypeArr = !isEmpty(updatedGlossary)
    ? updatedGlossary.map((glossary: any) => {
        const { name = "", guid = "", [dataKey]: items = [] } = glossary;
        return {
          [name]: {
            name,
            children: (items || []).map((item: any) => ({
              name: item.displayText,
              id: item.displayText,
              children: [],
              types: "child",
              parent: name,
              cGuid: item[`${itemType}Guid`],
              guid
            })),
            id: guid,
            types: "parent",
            parent: name,
            guid
          }
        };
      })
    : [];

  const treeData = !isEmpty(updatedGlossary)
    ? getGlossaryChildrenData(customSortByObjectKeys(newServiceTypeArr))
    : noTreeData();

  const handleNodeSelect = (nodeId: any) => {
    setSelectedNode(nodeId);
  };

  const assignItem = async () => {
    if (isEmpty(selectedNode)) {
      showToastError(`No ${treeLabel} Selected`, toastId);
      return;
    }
    const [itemName, glossaryName] = selectedNode!.split("@");
    const glossaryObj = !isEmpty(updatedGlossary)
      ? updatedGlossary.find(
          (obj: { name: string }) => obj.name === glossaryName
        )
      : {};
    const items = glossaryObj ? glossaryObj[dataKey] : [];
    const itemObj = !isEmpty(items)
      ? items.find(
          (item: { displayText: string }) => item.displayText === itemName
        )
      : {};
    const itemGuid = itemObj ? itemObj[`${itemType}Guid`] : undefined;

    try {
      let payload: any = cloneDeep(data);
      if (itemType === "term") {
        if (data.terms) {
          payload.terms = [...data.terms, { termGuid: itemGuid }];
        } else {
          payload.terms = [{ termGuid: itemGuid }];
        }

        if (gType == "category" && !isEmpty(entityGuid)) {
          await assignTermstoCategory(entityGuid as string, payload);
        } else {
          payload = [{ ["guid"]: guid || entityGuid }];
          if (isEmpty(guid) && isEmpty(entityGuid)) {
            payload = data.map((obj: { guid: any }) => {
              return { guid: obj.guid };
            });
          }
          await assignTermstoEntites(itemGuid, payload);
        }
      } else {
        payload = {
          ...data,
          [dataKey]: [
            ...(data[dataKey] || []),
            { [`${itemType}Guid`]: itemGuid }
          ]
        };
        await assignApiMethod(entityGuid as string, payload);
      }

      showToastSuccess(`${treeLabel} is associated successfully`, toastId);
      onClose();
      if (!isEmpty(updateTable)) updateTable(moment.now());
      if (!isEmpty(entityGuid)) {
        dispatchApi(fetchDetailPageData(entityGuid as string));
        if (!isEmpty(gType)) {
          let params: any = { gtype: gType, guid: entityGuid };
          dispatchApi(fetchGlossaryData());
          dispatchApi(fetchGlossaryDetails(params));
        }
      }
      if (!isEmpty(setRowSelection)) setRowSelection({});
    } catch (error) {
      serverError(error, toastId);
    }
  };

  const onSubmit = async (values: any) => {
    let formData = { ...values };
    if (isEmpty(selectedNode)) {
      showToastError(`No ${treeLabel} Selected`, toastId);
      return;
    }
    if (activeStep === 0) {
      showToastError("Please click on next step", toastId);
      return;
    }
    const [itemName, glossaryName] = selectedNode!.split("@");
    const glossaryObj = !isEmpty(updatedGlossary)
      ? updatedGlossary.find(
          (obj: { name: string }) => obj.name === glossaryName
        )
      : {};
    const items = glossaryObj ? glossaryObj[dataKey] : [];
    const itemObj = !isEmpty(items)
      ? items.find(
          (item: { displayText: string }) => item.displayText === itemName
        )
      : {};
    const itemGuid = itemObj ? itemObj[`${itemType}Guid`] : undefined;

    try {
      let relatedData = { ...formData, [`${itemType}Guid`]: itemGuid };
      let itemData: any = cloneDeep(data);
      columnVal && !isEmpty(itemData[columnVal])
        ? itemData[columnVal].push(relatedData)
        : columnVal && (itemData[columnVal] = [relatedData]);
      await assignApiMethod(entityGuid as string, itemData);
      if (!isEmpty(updateTable)) updateTable(moment.now());
      if (!isEmpty(entityGuid)) {
        let params: any = { gtype: gType, guid: entityGuid };
        dispatchApi(fetchGlossaryDetails(params));
        dispatchApi(fetchDetailPageData(entityGuid as string));
      }
      showToastSuccess(`${treeLabel} is associated successfully`, toastId);
      onClose();
    } catch (error) {
      serverError(error, toastId);
    }
  };

  const modalTitle = () => {
    if (relatedItem) return `Assign ${treeLabel} to ${columnVal}`;
    if (isEmpty(entityGuid) && !relatedItem)
      return `Assign ${treeLabel} to entity`;
    if (!isEmpty(entityGuid) && !isEmpty(gType) && !relatedItem)
      return `Assign ${treeLabel} to Category`;
    return `Assign ${treeLabel} to entity`;
  };

  return (
    <CustomModal
      open={open}
      onClose={onClose}
      title={modalTitle()}
      button1Label="Cancel"
      button1Handler={onClose}
      button2Label={"Assign"}
      maxWidth="sm"
      button2Handler={relatedItem ? handleSubmit(onSubmit) : assignItem}
      disableButton2={isSubmitting}
    >
      {relatedItem ? (
        <RelatedTermStepper
          activeStep={activeStep}
          completed={completed}
          handleStep={handleStep}
          handleBack={handleBack}
          handleNext={handleNext}
          handleReset={handleReset}
          allStepsCompleted={allStepsCompleted}
          searchTerm={searchTerm}
          setSearchTerm={setSearchTerm}
          treeData={treeData}
          loader={loader}
          handleNodeSelect={handleNodeSelect}
          control={control}
          handleSubmit={handleSubmit}
          onSubmit={onSubmit}
          isSubmitting={isSubmitting}
        />
      ) : (
        <Stack>
          <TextField
            id="outlined-search"
            fullWidth
            label={`Search ${treeLabel}`}
            type={`Search ${treeLabel}`}
            size="small"
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
              setSearchTerm(e.target.value);
            }}
          />
          <FormTreeView
            treeData={treeData}
            searchTerm={searchTerm}
            treeName={treeLabel}
            loader={loader}
            onNodeSelect={handleNodeSelect}
          />
        </Stack>
      )}
    </CustomModal>
  );
};

export default AssignGlossaryItem;
