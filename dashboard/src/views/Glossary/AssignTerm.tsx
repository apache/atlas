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
  assignGlossaryType,
  assignTermstoCategory,
  assignTermstoEntites
} from "@api/apiMethods/glossaryApiMethod";
import FormTreeView from "@components/Forms/FormTreeView";
import CustomModal from "@components/Modal";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import {
  ChildrenInterface,
  ChildrenInterfaces,
  ServiceTypeInterface
} from "@models/entityTreeType";
import {
  EnumCategoryRelation,
  EnumCategoryRelations
} from "@models/glossaryTreeType";
import Button from "@mui/material/Button";

import InputLabel from "@mui/material/InputLabel";
import Stack from "@mui/material/Stack";
import Step from "@mui/material/Step";
import StepButton from "@mui/material/StepButton";
import Stepper from "@mui/material/Stepper";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";
import { fetchGlossaryDetails } from "@redux/slice/glossaryDetailsSlice";
import { fetchGlossaryData } from "@redux/slice/glossarySlice";
import { cloneDeep } from "@utils/Helper";
import {
  customSortBy,
  customSortByObjectKeys,
  isEmpty,
  noTreeData,
  serverError
} from "@utils/Utils";
import moment from "moment-timezone";
import React from "react";
import { useMemo, useRef, useState } from "react";
import { Controller, useForm } from "react-hook-form";
import { useLocation, useParams } from "react-router-dom";
import { toast } from "react-toastify";

const steps = ["Select Term", "Attributes"];

const AssignTerm = ({
  open,
  onClose,
  data,
  updateTable,
  relatedTerm,
  columnVal,
  setRowSelection
}: {
  open: boolean;
  onClose: () => void;
  data: any;
  updateTable: any;
  relatedTerm: any;
  columnVal?: any;
  setRowSelection?: any;
}) => {
  const { guid, meanings, terms } = data;
  const dispatchApi = useAppDispatch();
  const location = useLocation();
  const toastId: any = useRef(null);
  const searchParams = new URLSearchParams(location.search);
  const gType = searchParams.get("gtype");
  const { guid: entityGuid } = useParams();
  const termNames = !isEmpty(meanings || terms || data[columnVal])
    ? (meanings || terms || data[columnVal])?.map(
        (obj: { displayText: string }) => {
          return obj.displayText;
        }
      )
    : [];
  const { glossaryData, loader }: any = useAppSelector(
    (state: any) => state.glossary
  );
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedNode, setSelectedNode] = useState(null);
  const [activeStep, setActiveStep] = React.useState(0);
  const [completed, setCompleted] = React.useState<{
    [k: number]: boolean;
  }>({});
  const {
    control,
    handleSubmit,
    formState: { isSubmitting }
  } = useForm();

  const glossary = [...glossaryData];

  const totalSteps = () => {
    return steps.length;
  };

  const completedSteps = () => {
    return Object.keys(completed).length;
  };

  const isLastStep = () => {
    return activeStep === totalSteps() - 1;
  };

  const allStepsCompleted = () => {
    return completedSteps() === totalSteps();
  };

  const handleNext = () => {
    if (isEmpty(selectedNode)) {
      toast.error("Please select Term for association");
      return;
    }
    const newActiveStep =
      isLastStep() && !allStepsCompleted()
        ? steps.findIndex((_step, i) => !(i in completed))
        : activeStep + 1;
    setActiveStep(newActiveStep);
  };

  const handleBack = () => {
    setActiveStep((prevActiveStep) => prevActiveStep - 1);
  };

  const handleStep = (step: number) => () => {
    setActiveStep(step);
  };

  const handleReset = () => {
    setActiveStep(0);
    setCompleted({});
  };

  const updatedGlossary = glossary.map((gloss) => {
    if (isEmpty(gloss?.terms)) {
      return gloss;
    }

    return {
      ...gloss,
      terms: gloss?.terms?.filter(
        (term: { displayText: string }) => !termNames.includes(term.displayText)
      )
    };
  });

  let newServiceTypeArr: any = [];

  newServiceTypeArr =
    updatedGlossary != null
      ? updatedGlossary.map(
          (glossary: {
            name: string;
            subTypes: [];
            guid: string;
            superTypes: string[];
            categories: EnumCategoryRelation[];
            terms: EnumCategoryRelation[];
          }) => {
            let categoryRelation: EnumCategoryRelation[] = [];

            glossary?.categories?.map((obj: EnumCategoryRelations) => {
              if (obj.parentCategoryGuid != undefined) {
                categoryRelation.push(obj as EnumCategoryRelations);
              }
            });

            const getChildren = (glossaries: {
              children: EnumCategoryRelation[];
              parent: string;
            }) => {
              return !isEmpty(glossaries.children)
                ? glossaries.children
                    .map((glossariesType: any) => {
                      const getChild = () => {
                        return categoryRelation
                          .map((obj: EnumCategoryRelations) => {
                            if (
                              obj.parentCategoryGuid ==
                              glossariesType.categoryGuid
                            ) {
                              return {
                                ["name"]: obj.displayText,
                                id: obj.displayText,
                                ["children"]: [],
                                types: "child",
                                parent: glossaries.parent,
                                cGuid: obj.termGuid,
                                guid: glossary.guid
                              };
                            }
                          })
                          .filter(Boolean);
                      };
                      if (glossariesType.parentCategoryGuid == undefined) {
                        return {
                          ["name"]: glossariesType.displayText,
                          id: glossariesType.displayText,
                          ["children"]: getChild(),
                          types: "child",
                          parent: glossaries.parent,
                          cGuid: glossariesType.termGuid,
                          guid: glossary.guid
                        };
                      }
                    })
                    .filter(Boolean)
                : [];
            };

            let name: string = glossary.name,
              children: any = getChildren({
                children: glossary?.terms,
                parent: glossary.name
              });

            return {
              [name]: {
                ["name"]: name,
                ["children"]: children || [],
                id: glossary.guid,
                types: "parent",
                parent: name,
                guid: glossary.guid
              }
            };
          }
        )
      : [];

  const generateChildrenData = useMemo(() => {
    const child = (childs: any) => {
      return customSortBy(
        childs.map((obj: ChildrenInterface) => {
          return {
            id: obj?.name,
            label: obj?.name,
            children:
              obj?.children != undefined
                ? child(
                    obj.children.filter(
                      Boolean
                    ) as unknown as ChildrenInterfaces[]
                  )
                : [],
            types: obj?.types,
            parent: obj?.parent,
            guid: obj?.guid,
            cGuid: obj?.cGuid
          };
        }),
        ["label"]
      );
    };

    return (serviceTypeData: ServiceTypeInterface[]) =>
      serviceTypeData.map((entity: any) => ({
        id: entity[Object.keys(entity)[0]].name,
        label: entity[Object.keys(entity)[0]].name,
        children: child(
          entity[Object.keys(entity)[0]].children as ChildrenInterfaces[]
        ),
        types: entity[Object.keys(entity)[0]].types,
        parent: entity[Object.keys(entity)[0]].parent,
        guid: entity[Object.keys(entity)[0]].guid
      }));
  }, []);

  const treeData = useMemo(() => {
    return !isEmpty(updatedGlossary)
      ? generateChildrenData(
          customSortByObjectKeys(newServiceTypeArr as ServiceTypeInterface[])
        )
      : noTreeData();
  }, []);

  const handleNodeSelect = (nodeId: any) => {
    setSelectedNode(nodeId);
  };

  const assignTerm = async () => {
    if (isEmpty(selectedNode)) {
      toast.dismiss(toastId.current);
      toastId.current = toast.error(`No Term Selected`);
      return;
    }
    let selectedTerm: any = selectedNode;
    let termGlossaryNames = selectedTerm.split("@");
    let termName: string = termGlossaryNames[0];
    let glossaryName: string = termGlossaryNames[1];

    let glossaryObj = updatedGlossary.find(
      (obj: { name: string }) => obj.name == glossaryName
    );
    let termObj = !isEmpty(glossaryObj?.terms)
      ? glossaryObj?.terms.find(
          (term: { displayText: string }) => term.displayText == termName
        )
      : {};

    const { termGuid } = termObj || {};

    try {
      if (gType == "category" && !isEmpty(entityGuid)) {
        let termData: any = cloneDeep(data);
        if (data.terms) {
          termData.terms = [...data.terms, { termGuid: termGuid }];
        } else {
          termData.terms = [{ termGuid: termGuid }];
        }

        await assignTermstoCategory(entityGuid as string, termData);
      } else {
        let termData: any = [{ ["guid"]: guid || entityGuid }];

        if (isEmpty(guid) && isEmpty(entityGuid)) {
          termData = data.map((obj: { guid: any }) => {
            return { guid: obj.guid };
          });
        }
        await assignTermstoEntites(termGuid, termData);
      }
      toast.success(`Term is associated successfully`);
      onClose();
      if (!isEmpty(updateTable)) {
        updateTable(moment.now());
      }

      if (!isEmpty(entityGuid)) {
        dispatchApi(fetchDetailPageData(entityGuid as string));

        if (!isEmpty(gType)) {
          const params = { gtype: gType, entityGuid };
          dispatchApi(fetchGlossaryData());
          dispatchApi(fetchGlossaryDetails(params));
        }
      }

      if (!isEmpty(setRowSelection)) {
        setRowSelection({});
      }
    } catch (error) {
      console.log(`Error occur while assigningTerm`, error);
      serverError(error, toastId);
    }
  };

  const onSubmit = async (values: any) => {
    let formData = { ...values };

    if (isEmpty(selectedNode)) {
      toast.dismiss(toastId.current);
      toastId.current = toast.error(`No Term Selected`);
      return;
    }
    if (activeStep == 0) {
      toast.dismiss(toastId.current);
      toastId.current = toast.error("Please click on next step");
      return;
    }

    let selectedTerm: any = selectedNode;
    let termGlossaryNames = selectedTerm.split("@");
    let termName: string = termGlossaryNames[0];
    let glossaryName: string = termGlossaryNames[1];

    let glossaryObj = updatedGlossary.find(
      (obj: { name: string }) => obj.name == glossaryName
    );
    let termObj = !isEmpty(glossaryObj?.terms)
      ? glossaryObj?.terms.find(
          (term: { displayText: string }) => term.displayText == termName
        )
      : {};

    const { termGuid } = termObj || {};

    try {
      let relatedTermData = { ...formData, ...{ termGuid: termGuid } };

      let termData: any = cloneDeep(data);
      !isEmpty(termData[columnVal])
        ? termData[columnVal].push(relatedTermData)
        : (termData[columnVal] = [relatedTermData]);
      await assignGlossaryType(entityGuid as string, termData);
      if (!isEmpty(updateTable)) {
        updateTable(moment.now());
      }
      if (!isEmpty(entityGuid)) {
        let params: any = { gtype: gType, guid: entityGuid };
        dispatchApi(fetchGlossaryDetails(params));
        dispatchApi(fetchDetailPageData(entityGuid as string));
      }
      toast.dismiss(toastId.current);
      toastId.current = toast.success(`Term is associated successfully`);
      onClose();
    } catch (error) {
      console.log(`Error occur while assigningTerm`, error);
      serverError(error, toastId);
    }
  };

  const termModalTitle = () => {
    let type: string = "";
    if (relatedTerm) {
      type = columnVal;
    }
    if (isEmpty(entityGuid) && !relatedTerm) {
      type = "entity";
    } else if (!isEmpty(entityGuid) && !isEmpty(gType) && !relatedTerm) {
      type = "Catgeory";
    }

    return `Assign term to ${type}`;
  };

  return (
    <>
      <CustomModal
        open={open}
        onClose={onClose}
        title={termModalTitle()}
        button1Label="Cancel"
        button1Handler={onClose}
        button2Label={"Assign"}
        maxWidth="sm"
        button2Handler={relatedTerm ? handleSubmit(onSubmit) : assignTerm}
        disableButton2={isSubmitting}
      >
        {relatedTerm ? (
          <Stack gap="16px" sx={{ width: "100%" }}>
            <Stepper nonLinear activeStep={activeStep} sx={{ gap: "0.5rem" }}>
              {steps.map((label, index) => (
                <Step
                  sx={{ padding: 0, display: "flex", justifyContent: "center" }}
                  key={label}
                  completed={completed[index]}
                >
                  <StepButton
                    sx={{
                      height: "72px",
                      paddingLeft: "8px",
                      paddingRight: "8px"
                    }}
                    color="inherit"
                    className="m-0 py-0"
                    onClick={handleStep(index)}
                  >
                    {label}
                  </StepButton>
                </Step>
              ))}
            </Stepper>
            <Stack
              gap="1rem"
              sx={{
                background: "#f5f5f5",
                overflowY: "auto",
                maxHeight: "500px"
              }}
              position="relative"
              padding={"1rem"}
            >
              {allStepsCompleted() ? (
                <React.Fragment>
                  <Typography sx={{ mt: 2, mb: 1 }}>
                    All steps completed - you&apos;re finished
                  </Typography>
                  <Stack sx={{ display: "flex", flexDirection: "row", pt: 2 }}>
                    <Stack sx={{ flex: "1 1 auto" }} />
                    <Button onClick={handleReset}>Reset</Button>
                  </Stack>
                </React.Fragment>
              ) : (
                <React.Fragment>
                  {activeStep === 0 ? (
                    <>
                      <TextField
                        sx={{
                          position: "sticky",
                          top: "0",
                          background: "white"
                        }}
                        id="outlined-search"
                        fullWidth
                        label="Search Term"
                        type="Search Term"
                        size="small"
                        onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                          let newValue: string = e.target.value;
                          setSearchTerm(newValue);
                        }}
                      />
                      <FormTreeView
                        treeData={treeData}
                        searchTerm={searchTerm}
                        treeName={"Term"}
                        loader={loader}
                        onNodeSelect={handleNodeSelect}
                      />
                    </>
                  ) : (
                    <Stack>
                      <form onSubmit={handleSubmit(onSubmit)}>
                        <Stack direction="row" gap="2rem">
                          <Controller
                            control={control}
                            name={"description"}
                            render={({ field: { onChange, value } }) => (
                              <>
                                <div
                                  className="form-fields"
                                  style={{ textAlign: "right" }}
                                >
                                  <InputLabel>description</InputLabel>
                                </div>

                                <TextField
                                  margin="normal"
                                  fullWidth
                                  value={value}
                                  onChange={(e) => {
                                    const value = e.target.value;
                                    onChange(value);
                                  }}
                                  variant="outlined"
                                  size="small"
                                  placeholder={"description"}
                                  className="form-textfield"
                                />
                              </>
                            )}
                          />
                        </Stack>
                        <Stack direction="row" gap="2rem">
                          <Controller
                            control={control}
                            name={"expression"}
                            render={({ field: { onChange, value } }) => (
                              <>
                                <div
                                  className="form-fields"
                                  style={{ textAlign: "right" }}
                                >
                                  <InputLabel>expression</InputLabel>
                                </div>

                                <TextField
                                  margin="normal"
                                  fullWidth
                                  value={value}
                                  onChange={(e) => {
                                    const value = e.target.value;
                                    onChange(value);
                                  }}
                                  variant="outlined"
                                  size="small"
                                  placeholder={"expression"}
                                  className="form-textfield"
                                />
                              </>
                            )}
                          />
                        </Stack>
                        <Stack direction="row" gap="2rem">
                          <Controller
                            control={control}
                            name={"steward"}
                            render={({ field: { onChange, value } }) => (
                              <>
                                <div
                                  className="form-fields"
                                  style={{ textAlign: "right" }}
                                >
                                  <InputLabel>steward</InputLabel>
                                </div>

                                <TextField
                                  margin="normal"
                                  fullWidth
                                  value={value}
                                  onChange={(e) => {
                                    const value = e.target.value;
                                    onChange(value);
                                  }}
                                  variant="outlined"
                                  size="small"
                                  placeholder={"steward"}
                                  className="form-textfield"
                                />
                              </>
                            )}
                          />
                        </Stack>
                        <Stack direction="row" gap="2rem">
                          <Controller
                            control={control}
                            name={"source"}
                            render={({ field: { onChange, value } }) => (
                              <>
                                <div
                                  className="form-fields"
                                  style={{ textAlign: "right" }}
                                >
                                  <InputLabel>source</InputLabel>
                                </div>

                                <TextField
                                  margin="normal"
                                  fullWidth
                                  value={value}
                                  onChange={(e) => {
                                    const value = e.target.value;
                                    onChange(value);
                                  }}
                                  variant="outlined"
                                  size="small"
                                  placeholder={"source"}
                                  className="form-textfield"
                                />
                              </>
                            )}
                          />
                        </Stack>
                      </form>
                    </Stack>
                  )}
                  <Stack sx={{ display: "flex", flexDirection: "row", pt: 2 }}>
                    <Button
                      color="inherit"
                      disabled={activeStep === 0}
                      onClick={handleBack}
                      sx={{ mr: 1 }}
                    >
                      Back
                    </Button>
                    <Stack sx={{ flex: "1 1 auto" }} />
                    <Button
                      disabled={activeStep === 1}
                      onClick={handleNext}
                      sx={{ mr: 1 }}
                    >
                      Next
                    </Button>
                  </Stack>
                </React.Fragment>
              )}
            </Stack>
          </Stack>
        ) : (
          <Stack>
            <TextField
              id="outlined-search"
              fullWidth
              label="Search Term"
              type="Search Term"
              size="small"
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                let newValue: string = e.target.value;
                setSearchTerm(newValue);
              }}
            />
            <FormTreeView
              treeData={treeData}
              searchTerm={searchTerm}
              treeName={"Term"}
              loader={loader}
              onNodeSelect={handleNodeSelect}
            />
          </Stack>
        )}
      </CustomModal>
    </>
  );
};

export default AssignTerm;
