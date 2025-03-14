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

import { assignGlossaryType } from "@api/apiMethods/glossaryApiMethod";
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
import { Stack, TextField } from "@mui/material";
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
import { useMemo, useRef, useState } from "react";
import { useLocation, useParams } from "react-router-dom";
import { toast } from "react-toastify";

const AssignCategory = ({
  open,
  onClose,
  data,
  updateTable
}: {
  open: boolean;
  onClose: () => void;
  data: any;
  updateTable: any;
}) => {
  const { categories } = data;
  const dispatchApi = useAppDispatch();
  const location = useLocation();
  const { guid: entityGuid } = useParams();
  const searchParams = new URLSearchParams(location.search);
  const gType = searchParams.get("gtype");
  const toastId: any = useRef(null);
  const categoryNames = !isEmpty(categories)
    ? categories?.map((obj: { displayText: string }) => {
        return obj.displayText;
      })
    : [];
  const { glossaryData, loader }: any = useAppSelector(
    (state: any) => state.glossary
  );
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedNode, setSelectedNode] = useState(null);

  const glossary = [...glossaryData];

  const updatedGlossary = glossary.map((gloss) => {
    if (isEmpty(gloss?.catgeories)) {
      return gloss;
    }

    return {
      ...gloss,
      categories: gloss?.categories?.filter(
        (category: { displayText: string }) =>
          !categoryNames.includes(category.displayText)
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
                                cGuid: obj.categoryGuid,
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
                          cGuid: glossariesType.categoryGuid,
                          guid: glossary.guid
                        };
                      }
                    })
                    .filter(Boolean)
                : [];
            };

            let name: string = glossary.name,
              children: any = getChildren({
                children: glossary?.categories,
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

  const assignCatgeory = async () => {
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
    let termObj = !isEmpty(glossaryObj?.categories)
      ? glossaryObj?.categories.find(
          (category: { displayText: string }) =>
            category.displayText == termName
        )
      : {};

    const { categoryGuid } = termObj || {};

    try {
      setLoading(true);
      let categoryData: any = cloneDeep(data);
      if (categoryData.categories) {
        categoryData.categories.push({ categoryGuid: categoryGuid });
      } else {
        categoryData.categories = [{ categoryGuid: categoryGuid }];
      }

      await assignGlossaryType(entityGuid as string, categoryData);
      toast.success(`${"Category"} is associated successfully`);
      setLoading(false);
      onClose();
      if (!isEmpty(updateTable)) {
        updateTable(moment.now());
      }
      if (!isEmpty(entityGuid)) {
        let params: any = { gtype: gType, guid: entityGuid };
        dispatchApi(fetchGlossaryData());
        dispatchApi(fetchGlossaryDetails(params));
        dispatchApi(fetchDetailPageData(entityGuid as string));
      }
    } catch (error) {
      setLoading(false);
      console.log(`Error occur while assigning ${"Category"}`, error);
      serverError(error, toastId);
    }
  };

  return (
    <>
      <CustomModal
        open={open}
        onClose={onClose}
        title={`Assign Category to term`}
        button1Label="Cancel"
        button1Handler={onClose}
        button2Label={"Assign"}
        maxWidth="sm"
        button2Handler={assignCatgeory}
        disableButton2={loading}
      >
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
            treeName={"Category"}
            loader={loader}
            onNodeSelect={handleNodeSelect}
          />
        </Stack>
      </CustomModal>
    </>
  );
};

export default AssignCategory;
