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

import { useState, useEffect, useMemo } from "react";
import SideBarTree from "../../SideBar/SideBarTree/SideBarTree.tsx";
import { customSortByObjectKeys, isEmpty, noTreeData } from "@utils/Utils";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import type { Props } from "@models/treeStructureType.ts";
import {
  ServiceTypeArrType,
  ServiceTypeInterface
} from "@models/entityTreeType.ts";
import {
  EnumCategoryRelation,
  EnumCategoryRelations
} from "@models/glossaryTreeType.ts";
import { fetchGlossaryData } from "@redux/slice/glossarySlice.ts";
import { getGlossaryChildrenData } from "@utils/CommonViewFunction.ts";

const GlossaryTree = ({ sideBarOpen, searchTerm }: Props) => {
  const dispatch = useAppDispatch();
  const { glossaryData, loading }: any = useAppSelector(
    (state: any) => state.glossary
  );
  const [glossaryType, setGlossaryType] = useState<boolean>(true);
  const [glossaryTypeData, setGlossaryTypeData] = useState<
    ServiceTypeArrType<typeof glossaryType>
  >([]);

  useEffect(() => {
    dispatch(fetchGlossaryData());
  }, []);

  const fetchInitialData = async () => {
    await dispatch(fetchGlossaryData());
  };

  useEffect(() => {
    let newServiceTypeArr: ServiceTypeArrType<typeof glossaryType> = [];

    newServiceTypeArr =
      glossaryData != null
        ? glossaryData.map(
            (glossary: {
              name: string;
              subTypes: [];
              guid: string;
              superTypes: string[];
              categories: EnumCategoryRelation[];
              terms: EnumCategoryRelation[];
            }) => {
              let categoryRelation: EnumCategoryRelation[] = [];
              const {
                categories = [],
                terms = [],
                name = "",
                guid = ""
              } = glossary;
              categories?.map((obj: EnumCategoryRelations) => {
                if (!isEmpty(obj.parentCategoryGuid)) {
                  categoryRelation.push(obj as EnumCategoryRelations);
                }
              });

              const getChildren = (glossaries: {
                children: EnumCategoryRelation[];
                parent: string;
              }) => {
                const { children = [], parent = "" } = glossaries;
                return !isEmpty(children)
                  ? children
                      .map((glossariesType: any) => {
                        const getChild = () => {
                          return categoryRelation
                            .map((obj: EnumCategoryRelations) => {
                              const {
                                displayText = "",
                                termGuid = "",
                                categoryGuid = "",
                                parentCategoryGuid = ""
                              } = obj || {};
                              if (
                                parentCategoryGuid ==
                                glossariesType?.categoryGuid
                              ) {
                                return {
                                  ["name"]: displayText,
                                  id: displayText,
                                  ["children"]: [],
                                  types: "child",
                                  parent: parent,
                                  cGuid: glossaryType ? termGuid : categoryGuid,
                                  guid: guid
                                };
                              }
                            })
                            .filter(Boolean);
                        };
                        const {
                          parentCategoryGuid = "",
                          displayText = "",
                          termGuid = "",
                          categoryGuid = ""
                        } = glossariesType || {};
                        if (isEmpty(parentCategoryGuid)) {
                          return {
                            ["name"]: displayText,
                            id: displayText,
                            ["children"]: getChild(),
                            types: "child",
                            parent: parent,
                            cGuid: glossaryType ? termGuid : categoryGuid,
                            guid: guid
                          };
                        }
                      })
                      .filter(Boolean)
                  : [];
              };

              let children: any = glossaryType
                ? getChildren({
                    children: terms,
                    parent: name
                  })
                : getChildren({
                    children: categories,
                    parent: name
                  });

              return {
                [name]: {
                  ["name"]: name || "",
                  ["children"]: children || [],
                  id: guid || "",
                  types: "parent",
                  parent: name || "",
                  guid: guid || ""
                }
              };
            }
          )
        : [];

    setGlossaryTypeData(newServiceTypeArr);
  }, [glossaryData, glossaryType]);

  const treeData = useMemo(() => {
    return !isEmpty(glossaryData)
      ? getGlossaryChildrenData(
          customSortByObjectKeys(glossaryTypeData as ServiceTypeInterface[])
        )
      : noTreeData();
  }, [glossaryType, glossaryTypeData]);

  return (
    <SideBarTree
      treeData={treeData}
      treeName={"Glossary"}
      setisEmptyServicetype={setGlossaryType}
      isEmptyServicetype={glossaryType}
      refreshData={fetchInitialData}
      sideBarOpen={sideBarOpen}
      loader={loading}
      searchTerm={searchTerm}
    />
  );
};

export default GlossaryTree;
