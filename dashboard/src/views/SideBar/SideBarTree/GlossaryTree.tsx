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
import {
  customSortBy,
  customSortByObjectKeys,
  isEmpty,
  noTreeData
} from "@utils/Utils";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import type { Props } from "@models/treeStructureType.ts";
import {
  ChildrenInterface,
  ChildrenInterfaces,
  ServiceTypeArrType,
  ServiceTypeInterface
} from "@models/entityTreeType.ts";
import {
  EnumCategoryRelation,
  EnumCategoryRelations
} from "@models/glossaryTreeType.ts";
import { fetchGlossaryData } from "@redux/slice/glossarySlice.ts";

const GlossaryTree = ({ sideBarOpen, searchTerm }: Props) => {
  const dispatch = useAppDispatch();
  const { glossaryData, loading }: any = useAppSelector(
    (state: any) => state.glossary
  );
  const [glossaryType, setGlossaryType] = useState<boolean>(true);
  const [glossaryTypeData, setGlossaryData] = useState<
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
                                  cGuid: glossaryType
                                    ? obj.termGuid
                                    : obj.categoryGuid,
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
                            cGuid: glossaryType
                              ? glossariesType.termGuid
                              : glossariesType.categoryGuid,
                            guid: glossary.guid
                          };
                        }
                      })
                      .filter(Boolean)
                  : [];
              };

              let name: string = glossary.name,
                children: any = glossaryType
                  ? getChildren({
                      children: glossary?.terms,
                      parent: glossary.name
                    })
                  : getChildren({
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

    setGlossaryData(newServiceTypeArr);
  }, [glossaryData, glossaryType]);

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
  }, [glossaryType]);

  const treeData = useMemo(() => {
    return !isEmpty(glossaryData)
      ? generateChildrenData(
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
