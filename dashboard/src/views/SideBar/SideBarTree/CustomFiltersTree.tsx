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
  groupBy,
  isArray,
  isEmpty
} from "@utils/Utils";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import type { Props } from "@models/treeStructureType.ts";
import {
  ChildrenInterface,
  ChildrenInterfaces,
  ServiceTypeFlatInterface,
  ServiceTypeInterface
} from "@models/entityTreeType.ts";
import {
  SavedSearchArrType,
  SavedSearchDataType,
  SavedSearchArrInterface
} from "@models/savedSearchType.ts";
import { fetchSavedSearchData } from "@redux/slice/savedSearchSlice.ts";
import { globalSessionData } from "@utils/Enum.ts";

const CustomFiltersTree = ({ sideBarOpen, searchTerm }: Props) => {
  const dispatch = useAppDispatch();
  const { savedSearchData }: any = useAppSelector(
    (state: any) => state.savedSearch
  );
  const { relationshipSearch = {} } = globalSessionData || {};

  const [savedSearchType, setsavedSearchType] = useState<boolean>(true);
  const [savedSearchTypeData, setSavedSearchTypeData] = useState<
    SavedSearchArrType<typeof savedSearchType>
  >([]);
  const [customFilterLoader, setCustomFilterLoader] = useState<boolean>(false);

  useEffect(() => {
    setCustomFilterLoader(true);
    dispatch(fetchSavedSearchData());
    setCustomFilterLoader(false);
  }, []);

  const fetchInitialData = async () => {
    setCustomFilterLoader(true);
    await dispatch(fetchSavedSearchData());
    setCustomFilterLoader(false);
  };

  useEffect(() => {
    let savedSearchTypeArr = [];
    let emptySearchTypes = [
      { name: "Advanced Search", searchType: "ADVANCED" },
      { name: "Basic Search", searchType: "BASIC" },
      ...(relationshipSearch
        ? [{ name: "Relationship Search", searchType: "BASIC_RELATIONSHIP" }]
        : [])
    ];

    let searchTypes = !isEmpty(savedSearchData)
      ? savedSearchType
        ? groupBy(savedSearchData, "searchType")
        : savedSearchData
      : groupBy(emptySearchTypes, "searchType");
    for (let type in searchTypes) {
      const getType = (type: string) => {
        if (type == "BASIC") {
          return "Basic Search";
        } else if (type == "ADVANCED") {
          return "Advanced Search";
        } else if (type == "BASIC_RELATIONSHIP") {
          return "Relationship Search";
        }
      };
      const getChildren = (types: SavedSearchDataType[], type: string) => {
        if (!isArray(types) || isEmpty(types.length)) {
          return [];
        }
        return types.map((obj: { name: string }) => ({
          name: obj.name,
          children: [],
          types: "child",
          parent: type
        }));
      };
      let name = savedSearchType ? getType(type) : searchTypes[type].name;
      let children =
        savedSearchType && !isEmpty(savedSearchData)
          ? getChildren(searchTypes[type] as SavedSearchDataType[], type)
          : [];
      savedSearchTypeArr.push({
        name,
        children,
        types: "parent",
        parent: savedSearchType ? type : searchTypes[type].searchType
      });
    }

    const existingSearchTypes = savedSearchTypeArr.map((child) => child.parent);
    if (savedSearchType) {
      emptySearchTypes.forEach((typeObj) => {
        if (!existingSearchTypes.includes(typeObj.searchType)) {
          savedSearchTypeArr.push({
            name: typeObj.searchType,
            children: [],
            types: "parent",
            parent: typeObj.searchType
          });
        }
      });
    }
    setSavedSearchTypeData(savedSearchTypeArr as any);
  }, [savedSearchData, savedSearchType]);

  const generateChildrenData = useMemo(() => {
    const child = (childs: any) => {
      return customSortBy(
        childs?.map((obj: ChildrenInterface) => {
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
            parent: obj?.parent
          };
        }),
        ["label"]
      );
    };

    return (serviceTypeData: ServiceTypeInterface[]) =>
      serviceTypeData.map((type: any) => ({
        id: type.name,
        label: type.name,
        children: savedSearchType
          ? child(type.children as ChildrenInterfaces[])
          : [],
        types: type.types,
        parent: type.parent
      }));
  }, [savedSearchType]);

  const treeData = useMemo(() => {
    return savedSearchType
      ? customSortBy(
          generateChildrenData(
            savedSearchTypeData as unknown as SavedSearchArrInterface[]
          ),
          ["label"]
        )
      : generateChildrenData(
          customSortByObjectKeys(
            savedSearchTypeData as ServiceTypeFlatInterface[]
          )
        );
  }, [savedSearchType, savedSearchTypeData]);

  return (
    <SideBarTree
      treeData={treeData}
      treeName={"CustomFilters"}
      setisEmptyServicetype={setsavedSearchType}
      isEmptyServicetype={savedSearchType}
      refreshData={fetchInitialData}
      sideBarOpen={sideBarOpen}
      loader={customFilterLoader}
      searchTerm={searchTerm}
    />
  );
};

export default CustomFiltersTree;
