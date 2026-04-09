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

import { useEffect, useMemo, useState } from "react";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import SideBarTree from "../../SideBar/SideBarTree/SideBarTree.tsx";
import { customSortBy, isEmpty, noTreeData } from "@utils/Utils.ts";
import type { EnumTypeDefData, Props } from "@models/treeStructureType.ts";
import { fetchBusinessMetaData } from "@redux/slice/typeDefSlices/typedefBusinessMetadataSlice.ts";

const BusinessMetadataTree = (props: Props) => {
  const { sideBarOpen, searchTerm } = props;
  const dispatch = useAppDispatch();
  const [businessMetadataData, setBusinessMetadataData] = useState([]);
  const { businessMetaData, loading }: any = useAppSelector(
    (state: any) => state.businessMetaData
  );

  useEffect(() => {
    dispatch(fetchBusinessMetaData());
  }, []);

  useEffect(() => {
    if (businessMetaData?.businessMetadataDefs != undefined) {
      const businessMetaDataStructure =
        businessMetaData.businessMetadataDefs.map((obj: EnumTypeDefData) => {
          return {
            id: obj.name,
            label: obj.name,
            childrenData: [],
            guid: obj.guid
          };
        });
      setBusinessMetadataData(businessMetaDataStructure);
    }
  }, [businessMetaData]);

  const fetchInitialData = async () => {
    await dispatch(fetchBusinessMetaData());
  };
  const treeData = useMemo(() => {
    return !isEmpty(businessMetadataData)
      ? customSortBy(businessMetadataData, ["label"])
      : noTreeData();
  }, [businessMetadataData]);

  return (
    <SideBarTree
      treeData={treeData}
      treeName={"Business MetaData"}
      refreshData={fetchInitialData}
      sideBarOpen={sideBarOpen}
      loader={loading}
      searchTerm={searchTerm}
    />
  );
};

export default BusinessMetadataTree;
