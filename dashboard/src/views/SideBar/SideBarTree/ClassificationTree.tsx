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
import SideBarTree from "../../SideBar/SideBarTree/SideBarTree.tsx";
import { customSortBy, customSortByObjectKeys, isEmpty } from "@utils/Utils";
import { addOnClassification } from "@utils/Enum.ts";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook.ts";
import type { Props } from "@models/treeStructureType.ts";
import {
  ChildrenInterface,
  ServiceTypeArrType
} from "@models/entityTreeType.ts";
import { fetchClassificationData } from "@redux/slice/typeDefSlices/typedefClassificationSlice.ts";
import { fetchMetricEntity } from "@redux/slice/metricsSlice.ts";
import React from "react";
import { isEmptyValueCheck } from "@utils/Helper.ts";

const ClassificationTree = (props: Props) => {
  const { sideBarOpen, searchTerm } = props;
  const dispatch = useAppDispatch();
  const { classificationData, loadingClassification }: any = useAppSelector(
    (state: any) => state.classification
  );
  const [isEmptyClassification, setIsEmptyClassification] =
    useState<boolean>(false);
  const { metricsData }: any = useAppSelector((state: any) => state.metrics);
  const [isGroupView, setisGroupView] = useState<boolean>(true);

  useEffect(() => {
    dispatch(fetchClassificationData());
  }, []);

  const fetchInitialData = async () => {
    await dispatch(fetchClassificationData());
    await dispatch(fetchMetricEntity());
  };

  let childrenData: object[] = [];
  childrenData =
    classificationData != null
      ? classificationData.classificationDefs.map(
          (obj: {
            name: string;
            subTypes: [];
            guid: string;
            superTypes: string[];
          }) => {
            const getEntityTree = (classification: {
              name: string;
              subTypes: string[];
              guid: string;
              superTypes: string[];
            }) => {
              const getChildren = (classify: { children: any }) => {
                return classify.children?.subTypes?.map((subtype: any) => {
                  var child: any =
                    classificationData != null
                      ? classificationData?.classificationDefs?.find(
                          (obj: { name: string }) => obj.name == subtype
                        )
                      : {};
                  let tagEntityCount: number =
                    metricsData != null
                      ? metricsData.data.tag.tagEntities[subtype]
                      : null;
                  let tagName = tagEntityCount
                    ? `${subtype} (${tagEntityCount})`
                    : subtype;
                  if (
                    isEmptyClassification &&
                    isEmptyValueCheck(tagEntityCount)
                  ) {
                    return;
                  }
                  return {
                    ["name"]: `${child.guid}@${classify.children.name}`,
                    ["totalCount"]: tagEntityCount,
                    id: `${child.guid}@${classify.children.name}`,
                    text: tagName,
                    types: "child",
                    ["children"]: isGroupView
                      ? getChildren({
                          children: child
                        })
                      : null,
                    nodeName: classify.children.name
                  };
                });
              };
              let parentTagEntityCount: number =
                metricsData?.data?.tag?.tagEntities?.[classification.name];
              let name: string =
                  parentTagEntityCount !== undefined
                    ? `${classification.name} (${parentTagEntityCount})`
                    : classification.name,
                children: any = isGroupView
                  ? getChildren({
                      children: classification
                    })
                  : null,
                totalCount: number = 0;
              if (
                isEmptyClassification &&
                isEmptyValueCheck(parentTagEntityCount) &&
                isEmpty(obj.subTypes) &&
                isEmpty(obj.superTypes)
              ) {
                return;
              }

              return {
                [name]: {
                  ["name"]: name,
                  ["children"]: children,
                  totalCount: totalCount,
                  id: classification.guid,
                  types: "parent",
                  text: classification.name,
                  nodeName: classification.name
                }
              };
            };
            if (isEmpty(obj.superTypes)) {
              return getEntityTree(obj);
            } else {
              return;
            }
          }
        )
      : [];

  const pushRootEntityTotree = (
    entities: ServiceTypeArrType<typeof isGroupView>
  ) => {
    let rootEntityChildren: ChildrenInterface | any = {
      gType: "",
      guid: "",
      id: "",
      name: "",
      text: ""
    };

    addOnClassification.map((obj) => {
      rootEntityChildren = {
        gType: "Classification",
        guid: obj,
        id: obj,
        name: obj,
        text: obj,
        label: obj
      };

      entities.push({ [obj]: rootEntityChildren } as any);
    });

    return entities;
  };
  childrenData = pushRootEntityTotree(
    childrenData as ServiceTypeArrType<typeof isGroupView>
  );
  const generateChildrenData = (
    serviceTypeData: ServiceTypeArrType<typeof isGroupView>[] | any
  ) => {
    let childrensData:
      | {
          id: string;
          label: string;
          children: { id: string; label: string }[];
        }[]
      | any = [];
    const child = (childs: ChildrenInterface[]) => {
      let filterChild: { id: string; label: string }[] = [];
      childs?.map((obj) => {
        !isEmpty(obj) &&
          filterChild?.push({
            id: obj.name,
            label: obj.text,
            children: child(obj.children),
            types: obj.types,
            text: obj.text,
            nodeName: obj.nodeName
          } as any);
      });
      return customSortBy(filterChild, ["label"]);
    };
    if (isGroupView) {
      serviceTypeData?.map((entity: any) => {
        return childrensData.push({
          id: entity[Object.keys(entity)[0]]?.name,
          label:
            entity[Object.keys(entity)[0]].totalCount == undefined ||
            entity[Object.keys(entity)[0]].totalCount == 0
              ? entity[Object.keys(entity)[0]].name
              : `${entity[Object.keys(entity)[0]].name} (${
                  entity[Object.keys(entity)[0]].totalCount
                })`,
          children: child(entity[Object.keys(entity)[0]].children),
          types: entity[Object.keys(entity)[0]]?.types,
          text: entity[Object.keys(entity)[0]]?.text,
          nodeName: entity[Object.keys(entity)[0]]?.nodeName
        });
      });
    } else {
      serviceTypeData.map((entity: any) => {
        return childrensData.push({
          id: entity[Object.keys(entity)[0]]?.name,
          label:
            entity[Object.keys(entity)[0]].totalCount == undefined ||
            entity[Object.keys(entity)[0]].totalCount == 0
              ? entity[Object.keys(entity)[0]].name
              : `${entity[Object.keys(entity)[0]].name} (${
                  entity[Object.keys(entity)[0]].totalCount
                })`,
          types: entity[Object.keys(entity)[0]]?.types,
          text: entity[Object.keys(entity)[0]]?.text,
          nodeName: entity[Object.keys(entity)[0]]?.nodeName
        });
      });
    }

    return childrensData;
  };

  const treeData = useMemo(() => {
    return generateChildrenData(
      customSortByObjectKeys(childrenData.filter(Boolean))
    );
  }, [classificationData, isEmptyClassification]);

  return (
    <SideBarTree
      treeData={treeData}
      treeName={"Classifications"}
      setisEmptyServicetype={setIsEmptyClassification}
      isEmptyServicetype={isEmptyClassification}
      refreshData={fetchInitialData}
      isGroupView={isGroupView}
      setisGroupView={setisGroupView}
      sideBarOpen={sideBarOpen}
      loader={loadingClassification}
      searchTerm={searchTerm}
    />
  );
};

export default React.memo(ClassificationTree);
