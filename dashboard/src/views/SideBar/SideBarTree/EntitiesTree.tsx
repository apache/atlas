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

import React from "react";
import { useState, useEffect, useMemo } from "react";
import SideBarTree from "../../SideBar/SideBarTree/SideBarTree.tsx";
import { addOnEntities } from "@utils/Enum";
import { customSortBy, customSortByObjectKeys, isEmpty } from "@utils/Utils";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import type { Props } from "@models/treeStructureType.ts";
import {
  ChildrenInterface,
  ServiceTypeArrType,
  ServiceTypeFlatInterface,
  ServiceTypeInterface,
  TypedefHeaderDataType,
  TypeHeaderInterface
} from "@models/entityTreeType.ts";
import { fetchEntityData } from "@redux/slice/typeDefSlices/typedefEntitySlice.ts";
import { fetchTypeHeaderData } from "@redux/slice/typeDefSlices/typeDefHeaderSlice.ts";
import { fetchMetricEntity } from "@redux/slice/metricsSlice.ts";

const EntitiesTree = ({ sideBarOpen, searchTerm }: Props) => {
  const dispatch = useAppDispatch();
  const { typeHeaderData, loading }: TypedefHeaderDataType = useAppSelector(
    (state: any) => state.typeHeader
  );
  const { allEntityTypesData }: any = useAppSelector(
    (state: any) => state.allEntityTypes
  );
  const { metricsData }: any = useAppSelector((state: any) => state.metrics);
  const [isEmptyServicetype, setisEmptyServicetype] = useState<boolean>(false);
  const [isGroupView, setisGroupView] = useState<boolean>(true);
  const [serviceTypeArr, setServiceTypeArr] = useState<
    ServiceTypeArrType<typeof isGroupView>
  >([]);
  const [serviceTypeArrwithEntity, setServiceTypeArrwithEntity] = useState<
    ServiceTypeArrType<typeof isGroupView>
  >([]);

  useEffect(() => {
    dispatch(fetchEntityData());
  }, []);

  const fetchInitialData = async () => {
    await dispatch(fetchEntityData());
    await dispatch(fetchMetricEntity());
    await dispatch(fetchTypeHeaderData());
  };

  useEffect(() => {
    if (!isEmpty(typeHeaderData)) {
      const newServiceTypeArr: ServiceTypeArrType<typeof isGroupView> = [];
      const newServiceTypeArrwithEntity: ServiceTypeArrType<
        typeof isGroupView
      > = [];

      typeHeaderData?.forEach((entity: TypeHeaderInterface) => {
        let { serviceType = "other_types", category, name, guid } = entity;
        let entityCount = 0;
        let modelName = "";
        let children: ChildrenInterface = {
          gType: "",
          guid: "",
          id: "",
          name: "",
          type: "",
          text: ""
        };

        if (
          category === "ENTITY" &&
          metricsData &&
          !isEmpty(metricsData.data)
        ) {
          entityCount =
            (metricsData.data.entity.entityActive[name] || 0) +
            (metricsData.data.entity.entityDeleted[name] || 0);
          modelName = entityCount ? `${name} (${entityCount})` : name;
          children = {
            text: modelName,
            name: name,
            type: category,
            gType: "Entity",
            guid: guid,
            id: guid
          };

          generateServiceTypeArr(
            newServiceTypeArr,
            serviceType,
            children,
            entityCount
          );

          if (entityCount > 0) {
            generateServiceTypeArr(
              newServiceTypeArrwithEntity,
              serviceType,
              children,
              entityCount
            );
          }
        }
      });

      setServiceTypeArr(pushRootEntityTotree(newServiceTypeArr));
      setServiceTypeArrwithEntity(
        pushRootEntityTotree(newServiceTypeArrwithEntity)
      );
    }
  }, [typeHeaderData, metricsData]);

  const generateServiceTypeArr = (
    entityCountArr: ServiceTypeArrType<typeof isGroupView>,
    serviceType: string,
    children: ChildrenInterface | any,
    entityCount: number
  ) => {
    if (isGroupView) {
      const existingServiceType: any = entityCountArr.find(
        (obj): obj is ServiceTypeInterface =>
          typeof obj === "object" && obj !== null && serviceType in obj
      );
      if (existingServiceType) {
        existingServiceType[serviceType].children.push(children);
        existingServiceType[serviceType].totalCount += entityCount;
      } else {
        const newServiceTypeObj: ServiceTypeInterface = {
          [serviceType]: {
            children: [children],
            name: serviceType,
            totalCount: entityCount
          }
        };
        entityCountArr.push(
          newServiceTypeObj as ServiceTypeFlatInterface & ServiceTypeInterface
        );
      }
    } else {
      entityCountArr.push(children);
    }
  };

  const pushRootEntityTotree = (
    entities: ServiceTypeArrType<typeof isGroupView>
  ) => {
    const rootEntityChildren: ChildrenInterface = {
      gType: "Entity",
      guid: addOnEntities[0],
      id: addOnEntities[0],
      name: addOnEntities[0],
      type: allEntityTypesData?.category,
      text: addOnEntities[0]
    };

    if (isGroupView) {
      const hasOtherTypes = entities.some(
        (obj: any) => obj["other_types"] !== undefined
      );
      if (hasOtherTypes) {
        const entityTypeIndex = entities.findIndex(
          (obj): obj is ServiceTypeInterface => "other_types" in obj
        );

        (entities[entityTypeIndex] as ServiceTypeInterface)[
          "other_types"
        ].children.push(rootEntityChildren);
      } else {
        entities.push({
          other_types: {
            name: "other_types",
            children: [rootEntityChildren],
            totalCount: 0
          }
        } as any);
      }
    } else {
      entities.push(
        rootEntityChildren as ServiceTypeFlatInterface & ServiceTypeInterface
      );
    }

    return entities;
  };

  const generateChildrenData = useMemo(() => {
    const child = (childs: ChildrenInterface[]) => {
      if (!childs || childs.length === 0) return [];
      return customSortBy(
        childs.map((obj) => ({
          id: obj.name,
          label: obj.text,
          types: "child"
        })),
        ["label"]
      );
    };

    if (isGroupView) {
      return (serviceTypeData: ServiceTypeInterface[]) =>
        serviceTypeData.map((entity: any) => {
          const key = Object.keys(entity)[0];
          const entityData = entity[key];
          return {
            id: entityData.name,
            label:
              entityData.totalCount === 0
                ? entityData.name
                : `${entityData.name} (${entityData.totalCount})`,
            children: child(entityData.children),
            types: "parent"
          };
        });
    } else {
      return (serviceTypeData: ServiceTypeFlatInterface[]) =>
        serviceTypeData.flatMap((entity: any) =>
          entity[Object.keys(entity)[0]].children?.map(
            (child: ChildrenInterface) => ({
              id: child.name,
              label: child.text
            })
          )
        );
    }
  }, [isGroupView]);

  const serviceTypeData = !isEmptyServicetype
    ? serviceTypeArrwithEntity
    : serviceTypeArr;

  const treeData = useMemo(() => {
    if (isGroupView) {
      const sortedData = customSortByObjectKeys(
        serviceTypeData as ServiceTypeInterface[]
      );
      return generateChildrenData(sortedData);
    } else {
      const flatData = generateChildrenData(serviceTypeData as any);
      return customSortBy(flatData, ["label"]);
    }
  }, [isGroupView, isEmptyServicetype, serviceTypeData]);

  return (
    <SideBarTree
      treeData={treeData}
      treeName={"Entities"}
      setisEmptyServicetype={setisEmptyServicetype}
      isEmptyServicetype={isEmptyServicetype}
      refreshData={fetchInitialData}
      isGroupView={isGroupView}
      setisGroupView={setisGroupView}
      sideBarOpen={sideBarOpen}
      loader={loading}
      searchTerm={searchTerm}
    />
  );
};

export default React.memo(EntitiesTree);
