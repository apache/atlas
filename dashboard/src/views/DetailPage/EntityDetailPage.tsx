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

import React, { useEffect, useState } from "react";
import { Divider, Stack, Tabs, Typography } from "@mui/material";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import DisplayImage from "@components/EntityDisplayImage";
import {
  extractKeyValueFromEntity,
  getNestedSuperTypes,
  getTagObj,
  isEmpty
} from "@utils/Utils";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import DialogShowMoreLess from "@components/DialogShowMoreLess";
import { entityStateReadOnly, globalSessionData } from "@utils/Enum";
import { removeClassification } from "@api/apiMethods/classificationApiMethod";
import { removeTerm } from "@api/apiMethods/glossaryApiMethod";
import PropertiesTab from "./EntityDetailTabs/PropertiesTab/PropertiesTab";
import SkeletonLoader from "@components/SkeletonLoader";
import RelationshipsTab from "./EntityDetailTabs/RelationshipsTab";
import ClassificationsTab from "./EntityDetailTabs/ClassificationsTab";
import { CustomButton, LinkTab } from "@components/muiComponents";
import AuditsTab from "./EntityDetailTabs/AuditsTab";
import { EntityState } from "@models/relationshipSearchType";
import { useSelector } from "react-redux";
import SchemaTab from "./EntityDetailTabs/SchemaTab";
import ReplicationAuditTable from "./EntityDetailTabs/ReplicationAuditTab";
import ProfileTab from "./EntityDetailTabs/ProfileTab";
import LineageTab from "./EntityDetailTabs/LineageTab";
import TaskTab from "./EntityDetailTabs/TaskTab";
import { Item, samePageLinkNavigation, StyledPaper } from "@utils/Muiutils";
import { cloneDeep } from "@utils/Helper";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";
// import { increaseCounter } from "@utils/Global";

const EntityDetailPage: React.FC = () => {
  const { guid } = useParams();
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const navigate = useNavigate();
  const dispatch = useAppDispatch();
  const { taskTabEnabled = {}, uiTaskTabEnabled } = globalSessionData || {};
  const activeTab: string | undefined | null = searchParams.get("tabActive");
  const { detailPageData, loading }: any = useAppSelector(
    (state: any) => state.detailPage
  );
  const { entityData, loading: _loader } = useSelector(
    (state: EntityState) => state.entity
  );

  const { entityDefs = {} } = entityData || {};
  let entityDefObj = cloneDeep(entityDefs) || {};
  // let entityDefObj = JSON.parse(JSON.stringify(entityDefs)) || {};
  const { entity, referredEntities }: any = detailPageData || {};
  const { classifications = {} } = entity || {};

  const { name }: { name: string; found: boolean; key: any } =
    extractKeyValueFromEntity(entity);
  let isProcess: boolean = false;
  let typeName: any = extractKeyValueFromEntity(entity, "typeName");
  let entityObj =
    !isEmpty(entityDefObj) && !isEmpty(entity)
      ? entityDefObj.find((obj: { name: string }) => {
          return obj.name == entity.typeName;
        })
      : {};
  let superTypes = !isEmpty(entityDefObj)
    ? getNestedSuperTypes({
        data: entityObj,
        collection: entityDefObj
      })
    : [];
  let isLineageRender: boolean | null = superTypes.find((type) => {
    if (type === "DataSet" || type === "Process") {
      if (type === "Process") {
        isProcess = true;
      }
      return true;
    }
  });
  if (!isLineageRender) {
    isLineageRender =
      typeName === "DataSet" || typeName === "Process" ? true : null;
  }

  let schemaOptions = entityObj?.options;
  let schemaElementsAttribute = schemaOptions?.schemaElementsAttribute;

  let allTabs = [
    "properties",
    "relationship",
    "classification",
    "audit",
    "pendingTask"
  ];
  let tabsName = [...allTabs];

  const addTab = (tabs: string) => {
    return tabsName.splice(tabsName.includes("lineage") ? 5 : 4, 0, tabs);
  };

  const removeTab = (tabs: string) => {
    if (!tabsName.includes(tabs)) {
      tabsName;
    } else {
      let index = tabsName.findIndex((tab) => tab === tabs);
      tabsName.splice(index, 1);
    }
  };

  if (isLineageRender) {
    tabsName.splice(1, 0, "lineage");
  } else {
    removeTab("lineage");
  }

  if (!isEmpty(schemaElementsAttribute)) {
    addTab("schema");
  } else {
    removeTab("schema");
  }

  if (!isEmpty(entity) && entity.typeName == "AtlasServer") {
    addTab("raudits");
  } else {
    removeTab("raudits");
  }

  if (
    !isEmpty(entity) &&
    (!isEmpty(entity?.attributes?.["profileData"]) ||
      entity.typeName == "hive_db" ||
      entity.typeName == "hbase_namespace")
  ) {
    addTab("profile");
  } else {
    removeTab("profile");
  }

  const [value, setValue] = useState(
    !isEmpty(activeTab) ? tabsName.findIndex((val) => val === activeTab) : 0
  );

  let tagObj = getTagObj(entity, classifications);

  const handleChange = (event: React.SyntheticEvent, newValue: number) => {
    if (
      event.type !== "click" ||
      (event.type === "click" &&
        samePageLinkNavigation(
          event as React.MouseEvent<HTMLAnchorElement, MouseEvent>
        ))
    ) {
      let currentTabName = tabsName[newValue];
      let keys = Array.from(searchParams.keys());
      for (let i = 0; i < keys.length; i++) {
        if (keys[i] != "searchType") {
          searchParams.delete(keys[i]);
        }
      }
      // increaseCounter();
      searchParams.set("tabActive", currentTabName);
      navigate({
        pathname: `/detailPage/${guid}`,
        search: searchParams.toString()
      });
      setValue(newValue);
    }
  };
  useEffect(() => {
    if (!isEmpty(guid)) {
      dispatch(fetchDetailPageData(guid as string));
    }
  }, [guid]);

  useEffect(() => {
    setValue(
      !isEmpty(activeTab) ? tabsName.findIndex((val) => val === activeTab) : 0
    );
  }, [activeTab]);

  return (
    <Stack
      direction="column"
      justifyContent="center"
      alignItems="stretch"
      spacing={1}
      minHeight="100%"
    >
      <StyledPaper
        sx={{ display: "flex", flexDirection: "column", gap: "1rem" }}
        className="detail-page-paper"
        variant="outlined"
      >
        {loading ? (
          <Stack direction="row" spacing={2} alignItems="center">
            <SkeletonLoader
              count={1}
              variant="circular"
              animation="wave"
              width={50}
              height={50}
            />
            <SkeletonLoader
              count={1}
              variant="text"
              animation="wave"
              width={1000}
              className="text-loader"
            />
          </Stack>
        ) : (
          <Stack spacing={2} direction="row" alignItems="center">
            {!isEmpty(entity) && (
              <DisplayImage
                entity={entity}
                width={56}
                height={50}
                avatarDisplay={true}
                isProcess={isProcess}
              />
            )}

            <Typography
              noWrap
              fontWeight={600}
              fontSize={"24px"}
              data-id="title"
            >
              {!isEmpty(entity) ? `${name} (${entity.typeName})` : ""}
            </Typography>
            {entity?.status && entityStateReadOnly?.[entity?.status] && (
              <CustomButton
                variant="outlined"
                className="table-filter-btn assignTag cursor-pointer"
                size="medium"
                data-cy="deletedEntity"
                disabled
              >
                <Typography fontWeight={600}>Deleted</Typography>
              </CustomButton>
            )}
          </Stack>
        )}
        <Divider />
        {loading ? (
          <Stack direction="row" spacing={2} alignItems="center">
            <SkeletonLoader
              count={1}
              variant="text"
              width={200}
              className="text-loader"
            />
            <SkeletonLoader
              count={1}
              variant="text"
              width={1000}
              className="text-loader"
            />
          </Stack>
        ) : (
          <Stack direction="row" alignItems="flex-start">
            <Typography
              lineHeight="26px"
              flexBasis="12%"
              fontWeight="800"
              className="entity-attribute-label"
            >
              Classifications :
            </Typography>
            <Stack
              data-cy="tag-list"
              direction="row"
              flex="1"
              justifyContent="flex-start"
            >
              {!isEmpty(entity) && (
                <DialogShowMoreLess
                  value={getTagObj(entity, classifications)}
                  readOnly={
                    entity.status && entityStateReadOnly[entity.status]
                      ? true
                      : false
                  }
                  // setUpdateTable={setUpdateTable}
                  columnVal="self"
                  colName="Classification"
                  displayText="typeName"
                  removeApiMethod={removeClassification}
                  isShowMoreLess={false}
                  detailPage={true}
                  entity={entity}
                  relatedTerm={false}
                />
              )}
            </Stack>
          </Stack>
        )}
        <Divider />
        {loading ? (
          <Stack direction="row" spacing={2} alignItems="center">
            <SkeletonLoader
              count={2}
              width={1000}
              variant="text"
              className="text-loader"
            />
          </Stack>
        ) : (
          <Stack direction="row" alignItems="flex-start">
            <Typography
              lineHeight="26px"
              flexBasis="12%"
              fontWeight="800"
              className="entity-attribute-label"
            >
              Terms :
            </Typography>
            <Stack
              data-cy="termList"
              direction="row"
              flex="1"
              justifyContent="flex-start"
            >
              {!isEmpty(entity) && (
                <DialogShowMoreLess
                  value={entity?.relationshipAttributes}
                  readOnly={
                    entity.status && entityStateReadOnly[entity.status]
                      ? true
                      : false
                  }
                  // setUpdateTable={setUpdateTable}
                  columnVal="meanings"
                  colName="Term"
                  removeApiMethod={removeTerm}
                  isShowMoreLess={false}
                  detailPage={true}
                  displayText={"qualifiedName"}
                  entity={entity}
                  relatedTerm={false}
                />
              )}
            </Stack>
          </Stack>
        )}
        <Divider />
        {isEmpty(tagObj.propagatedMap) && loading ? (
          <Stack direction="row" spacing={2} alignItems="center">
            <SkeletonLoader
              count={1}
              variant="text"
              width={200}
              className="text-loader"
            />
            <SkeletonLoader
              count={1}
              variant="text"
              width={1000}
              className="text-loader"
            />
          </Stack>
        ) : (
          <Stack direction="row">
            <Typography
              flexBasis="12%"
              fontWeight="800"
              className="entity-attribute-label"
            >
              Propagated Classifications :
            </Typography>
            <Stack
              data-cy="propagatedTagList"
              direction="row"
              flex="1"
              justifyContent="flex-start"
            >
              {!isEmpty(entity) && !isEmpty(entity?.classifications) && (
                <DialogShowMoreLess
                  value={tagObj}
                  readOnly={true}
                  // setUpdateTable={setUpdateTable}
                  columnVal="propagated"
                  colName="Propagated Classification"
                  displayText="typeName"
                  isShowMoreLess={false}
                  detailPage={true}
                  entity={entity}
                  relatedTerm={false}
                />
              )}
            </Stack>
          </Stack>
        )}
      </StyledPaper>

      <Item variant="outlined">
        <Stack height="100%" width="100%">
          <Tabs
            value={value}
            onChange={handleChange}
            aria-label="nav tabs example"
            role="navigation"
            className="detail-page-tabs entity-detail-tabs"
            data-cy="tab-list"
          >
            <LinkTab label="Properties" />
            {isLineageRender && <LinkTab label="Lineage" />}
            <LinkTab label="Relationships" />
            <LinkTab label="Classifications" />
            <LinkTab label="Audits" />
            {!isEmpty(schemaElementsAttribute) && <LinkTab label="Schema" />}
            {!isEmpty(entity) && entity.typeName == "AtlasServer" && (
              <LinkTab label="Export/Import Audits" />
            )}
            {!isEmpty(entity) &&
              (!isEmpty(entity?.attributes?.["profileData"]) ||
                entity.typeName == "hive_db" ||
                entity.typeName == "hbase_namespace") && (
                <LinkTab label="Table" />
              )}
            {taskTabEnabled && uiTaskTabEnabled && <LinkTab label="Tasks" />}
          </Tabs>
          {(activeTab == undefined || activeTab === "properties") && (
            <PropertiesTab
              entity={entity}
              referredEntities={referredEntities}
              loading={loading}
            />
          )}
          {activeTab === "lineage" && <LineageTab entity={entity} />}
          {activeTab === "relationship" && (
            <RelationshipsTab
              entity={entity}
              referredEntities={referredEntities}
              loading={loading}
            />
          )}
          {activeTab === "classification" && !isEmpty(entity) && (
            <ClassificationsTab
              entity={entity}
              loading={loading}
              tags={tagObj}
            />
          )}
          {activeTab === "audit" && !isEmpty(entity) && (
            <AuditsTab
              entity={entity}
              referredEntities={referredEntities}
              loading={loading}
            />
          )}
          {activeTab === "schema" && !isEmpty(entity) && (
            <SchemaTab
              entity={entity}
              referredEntities={referredEntities}
              loading={loading}
              schemaElementsAttribute={schemaElementsAttribute}
            />
          )}

          {activeTab === "raudits" && !isEmpty(entity) && (
            <ReplicationAuditTable
              entity={entity}
              referredEntities={referredEntities}
              loading={loading}
            />
          )}
          {activeTab === "profile" && !isEmpty(entity) && (
            <ProfileTab entity={entity} />
          )}
          {activeTab === "pendingTask" && !isEmpty(entity) && <TaskTab />}
        </Stack>
      </Item>
    </Stack>
  );
};

export default EntityDetailPage;
