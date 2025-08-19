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

import { useEffect, useState } from "react";
import { Divider, IconButton, Stack, Tabs, Typography } from "@mui/material";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import DisplayImage from "@components/EntityDisplayImage";
import {
  extractKeyValueFromEntity,
  getNestedSuperTypes,
  getTagObj,
  isEmpty
} from "@utils/Utils";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { entityStateReadOnly, globalSessionData } from "@utils/Enum";
import { removeClassification } from "@api/apiMethods/classificationApiMethod";
import PropertiesTab from "./EntityDetailTabs/PropertiesTab/PropertiesTab";
import SkeletonLoader from "@components/SkeletonLoader";
import RelationshipsTab from "./EntityDetailTabs/RelationshipsTab";
import ClassificationsTab from "./EntityDetailTabs/ClassificationsTab";
import { CustomButton, LightTooltip, LinkTab } from "@components/muiComponents";
import AuditsTab from "./EntityDetailTabs/AuditsTab";
import { EntityState } from "@models/relationshipSearchType";
import { useSelector } from "react-redux";
import SchemaTab from "./EntityDetailTabs/SchemaTab";
import ReplicationAuditTable from "./EntityDetailTabs/ReplicationAuditTab";
import ProfileTab from "./EntityDetailTabs/ProfileTab";
import TaskTab from "./EntityDetailTabs/TaskTab";
import { Item, samePageLinkNavigation, StyledPaper } from "@utils/Muiutils";
import { cloneDeep } from "@utils/Helper";
import { fetchDetailPageData } from "@redux/slice/detailPageSlice";
import React from "react";
import AddTag from "@views/Classification/AddTag";
import AssignTerm from "@views/Glossary/AssignTerm";
import { removeTerm } from "@api/apiMethods/glossaryApiMethod";
import AddCircleOutlineIcon from "@mui/icons-material/AddCircleOutline";
import ShowMoreView from "@components/ShowMore/ShowMoreView";
import LineageTab from "./EntityDetailTabs/LineageTab";

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

  const [openAddTagModal, setOpenAddTagModal] = useState<boolean>(false);
  const [openAddTermModal, setOpenAddTermModal] = useState<boolean>(false);

  const handleCloseAddTagModal = () => {
    setOpenAddTagModal(false);
  };

  const handleCloseAddTermModal = () => {
    setOpenAddTermModal(false);
  };

  const { entityDefs = {} } = entityData || {};
  let entityDefObj = cloneDeep(entityDefs) || {};
  const { entity, referredEntities }: any = detailPageData || {};
  const { classifications = {}, relationshipAttributes = {} } = entity || {};
  const { meanings = [] } = relationshipAttributes || {};

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
    const tabIndex = tabsName.findIndex((tab) => tab === activeTab);
    setValue(tabIndex !== -1 ? tabIndex : 0);
  }, [activeTab, tabsName]);

  const propagatedTagList = !isEmpty(tagObj?.["propagatedMap"])
    ? Object.values(tagObj?.["propagatedMap"] || {})
    : [];

  const tabComponents: Record<string, React.ReactNode> = {
    properties: (
      <PropertiesTab
        entity={entity}
        referredEntities={referredEntities}
        loading={loading}
      />
    ),
    lineage: <LineageTab entity={entity} isProcess={isProcess} />,
    relationship: (
      <RelationshipsTab
        entity={entity}
        referredEntities={referredEntities}
        loading={loading}
      />
    ),
    classification: (
      <ClassificationsTab entity={entity} loading={loading} tags={tagObj} />
    ),
    audit: (
      <AuditsTab
        entity={entity}
        referredEntities={referredEntities}
        loading={loading}
      />
    ),
    schema: (
      <SchemaTab
        entity={entity}
        referredEntities={referredEntities}
        loading={loading}
        schemaElementsAttribute={schemaElementsAttribute}
      />
    ),
    raudits: (
      <ReplicationAuditTable
        entity={entity}
        referredEntities={referredEntities}
        loading={loading}
      />
    ),
    profile: <ProfileTab entity={entity} />,
    pendingTask: <TaskTab />
  };

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
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gridGap: "1rem 2rem",
            marginBottom: "0.75rem"
          }}
        >
          <Stack>
            {loading ? (
              <Stack direction="column" spacing={2} alignItems="left">
                <SkeletonLoader
                  count={1}
                  variant="text"
                  width={300}
                  className="text-loader"
                />
              </Stack>
            ) : (
              <Stack direction="column" alignItems="flex-start" gap="0.5rem">
                <Stack direction="row" alignItems="center" gap="0.75rem">
                  <Typography
                    lineHeight="26px"
                    flexBasis="12%"
                    fontWeight="600"
                    fontSize="16px"
                    className="entity-attribute-label"
                  >
                    Classifications
                  </Typography>
                  <LightTooltip title={"Add Classifications"}>
                    <IconButton
                      component="label"
                      role={undefined}
                      tabIndex={-1}
                      size="small"
                      color="primary"
                      onClick={() => {
                        setOpenAddTagModal(true);
                      }}
                    >
                      <AddCircleOutlineIcon className="mr-0" fontSize="small" />{" "}
                    </IconButton>
                  </LightTooltip>
                </Stack>

                <Stack
                  data-cy="tag-list"
                  direction="row"
                  flex="1"
                  justifyContent="flex-start"
                >
                  <ShowMoreView
                    id="classifications"
                    data={getTagObj(entity, classifications)["self"]}
                    maxVisible={4}
                    title="Classifications"
                    displayKey="typeName"
                    removeApiMethod={removeClassification}
                    currentEntity={entity}
                    removeTagsTitle={"Remove Classification Assignment"}
                    isEditView={false}
                  />
                </Stack>
              </Stack>
            )}
          </Stack>

          {!entity?.typeName?.includes("AtlasGlossary") && (
            <Stack>
              {loading ? (
                <Stack direction="column" spacing={2} alignItems="flex-start">
                  <SkeletonLoader
                    count={1}
                    width={300}
                    variant="text"
                    className="text-loader"
                  />
                </Stack>
              ) : (
                <Stack direction="column" alignItems="flex-start" gap="0.5rem">
                  <Stack direction="row" alignItems="center" gap="0.75rem">
                    <Typography
                      lineHeight="26px"
                      flexBasis="12%"
                      fontSize="16px"
                      fontWeight="600"
                      className="entity-attribute-label"
                    >
                      Terms
                    </Typography>
                    <LightTooltip title="Add Term">
                      <IconButton
                        component="label"
                        role={undefined}
                        tabIndex={-1}
                        size="small"
                        color="primary"
                        onClick={() => setOpenAddTermModal(true)}
                      >
                        <AddCircleOutlineIcon
                          className="mr-0"
                          fontSize="small"
                        />
                      </IconButton>
                    </LightTooltip>
                  </Stack>

                  <Stack
                    data-cy="termList"
                    direction="row"
                    flex="1"
                    justifyContent="flex-start"
                  >
                    <ShowMoreView
                      id="terms"
                      data={meanings}
                      maxVisible={4}
                      title="Terms"
                      displayKey="qualifiedName"
                      removeApiMethod={removeTerm}
                      currentEntity={entity}
                      removeTagsTitle="Remove Term Assignment"
                      isEditView={false}
                    />
                  </Stack>
                </Stack>
              )}
            </Stack>
          )}

          <Stack>
            {!isEmpty(tagObj.propagatedMap) &&
              (loading ? (
                <Stack direction="column" spacing={2} alignItems="left">
                  <SkeletonLoader
                    count={1}
                    variant="text"
                    width={300}
                    className="text-loader"
                  />
                </Stack>
              ) : (
                <Stack direction="column">
                  <Typography
                    flexBasis="12%"
                    fontSize="16px"
                    fontWeight="600"
                    className="entity-attribute-label"
                    marginBottom="0.5rem"
                  >
                    Propagated Classifications
                  </Typography>
                  <Stack
                    data-cy="propagatedTagList"
                    direction="row"
                    flex="1"
                    justifyContent="flex-start"
                  >
                    {!isEmpty(entity) && !isEmpty(entity?.classifications) && (
                      <ShowMoreView
                        id="propagatedClassifications"
                        data={propagatedTagList}
                        maxVisible={4}
                        title="Propagated Classifications"
                        displayKey="typeName"
                        currentEntity={entity}
                        isEditView={false}
                        isDeleteIcon={true}
                      />
                    )}
                  </Stack>
                </Stack>
              ))}
          </Stack>
        </div>
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
            <LinkTab label="Properties" />-{" "}
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

          {tabComponents[activeTab || "properties"]}
        </Stack>
      </Item>
      {openAddTagModal && (
        <AddTag
          open={openAddTagModal}
          isAdd={true}
          entityData={entity}
          onClose={handleCloseAddTagModal}
          setUpdateTable={undefined}
          setRowSelection={undefined}
        />
      )}
      {openAddTermModal && (
        <AssignTerm
          updateTable={undefined}
          open={openAddTermModal}
          onClose={handleCloseAddTermModal}
          data={relationshipAttributes}
          relatedTerm={false}
          columnVal={"meanings"}
        />
      )}
    </Stack>
  );
};

export default EntityDetailPage;
