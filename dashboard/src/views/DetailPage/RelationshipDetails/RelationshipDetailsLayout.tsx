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

import { useEffect, useRef, useState } from "react";
import { Stack, Tabs, Typography } from "@mui/material";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import DisplayImage from "@components/EntityDisplayImage";
import { isEmpty, serverError } from "@utils/Utils";
import { entityStateReadOnly } from "@utils/Enum";
import SkeletonLoader from "@components/SkeletonLoader";
import { CustomButton, LinkTab } from "@components/muiComponents";
import { Item, samePageLinkNavigation } from "@utils/Muiutils";
import { getDetailPageRelationship } from "@api/apiMethods/detailpageApiMethod";
import RelationshipPropertiesTab from "./RelationshipPropertiesTab";

const EntityDetailPage: React.FC = () => {
  const { guid }: any = useParams();
  const location = useLocation();
  const toastId: any = useRef(null);
  const searchParams = new URLSearchParams(location.search);
  const navigate = useNavigate();
  const activeTab: string | undefined | null = searchParams.get("tabActive");
  const [loading, setLoading] = useState(false);
  const [relationshipData, setRelationshipData] = useState<any>({});

  let isProcess: boolean = false;

  let allTabs = ["properties"];
  let tabsName = [...allTabs];

  const [value, setValue] = useState(
    !isEmpty(activeTab) ? tabsName.findIndex((val) => val === activeTab) : 0
  );

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

  const fetchRelationShipDetails = async (guid: string) => {
    try {
      const relationshipResp = await getDetailPageRelationship(guid);
      const { relationship } = relationshipResp?.data;
      setRelationshipData(relationship);
    } catch (error) {
      setLoading(false);
      console.error(`Error occur while fetching relationship data`, error);
      serverError(error, toastId);
    }
  };

  useEffect(() => {
    fetchRelationShipDetails(guid);
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
      padding="16px"
      className="relationship-properties-accordion"
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
          {!isEmpty(relationshipData) && (
            <DisplayImage
              entity={relationshipData}
              width={56}
              height={50}
              avatarDisplay={true}
              isProcess={isProcess}
            />
          )}

          <Typography noWrap fontWeight={550} fontSize={"32px"} data-id="title">
            {!isEmpty(relationshipData)
              ? `${guid} (${relationshipData.typeName})`
              : ""}
          </Typography>
          {relationshipData?.status &&
            entityStateReadOnly?.[relationshipData?.status] && (
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
          </Tabs>
          {(activeTab == undefined || activeTab === "properties") && (
            <RelationshipPropertiesTab
              entity={relationshipData}
              loading={loading}
            />
          )}
        </Stack>
      </Item>
    </Stack>
  );
};

export default EntityDetailPage;
