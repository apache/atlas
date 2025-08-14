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

import { Stack, Tabs } from "@mui/material";
import DetailPageAttribute from "../DetailPageAttributes";
import SearchResult from "@views/SearchResult/SearchResult";
import { paramsType } from "@models/detailPageType";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import { useEffect, useState } from "react";
import { getTagObj, isEmpty } from "@utils/Utils";
import { Item, samePageLinkNavigation } from "@utils/Muiutils";
import { LinkTab } from "@components/muiComponents";
import TermProperties from "./TermProperties";
import ClassificationsTab from "../EntityDetailTabs/ClassificationsTab";
import TermRelation from "./TermRelation";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { fetchGlossaryDetails } from "@redux/slice/glossaryDetailsSlice";

let allTabs = [
  "entities",
  "entitiesProperties",
  "classification",
  "relatedTerm"
];

const GlossaryDetailLayout = () => {
  const { guid } = useParams<paramsType>();
  const location = useLocation();
  const navigate = useNavigate();
  const dispatchApi = useAppDispatch();
  const searchParams = new URLSearchParams(location.search);
  const gtype: string | undefined | null = searchParams.get("gtype");
  const activeTab: string | undefined | null = searchParams.get("tabActive");
  const { glossaryTypeData }: any = useAppSelector(
    (state: any) => state.glossaryType
  );

  const { data, loading } = glossaryTypeData || {};
  const [value, setValue] = useState(
    !isEmpty(activeTab) ? allTabs.findIndex((val) => val === activeTab) : 0
  );

  useEffect(() => {
    let params: any = { gtype: gtype, guid: guid };
    dispatchApi(fetchGlossaryDetails(params));
  }, [guid]);

  useEffect(() => {
    const tabIndex = allTabs.findIndex((val) => val === activeTab);
    setValue(!isEmpty(activeTab) ? (tabIndex == -1 ? 0 : tabIndex) : 0);
    if (tabIndex == -1) {
      searchParams.set("tabActive", "entities");
      navigate({
        pathname: `/glossary/${guid}`,
        search: searchParams.toString()
      });
    }
  }, [activeTab]);

  const {
    classifications = {},
    categories = {},
    shortDescription = {},
    longDescription = {},
    additionalAttributes = {},
    qualifiedName
  }: any = data || {};

  const handleChange = (event: React.SyntheticEvent, newValue: number) => {
    if (
      event.type !== "click" ||
      (event.type === "click" &&
        samePageLinkNavigation(
          event as React.MouseEvent<HTMLAnchorElement, MouseEvent>
        ))
    ) {
      let currentTabName = allTabs[newValue];
      let keys = Array.from(searchParams.keys());
      for (let i = 0; i < keys.length; i++) {
        if (keys[i] != "searchType") {
          searchParams.delete(keys[i]);
        }
      }
      searchParams.set("gtype", "term");
      searchParams.set("viewType", "term");
      searchParams.set("fromView", "entity");
      searchParams.set("tabActive", currentTabName);
      navigate({
        pathname: `/glossary/${guid}`,
        search: searchParams.toString()
      });
      setValue(newValue);
    }
  };

  let tagObj = getTagObj(data, classifications);
  return (
    <Stack direction="column">
      <>
        <DetailPageAttribute
          paramsAttribute={guid}
          data={data}
          classifications={classifications}
          categories={categories}
          shortDescription={shortDescription}
          description={longDescription}
          loading={loading}
        />

        {gtype == "term" && (
          <Item variant="outlined" className="glossary-detail-items">
            <Stack width="100%">
              <Tabs
                value={value == -1 ? 0 : value}
                onChange={handleChange}
                aria-label="nav tabs example"
                role="navigation"
                className="detail-page-tabs"
                data-cy="tabList"
              >
                <LinkTab label="Entities" />
                <LinkTab label="Properties" />
                <LinkTab label="Classifications" />
                <LinkTab label="Related Terms" />
              </Tabs>

              <>
                {(activeTab == undefined || activeTab === "entities") &&
                  !isEmpty(data) && (
                    <div style={{ padding: "16px" }}>
                      <SearchResult glossaryTypeParams={qualifiedName} />
                    </div>
                  )}
                {activeTab === "entitiesProperties" && (
                  <TermProperties
                    additionalAttributes={additionalAttributes}
                    loader={loading}
                  />
                )}
                {activeTab === "classification" && !isEmpty(data) && (
                  <ClassificationsTab
                    entity={data}
                    loading={loading}
                    tags={tagObj}
                  />
                )}
                {activeTab === "relatedTerm" && !isEmpty(data) && (
                  <TermRelation
                    glossaryTypeData={data}
                    loading={loading}
                    fetchGlossaryDetails={fetchGlossaryDetails}
                  />
                )}
              </>
            </Stack>
          </Item>
        )}
      </>
    </Stack>
  );
};

export default GlossaryDetailLayout;
