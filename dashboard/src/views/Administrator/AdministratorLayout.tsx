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

import { LinkTab } from "@components/muiComponents";
import { Stack, Tabs } from "@mui/material";
import { Item, samePageLinkNavigation } from "@utils/Muiutils";
import { isEmpty } from "@utils/Utils";
import { useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import BusinessMetadataTab from "./BusinessMetadataTab";
import Enumerations from "./Enumerations";
import AdminAuditTable from "./Audits/AdminAuditTable";
import BusinessMetaDataForm from "@views/BusinessMetadata/BusinessMetadataForm";
import { useAppSelector } from "@hooks/reducerHook";
import TypeSystemTreeView from "./TypeSystemTreeView";

const allTabs = ["businessMetadata", "enum", "audit", "typeSystem"];

const AdministratorLayout = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const searchParams = new URLSearchParams(location.search);
  const { entityData = {} }: any = useAppSelector((state: any) => state.entity);
  const { entityDefs = [] } = entityData || {};

  const [form, setForm] = useState(false);
  const [bmAttribute, setBMAttribute] = useState({});
  const activeTab: string | undefined | null = searchParams.get("tabActive");
  const [value, setValue] = useState(
    !isEmpty(activeTab) ? allTabs.findIndex((val) => val === activeTab) : 0
  );

  const handleChange = (event: React.SyntheticEvent, newValue: number) => {
    if (
      event.type !== "click" ||
      (event.type === "click" &&
        samePageLinkNavigation(
          event as React.MouseEvent<HTMLAnchorElement, MouseEvent>
        ))
    ) {
      setValue(newValue);
      let currentTabName = allTabs[newValue];
      const searchParams = new URLSearchParams();
      searchParams.set("tabActive", currentTabName);
      navigate({
        pathname: `/administrator`,
        search: searchParams.toString(),
      });
    }
  };
  return (
    <Item
      variant="outlined"
      sx={{ background: "white" }}
      className="administration-items"
    >
      {form ? (
        <Stack width="100%">
          <BusinessMetaDataForm
            setForm={setForm}
            setBMAttribute={setBMAttribute}
            bmAttribute={bmAttribute}
          />
        </Stack>
      ) : (
        <Stack width="100%">
          <Tabs
            value={value}
            onChange={handleChange}
            role="navigation"
            className="detail-page-tabs administration-tabs"
            data-cy="tab-list"
          >
            <LinkTab label="Business Metadata" />
            <LinkTab label="Enumerations" />
            <LinkTab label="Audits" />
            <LinkTab label="Type System" />
          </Tabs>
          {(activeTab == undefined || activeTab === "businessMetadata") && (
            <BusinessMetadataTab
              setForm={setForm}
              setBMAttribute={setBMAttribute}
            />
          )}
          {activeTab === "enum" && <Enumerations />}
          {activeTab === "audit" && <AdminAuditTable />}
          {activeTab === "typeSystem" && !isEmpty(entityDefs) && (
            <TypeSystemTreeView entityDefs={entityDefs} />
          )}
        </Stack>
      )}
    </Item>
  );
};

export default AdministratorLayout;
