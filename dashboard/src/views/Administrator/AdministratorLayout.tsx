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
import SkeletonLoader from "@components/SkeletonLoader";
import { Stack, Tabs } from "@mui/material";
import { Item, samePageLinkNavigation } from "@utils/Muiutils";
import { isEmpty } from "@utils/Utils";
import { lazy, Suspense, useState, useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { useAppSelector } from "@hooks/reducerHook";

const BusinessMetadataTab = lazy(() => import("./BusinessMetadataTab"));
const Enumerations = lazy(() => import("./Enumerations"));
const AdminAuditTable = lazy(() => import("./Audits/AdminAuditTable"));
const TypeSystemTreeView = lazy(() => import("./TypeSystemTreeView"));
const BusinessMetaDataForm = lazy(
  () => import("@views/BusinessMetadata/BusinessMetadataForm")
);

const tabFallback = (
  <Stack direction="column" spacing={2} sx={{ p: 2 }}>
    <SkeletonLoader count={4} variant="text" className="text-loader" />
  </Stack>
);

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

  useEffect(() => {
    const tabIndex = !isEmpty(activeTab)
      ? allTabs.findIndex((val) => val === activeTab)
      : 0;
    const resolvedIndex = tabIndex >= 0 ? tabIndex : 0;
    setValue(resolvedIndex);

    if (activeTab && activeTab !== "businessMetadata") {
      setForm(false);
    }
    const createParam = searchParams.get("create");
    if (createParam === "true" && activeTab === "businessMetadata") {
      setForm(true);
      setBMAttribute({});
      const newParams = new URLSearchParams(location.search);
      newParams.delete("create");
      navigate({ pathname: "/administrator", search: newParams.toString() }, { replace: true });
    }
  }, [activeTab, location.search, navigate]);

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
          <Suspense fallback={tabFallback}>
            <BusinessMetaDataForm
              setForm={setForm}
              setBMAttribute={setBMAttribute}
              bmAttribute={bmAttribute}
            />
          </Suspense>
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
          <Suspense fallback={tabFallback}>
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
          </Suspense>
        </Stack>
      )}
    </Item>
  );
};

export default AdministratorLayout;
