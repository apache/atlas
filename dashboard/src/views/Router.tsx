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

import { Routes, Route, HashRouter } from "react-router-dom";
import { lazy } from "react";
import DebugMetrics from "./Layout/DebugMetrics";
// import ErrorBoundary from "./ErrorBoundary";

const Layout = lazy(() => import("./Layout/Layout"));
const SearchResult = lazy(() => import("./SearchResult/SearchResult"));
const RelationShipSearch = lazy(
  () => import("./SearchResult/RelationShipSearch")
);
const DashBoard = lazy(() => import("./DashBoard"));
const EntityDetailPage = lazy(() => import("./DetailPage/EntityDetailPage"));
const ClassificationDetailsLayout = lazy(
  () => import("./DetailPage/ClassificationDetailsLayout")
);
const GlossaryDetailLayout = lazy(
  () => import("./DetailPage/GlossaryDetails/GlossaryDetailsLayout")
);
const AdministratiorLayout = lazy(
  () => import("./Administrator/AdministratorLayout")
);

const BusinessMetadataDetailsLayout = lazy(
  () =>
    import("./DetailPage/BusinessMetadataDetails/BusinessMetadataDetailsLayout")
);

const RelationshipDetailsLayout = lazy(
  () => import("./DetailPage/RelationshipDetails/RelationshipDetailsLayout")
);

const Router = () => {
  return (
    // <ErrorBoundary>
    <HashRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route path="/search" element={<DashBoard />} />
          <Route path="/search">
            <Route path="searchResult" element={<SearchResult />} />
          </Route>
          <Route path="/relationship">
            <Route
              path="relationshipSearchResult"
              element={<RelationShipSearch />}
            />
          </Route>
          <Route path="/detailPage">
            <Route path=":guid" element={<EntityDetailPage />} />
          </Route>
          <Route path="/tag/tagAttribute">
            <Route path=":tagName" element={<ClassificationDetailsLayout />} />
          </Route>
          <Route path="/administrator/" element={<AdministratiorLayout />} />
          <Route path="/administrator/businessMetadata">
            <Route path=":bmguid" element={<BusinessMetadataDetailsLayout />} />
          </Route>
          <Route path="/glossary">
            <Route path=":guid" element={<GlossaryDetailLayout />} />
          </Route>
          <Route path="/relationshipDetailPage">
            <Route path=":guid" element={<RelationshipDetailsLayout />} />
          </Route>
          <Route path="/debugMetrics" element={<DebugMetrics />} />

          <Route path="*" element={<DashBoard />} />
        </Route>
      </Routes>
    </HashRouter>
    // </ErrorBoundary>
  );
};
export default Router;
