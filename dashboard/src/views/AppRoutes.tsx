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

import DashBoard from "./DashBoard";
import ClassificationDetailsLayout from "./DetailPage/ClassificationDetailsLayout";
import EntityDetailPage from "./DetailPage/EntityDetailPage";
import AdministratorLayout from "./Administrator/AdministratorLayout"; // Correct import
import BusinessMetadataDetailsLayout from "./DetailPage/BusinessMetadataDetails/BusinessMetadataDetailsLayout";
import GlossaryDetailLayout from "./DetailPage/GlossaryDetails/GlossaryDetailsLayout";
import RelationshipDetailsLayout from "./DetailPage/RelationshipDetails/RelationshipDetailsLayout";
import ErrorPage from "./ErrorPage";
import DebugMetrics from "./Layout/DebugMetrics";
import SearchResult from "./SearchResult/SearchResult";
import RelationShipSearch from "./SearchResult/RelationShipSearch";

const AppRoutes = [
  
      { path: "/", element: <DashBoard /> },
      { path: "/search", element: <DashBoard /> },
      {
        path: "/search/searchResult",
        element: <SearchResult />
      },
      {
        path: "/relationship/relationshipSearchResult",
        element: <RelationShipSearch />
      },
      { path: "/detailPage/:guid", element: <EntityDetailPage /> },

      {
        path: "/tag/tagAttribute/:tagName",
        element: <ClassificationDetailsLayout />
      },
      { path: "/administrator", element: <AdministratorLayout /> },
      {
        path: "/administrator/businessMetadata/:bmguid",
        element: <BusinessMetadataDetailsLayout />
      },
      { path: "/glossary/:guid", element: <GlossaryDetailLayout /> },
      {
        path: "/relationshipDetailPage/:guid",
        element: <RelationshipDetailsLayout />
      },
      { path: "/debugMetrics", element: <DebugMetrics /> },
      { path: "/dataNotFound", element: <ErrorPage errorCode="400" /> },
      { path: "/pageNotFound", element: <ErrorPage errorCode="404" /> },
      { path: "/forbidden", element: <ErrorPage errorCode="403" /> },
      { path: "*", element: <DashBoard /> }
  

];

export default AppRoutes;
