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

import { combineReducers } from "@reduxjs/toolkit";
import { sessionReducer } from "@redux/slice/sessionSlice";
import { enumReducer } from "@redux/slice/enumSlice";
import createBMReducer from "@redux/slice/createBMSlice";
import { glossaryTypeReducer } from "@redux/slice/glossaryDetailsSlice";
import { rootClassificationTypeReducer } from "@redux/slice/rootClassificationSlice";
import { businessMetadataReducer } from "@redux/slice/typeDefSlices/typedefBusinessMetadataSlice";
import { classificationReducer } from "@redux/slice/typeDefSlices/typedefClassificationSlice";
import { entityReducer } from "@redux/slice/typeDefSlices/typedefEntitySlice";
import { typeHeaderReducer } from "@redux/slice/typeDefSlices/typeDefHeaderSlice";
import { allEntityTypesReducer } from "@redux/slice/allEntityTypesSlice";
import { detailPageReducer } from "@redux/slice/detailPageSlice";
import { glossaryReducer } from "@redux/slice/glossarySlice";
import { relationshipsReducer } from "@redux/slice/typeDefSlices/typedefRelationshipsSlice";
import { metricsReducer } from "@redux/slice/metricsSlice";
import { savedSearchReducer } from "@redux/slice/savedSearchSlice";
import { drawerSliceReducer } from "@redux/slice/drawerSlice";

const rootReducer = combineReducers({
  session: sessionReducer,
  entity: entityReducer,
  typeHeader: typeHeaderReducer,
  allEntityTypes: allEntityTypesReducer,
  metrics: metricsReducer,
  classification: classificationReducer,
  businessMetaData: businessMetadataReducer,
  glossary: glossaryReducer,
  relationships: relationshipsReducer,
  savedSearch: savedSearchReducer,
  detailPage: detailPageReducer,
  enum: enumReducer,
  createBM: createBMReducer,
  glossaryType: glossaryTypeReducer,
  rootClassification: rootClassificationTypeReducer,
  drawerState: drawerSliceReducer
});

export default rootReducer;
