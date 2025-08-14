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

import { globalSessionData } from "./Enum";

const entityImgPath: string = "/img/entity-icon/";
const dateTimeFormat = "MM/DD/YYYY hh:mm:ss A";
const dateFormat = "MM/DD/YYYY";

const globalSession = (sessionData: any) => {
  globalSessionData.restCrsfHeader =
    sessionData["atlas.rest-csrf.custom-header"] || "";
  globalSessionData.crsfToken = sessionData["_csrfToken"];
  globalSessionData.debugMetrics = sessionData["atlas.debug.metrics.enabled"];
  globalSessionData.entityCreate =
    sessionData["atlas.entity.create.allowed"] || true;
  globalSessionData.entityUpdate =
    sessionData["atlas.entity.update.allowed"] || true;
  globalSessionData.taskTabEnabled =
    sessionData["atlas.tasks.enabled"] || false;
  globalSessionData.sessionTimeout =
    sessionData["atlas.session.timeout.secs"] || 900;
  globalSessionData.uiTaskTabEnabled =
    sessionData["atlas.tasks.ui.tab.enabled"];
  globalSessionData.relationshipSearch =
    sessionData["atlas.relationship.search.enabled"] || false;
  globalSessionData.isLineageOnDemandEnabled =
    sessionData["atlas.lineage.on.demand.enabled"] || false;
  globalSessionData.lineageNodeCount =
    sessionData["atlas.lineage.on.demand.default.node.count"] || 3;
  globalSessionData.isTimezoneFormatEnabled =
    sessionData["atlas.ui.date.timezone.format.enabled"] || true;
};

export { globalSession, entityImgPath, dateTimeFormat, dateFormat };
