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

import type { ComponentType } from "react";
import type { Props } from "@models/treeStructureType.ts";

export type SidebarTreeModuleId =
  | "entities"
  | "classification"
  | "glossary"
  | "businessMetadata"
  | "relationships"
  | "customFilters";

export interface SidebarTreeModuleConfig {
  id: SidebarTreeModuleId;
  dataCy: string;
  Tree: ComponentType<Props>;
  /** When false, tree is omitted (e.g. relationships gated by session flag). */
  visible: boolean;
}

export const buildSidebarTreeModules = (
  trees: Record<SidebarTreeModuleId, ComponentType<Props>>,
  relationshipSearchEnabled: boolean
): SidebarTreeModuleConfig[] => [
  {
    id: "entities",
    dataCy: "r_entityTreeRender",
    Tree: trees.entities,
    visible: true,
  },
  {
    id: "classification",
    dataCy: "r_classificationTreeRender",
    Tree: trees.classification,
    visible: true,
  },
  {
    id: "glossary",
    dataCy: "r_glossaryTreeRender",
    Tree: trees.glossary,
    visible: true,
  },
  {
    id: "businessMetadata",
    dataCy: "r_businessMetadataTreeRender",
    Tree: trees.businessMetadata,
    visible: true,
  },
  {
    id: "relationships",
    dataCy: "r_relationshipTreeRender",
    Tree: trees.relationships,
    visible: relationshipSearchEnabled,
  },
  {
    id: "customFilters",
    dataCy: "r_customFilterTreeRender",
    Tree: trees.customFilters,
    visible: true,
  },
];
