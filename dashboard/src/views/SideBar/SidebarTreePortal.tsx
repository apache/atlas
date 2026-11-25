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

import { Suspense, ComponentType } from "react";
import { createPortal } from "react-dom";
import TreeSkeletonLoader from "@components/TreeSkeletonLoader";
import type { Props } from "@models/treeStructureType.ts";

export interface SidebarTreePortalProps {
  moduleId: string;
  dataCy: string;
  Tree: ComponentType<Props>;
  searchTerm: string;
  drawerOpen: boolean;
  activePopover: string | null;
  popoverContainer: HTMLDivElement | null;
  sidebarHost: HTMLDivElement | null;
  setSidebarRef: (id: string) => (el: HTMLDivElement | null) => void;
}

export const SidebarTreePortal = ({
  moduleId,
  dataCy,
  Tree,
  searchTerm,
  drawerOpen,
  activePopover,
  popoverContainer,
  sidebarHost,
  setSidebarRef,
}: SidebarTreePortalProps) => {
  const isPopover = !drawerOpen && activePopover === moduleId;
  const sideBarOpen = isPopover ? true : drawerOpen;
  const portalTarget =
    isPopover && popoverContainer ? popoverContainer : sidebarHost;

  return (
    <div
      className="sidebar-treeview-container"
      data-cy={dataCy}
      ref={setSidebarRef(moduleId)}
    >
      <Suspense fallback={<TreeSkeletonLoader count={2} />}>
        {sidebarHost &&
          portalTarget &&
          createPortal(
            <Tree
              sideBarOpen={sideBarOpen}
              searchTerm={searchTerm}
              isPopover={isPopover}
            />,
            portalTarget
          )}
      </Suspense>
    </div>
  );
};

export default SidebarTreePortal;
