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

import { Stack } from "@mui/material";
import SkeletonLoader from "./SkeletonLoader";

const TreeSkeletonLoader = ({ count = 7 }: { count?: number }) => {
  const treeItemSkeleton = (indentLevel: number, textWidth: string, key: number) => (
    <Stack key={key} direction="row" alignItems="center" gap="12px" paddingLeft={`${indentLevel * 24}px`}>
      <SkeletonLoader animation="pulse" variant="rectangular" width={16} height={16} sx={{ borderRadius: "4px", backgroundColor: "rgba(255,255,255,0.08)" }} count={1} />
      <SkeletonLoader animation="pulse" variant="rectangular" width={textWidth} height={20} sx={{ borderRadius: "4px", backgroundColor: "rgba(255,255,255,0.08)" }} count={1} />
    </Stack>
  );

  const allRows = [
    treeItemSkeleton(0, "80%", 0),
    treeItemSkeleton(1, "65%", 1),
    treeItemSkeleton(1, "75%", 2),
    treeItemSkeleton(2, "50%", 3),
    treeItemSkeleton(2, "60%", 4),
    treeItemSkeleton(0, "85%", 5),
    treeItemSkeleton(1, "70%", 6)
  ];

  return (
    <Stack gap="16px" padding="16px 16px" width="100%">
      {allRows.slice(0, count)}
    </Stack>
  );
};

export default TreeSkeletonLoader;
