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

import { useMemo } from 'react';

interface UseVirtualizationProps<T> {
  items: T[];
  scrollTop: number;
  itemHeight?: number;
  overscan?: number;
  visibleCount?: number;
}

export const useVirtualization = <T>({
  items,
  scrollTop,
  itemHeight = 37,
  overscan = 10,
  visibleCount = 40,
}: UseVirtualizationProps<T>) => {
  return useMemo(() => {
    const totalItems = items.length;

    if (totalItems === 0) {
      return {
        visibleItems: [],
        paddingTop: 0,
        paddingBottom: 0,
        startIndex: 0,
      };
    }

    const startIndex = Math.max(0, Math.floor(scrollTop / itemHeight) - overscan);
    const endIndex = Math.min(totalItems - 1, Math.floor(scrollTop / itemHeight) + visibleCount + overscan);

    const visibleItems = items.slice(startIndex, endIndex + 1);

    const paddingTop = startIndex * itemHeight;
    const paddingBottom = Math.max(0, (totalItems - 1 - endIndex) * itemHeight);

    return {
      visibleItems,
      paddingTop,
      paddingBottom,
      startIndex,
    };
  }, [items, scrollTop, itemHeight, overscan, visibleCount]);
};
