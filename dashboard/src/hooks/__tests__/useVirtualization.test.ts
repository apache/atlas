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

import { renderHook } from '@testing-library/react';
import { useVirtualization } from '../useVirtualization';

describe('useVirtualization', () => {
  it('should return empty values when items array is empty', () => {
    const { result } = renderHook(() =>
      useVirtualization({ items: [], scrollTop: 0 })
    );

    expect(result.current).toEqual({
      visibleItems: [],
      paddingTop: 0,
      paddingBottom: 0,
      startIndex: 0,
    });
  });

  it('should calculate visible items and padding correctly for initial state', () => {
    const items = Array.from({ length: 100 }, (_, i) => i);
    const { result } = renderHook(() =>
      useVirtualization({ items, scrollTop: 0, itemHeight: 37, overscan: 10, visibleCount: 40 })
    );

    // Initial state (scrollTop = 0)
    // startIndex = max(0, 0 - 10) = 0
    // endIndex = min(99, 0 + 40 + 10) = 50
    // visibleItems = items.slice(0, 51)

    expect(result.current.startIndex).toBe(0);
    expect(result.current.visibleItems.length).toBe(51);
    expect(result.current.paddingTop).toBe(0);

    // paddingBottom = (100 - 1 - 50) * 37 = 49 * 37 = 1813
    expect(result.current.paddingBottom).toBe(1813);
  });

  it('should calculate visible items correctly when scrolled down', () => {
    const items = Array.from({ length: 100 }, (_, i) => i);
    // Scrolled 20 items down: 20 * 37 = 740
    const { result } = renderHook(() =>
      useVirtualization({ items, scrollTop: 740, itemHeight: 37, overscan: 10, visibleCount: 40 })
    );

    // startIndex = max(0, 20 - 10) = 10
    // endIndex = min(99, 20 + 40 + 10) = 70

    expect(result.current.startIndex).toBe(10);
    expect(result.current.visibleItems.length).toBe(61); // 70 - 10 + 1

    // paddingTop = 10 * 37 = 370
    expect(result.current.paddingTop).toBe(370);

    // paddingBottom = (100 - 1 - 70) * 37 = 29 * 37 = 1073
    expect(result.current.paddingBottom).toBe(1073);
  });

  it('should cap end index at total items length', () => {
    const items = Array.from({ length: 50 }, (_, i) => i);
    // Scrolled way past the bottom
    const { result } = renderHook(() =>
      useVirtualization({ items, scrollTop: 5000, itemHeight: 37, overscan: 10, visibleCount: 40 })
    );

    // startIndex = max(0, 135 - 10) = 125
    // endIndex = min(49, 135 + 40 + 10) = 49
    // Wait, if startIndex > endIndex, slice will return empty array

    expect(result.current.startIndex).toBe(125);
    expect(result.current.visibleItems.length).toBe(0);
    expect(result.current.paddingBottom).toBe(0);
  });
});
