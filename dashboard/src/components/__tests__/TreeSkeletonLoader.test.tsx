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

import { render } from '@testing-library/react';
import '@testing-library/jest-dom';
import TreeSkeletonLoader from '../TreeSkeletonLoader';

describe('TreeSkeletonLoader', () => {
  it('renders default number of skeletons when count is not provided', () => {
    const { container } = render(<TreeSkeletonLoader />);

    // By default count is 7
    const skeletons = container.querySelectorAll('.MuiSkeleton-root');
    // Each row has 1 arrow, 1 text = 2 skeletons per row
    // 7 rows * 2 = 14 skeletons
    expect(skeletons.length).toBe(14);
  });

  it('renders specific number of skeletons based on count prop', () => {
    const { container } = render(<TreeSkeletonLoader count={2} />);

    const skeletons = container.querySelectorAll('.MuiSkeleton-root');
    // 2 rows * 2 = 4 skeletons
    expect(skeletons.length).toBe(4);
  });

  it('renders correctly with 0 count', () => {
    const { container } = render(<TreeSkeletonLoader count={0} />);

    const skeletons = container.querySelectorAll('.MuiSkeleton-root');
    expect(skeletons.length).toBe(0);
  });
});
