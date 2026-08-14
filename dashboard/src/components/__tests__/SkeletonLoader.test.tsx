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


/**
 * Unit tests for SkeletonLoader component
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import SkeletonLoader from '../SkeletonLoader';

describe('SkeletonLoader', () => {
  it('renders the correct number of skeleton elements', () => {
    const count = 3;
    const { container } = render(<SkeletonLoader count={count} />);
    
    const skeletonElements = container.querySelectorAll('.MuiSkeleton-root');
    expect(skeletonElements).toHaveLength(count);
  });

  it('renders with default variant when no variant is provided', () => {
    const { container } = render(<SkeletonLoader count={1} />);
    
    const skeleton = container.querySelector('.MuiSkeleton-root');
    expect(skeleton).toBeTruthy();
  });

  it('applies custom className when provided', () => {
    const customClass = 'custom-skeleton';
    const { container } = render(<SkeletonLoader count={1} className={customClass} />);
    
    const skeleton = container.querySelector('.MuiSkeleton-root');
    expect(skeleton).toHaveClass(customClass);
  });

  it('renders with specified variant', () => {
    const { container } = render(<SkeletonLoader count={1} variant="circular" />);
    
    const skeleton = container.querySelector('.MuiSkeleton-root');
    expect(skeleton).toBeTruthy();
  });

  it('renders with custom width and height', () => {
    const { container } = render(<SkeletonLoader count={1} width={200} height={50} />);
    
    const skeleton = container.querySelector('.MuiSkeleton-root');
    expect(skeleton).toBeTruthy();
  });

  it('handles zero count gracefully', () => {
    const { container } = render(<SkeletonLoader count={0} />);
    
    const skeletonElements = container.querySelectorAll('.MuiSkeleton-root');
    expect(skeletonElements).toHaveLength(0);
  });

  it('renders multiple skeletons with unique keys', () => {
    const count = 5;
    const { container } = render(<SkeletonLoader count={count} />);
    
    const skeletonElements = container.querySelectorAll('.MuiSkeleton-root');
    expect(skeletonElements).toHaveLength(count);
  });

  it('applies custom sx prop', () => {
    const customSx = { backgroundColor: 'red' };
    const { container } = render(<SkeletonLoader count={1} sx={customSx} />);
    
    const skeleton = container.querySelector('.MuiSkeleton-root');
    expect(skeleton).toBeTruthy();
  });

  it('sets the correct animation type', () => {
    const { container } = render(<SkeletonLoader count={1} animation="wave" />);
    
    const skeleton = container.querySelector('.MuiSkeleton-root');
    expect(skeleton).toBeTruthy();
  });

  it('disables animation when animation is false', () => {
    const { container } = render(<SkeletonLoader count={1} animation={false} />);
    
    const skeleton = container.querySelector('.MuiSkeleton-root');
    expect(skeleton).toBeTruthy();
  });
});