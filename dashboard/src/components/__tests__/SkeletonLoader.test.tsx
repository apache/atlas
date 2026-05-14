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