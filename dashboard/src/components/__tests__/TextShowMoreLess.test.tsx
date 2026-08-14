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
 * Unit tests for TextShowMoreLess component
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { TextShowMoreLess } from '../TextShowMoreLess';
// Mock react-redux used indirectly by commonComponents -> EllipsisText
jest.mock('react-redux', () => ({
  useSelector: () => ({})
}), { virtual: true } as any);
// Avoid axios import chain by mocking API modules consumed by commonComponents
jest.mock('../../api/apiMethods/detailpageApiMethod', () => ({
  getDetailPageData: jest.fn(() => Promise.resolve({ data: {} }))
}));

// Create a theme for testing
const theme = createTheme();

// Wrapper component to provide theme
const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => (
  <ThemeProvider theme={theme}>{children}</ThemeProvider>
);

describe('TextShowMoreLess', () => {
  const mockData = ['Item 1', 'Item 2', 'Item 3', 'Item 4', 'Item 5'];
  const shortData = ['Item 1', 'Item 2'];

  it('should render all items when data length is 2 or less', () => {
    render(
      <TestWrapper>
        <TextShowMoreLess data={shortData} />
      </TestWrapper>
    );

    expect(!!screen.getByText('Item 1')).toBe(true);
    expect(!!screen.getByText('Item 2')).toBe(true);
    expect(screen.queryByText('+ More..')).toBeNull();
  });

  it('should show only first item and "More" link when data length > 2', () => {
    render(
      <TestWrapper>
        <TextShowMoreLess data={mockData} />
      </TestWrapper>
    );

    expect(!!screen.getByText('Item 1')).toBe(true);
    expect(screen.queryByText('Item 2')).toBeNull();
    expect(!!screen.getByText('+ More..')).toBe(true);
  });

  it('should expand to show all items when "More" is clicked', async () => {
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    render(
      <TestWrapper>
        <TextShowMoreLess data={mockData} />
      </TestWrapper>
    );

    // Initially only first item is visible
    expect(!!screen.getByText('Item 1')).toBe(true);
    expect(screen.queryByText('Item 2')).toBeNull();

    // Click "More" link
    await user.click(screen.getByText('+ More..'));

    // Now all items should be visible
    expect(!!screen.getByText('Item 1')).toBe(true);
    expect(!!screen.getByText('Item 2')).toBe(true);
    expect(!!screen.getByText('Item 3')).toBe(true);
    expect(!!screen.getByText('Item 4')).toBe(true);
    expect(!!screen.getByText('Item 5')).toBe(true);
    expect(!!screen.getByText('- Less..')).toBe(true);
  });

  it('should collapse to show only first item when "Less" is clicked', async () => {
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    render(
      <TestWrapper>
        <TextShowMoreLess data={mockData} />
      </TestWrapper>
    );

    // Expand first
    await user.click(screen.getByText('+ More..'));
    expect(!!screen.getByText('Item 5')).toBe(true);

    // Then collapse
    await user.click(screen.getByText('- Less..'));

    // After collapse, only first item should be visible and second hidden
    const firstChip = screen.getAllByRole('button')[0];
    expect(firstChip.textContent?.includes('Item 1')).toBe(true);
    expect(screen.queryByText('Item 2')).toBeNull();
    expect(!!screen.getByText('+ More..')).toBe(true);
  });

  it('should render chips with correct styling', () => {
    render(
      <TestWrapper>
        <TextShowMoreLess data={shortData} />
      </TestWrapper>
    );

    const chips = screen.getAllByRole('button');
    expect(chips.length).toBe(2);
    chips.forEach(chip => {
      expect((chip as HTMLElement).className).toMatch(/MuiChip-root/);
    });
  });

  it('should render simple strings with displayText ignored', () => {
    const data = ['Display Name 1', 'Display Name 2'];
    render(
      <TestWrapper>
        <TextShowMoreLess data={data} displayText="name" />
      </TestWrapper>
    );
    // When data items are strings and displayText is provided, 
    // the component tries key[displayText] which is undefined for strings.
    // The component doesn't check if the property exists, so it renders undefined.
    // Since data.length is 2 (not > 2), it should render both items
    // Check for chips using getAllByRole - chips are rendered as buttons
    const chips = screen.getAllByRole('button');
    // Should render 2 chips (one for each data item)
    // Even though the labels are undefined, the chips should still be rendered
    expect(chips.length).toBe(2);
  });

  it('should show tooltips for chip items', () => {
    render(
      <TestWrapper>
        <TextShowMoreLess data={["Long item name that might need tooltip"]} />
      </TestWrapper>
    );

    const chip = screen.getByText('Long item name that might need tooltip');
    expect(!!chip.closest('[data-mui-internal-clone-element]')).toBe(true);
  });

  it('should have correct CSS classes for show/hide states', () => {
    const { container } = render(
      <TestWrapper>
        <TextShowMoreLess data={mockData} />
      </TestWrapper>
    );

    expect(!!container.querySelector('.show-less')).toBe(true);
    expect(container.querySelector('.show-more')).toBeNull();
  });

  it('should toggle CSS classes when expanding/collapsing', async () => {
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;
    const { container } = render(
      <TestWrapper>
        <TextShowMoreLess data={mockData} />
      </TestWrapper>
    );

    await user.click(screen.getByText('+ More..'));
    expect(!!container.querySelector('.show-more')).toBe(true);
    expect(container.querySelector('.show-less')).toBeNull();

    await user.click(screen.getByText('- Less..'));
    expect(!!container.querySelector('.show-less')).toBe(true);
    expect(container.querySelector('.show-more')).toBeNull();
  });

  it('should have correct test data attributes for Cypress', () => {
    render(
      <TestWrapper>
        <TextShowMoreLess data={mockData} />
      </TestWrapper>
    );

    const moreLink = screen.getByText('+ More..');
    expect(moreLink.getAttribute('data-cy')).toBe('showMore');
    expect(moreLink.getAttribute('data-id')).toBe('showMore');
  });

  it('should handle empty data array gracefully', () => {
    render(
      <TestWrapper>
        <TextShowMoreLess data={[]} />
      </TestWrapper>
    );

    expect(screen.queryByText('+ More..')).toBeNull();
    expect(screen.queryByRole('button')).toBeNull();
  });
});