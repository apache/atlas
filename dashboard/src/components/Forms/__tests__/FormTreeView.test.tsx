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


/* Tests for FormTreeView */
import React from 'react';
import { render, screen } from '@utils/test-utils';
import userEvent from '@testing-library/user-event';
import FormTreeView from '../FormTreeView';

// Mock toast
const dismiss = jest.fn();
const warning = jest.fn();
jest.mock('react-toastify', () => ({
  toast: { dismiss: (...args: any[]) => dismiss(...args), warning: (...args: any[]) => warning(...args) }
}));

// Mock MUI X Tree components to make events deterministic
jest.mock('@mui/x-tree-view', () => {
  const React = require('react');
  const SimpleTreeView = ({ onItemFocus, onExpandedItemsChange, children }: any) => {
    React.useEffect(() => {
      // Trigger focus twice: once without '@' to hit early return, and once with '@'
      onItemFocus?.({}, 'parentA');
      onItemFocus?.({}, 'childA@parentA');
      onExpandedItemsChange?.({}, ['Assets']);
    }, []);
    return <div data-testid="tree-root">{children}</div>;
  };
  const useTreeItemState = () => ({
    disabled: false,
    expanded: false,
    selected: false,
    focused: false,
    handleExpansion: jest.fn(),
    handleSelection: jest.fn(),
    preventSelection: jest.fn()
  });
  return { SimpleTreeView, useTreeItemState };
});

jest.mock('@mui/x-tree-view/TreeItem', () => {
  const React = require('react');
  const { useTreeItemState } = require('@mui/x-tree-view');
  const TreeItem = ({ itemId, label, children, ContentComponent }: any) => {
    const classes = { root: '', iconContainer: '', label: '' } as any;
    const content = ContentComponent ? (
      <ContentComponent classes={classes} className="custom-content-root" itemId={itemId} label={label} />
    ) : (
      label
    );
    return <div data-testid={`tree-item-${itemId}`}>{content}{children}</div>;
  };
  const TreeItemProps = {} as any;
  const TreeItemContentProps = {} as any;
  return { TreeItem, TreeItemProps, TreeItemContentProps, useTreeItemState };
});

// Typography from custom muiComponents can be left as-is

describe('FormTreeView', () => {
  const treeName = 'Assets';
  const onNodeSelect = jest.fn();

  const treeData = [
    {
      id: 'parentA',
      label: 'Parent A',
      types: 'parent',
      parent: null,
      children: [
        { id: 'childA', label: 'Matches', types: 'child', parent: 'parentA', children: [] }
      ]
    },
    {
      id: 'parentEmpty',
      label: 'Empty Parent',
      types: 'parent',
      parent: null,
      children: []
    }
  ] as any;

  it('renders, highlights search matches, handles empty parent click, loader, and focus select', async () => {
    const user = (userEvent as any).setup ? (userEvent as any).setup() : userEvent;

    // Render non-loader first with a searchTerm to exercise highlightText
    const { container, rerender } = render(
      <FormTreeView
        treeData={treeData}
        treeName={treeName}
        loader={false}
        onNodeSelect={onNodeSelect}
        searchTerm="Match"
      />
    );

    // Highlighted span should exist for the matching child label
    expect(container.innerHTML.includes('<span')).toBe(true);

    // Rerender without search filter so empty parent is visible, then click its label
    rerender(
      <FormTreeView
        treeData={treeData}
        treeName={treeName}
        loader={false}
        onNodeSelect={onNodeSelect}
        searchTerm=""
      />
    );
    const emptyParentItem = container.querySelector('[data-testid="tree-item-parentEmpty"] .custom-treeitem-label') as HTMLElement;
    await user.click(emptyParentItem);
    expect(warning).toHaveBeenCalled();

    // onItemFocus mock should have invoked onNodeSelect with id containing '@'
    expect(onNodeSelect).toHaveBeenCalledWith('childA@parentA');

    // Render loader branch
    rerender(
      <FormTreeView
        treeData={treeData}
        treeName={treeName}
        loader={true}
        onNodeSelect={onNodeSelect}
        searchTerm=""
      />
    );
    // Loader element should be present
    expect(container.querySelector('.tree-item-loader')).toBeTruthy();
  });
});


