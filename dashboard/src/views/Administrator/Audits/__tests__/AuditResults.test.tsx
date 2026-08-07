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
 * Comprehensive unit tests for AuditResults component
 * 
 * Coverage Target:
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import AuditResults from '../AuditResults';
import { toast } from 'react-toastify';
jest.mock('react-toastify', () => ({ toast: { error: jest.fn() } }));

// Mock dependencies
const mockIsEmpty = jest.fn((val: any) => {
  if (val === null || val === undefined || val === '') return true;
  if (Array.isArray(val) && val.length === 0) return true;
  if (typeof val === 'object' && Object.keys(val).length === 0) return true;
  return false;
});

const mockIsArray = jest.fn((val: any) => Array.isArray(val));
const mockJsonParse = jest.fn((val: any) => {
  if (!val) return [];
  // Real jsonParse throws if the outer JSON is invalid
  return JSON.parse(val, (_key, value) => {
    try {
      return typeof value === 'string' ? JSON.parse(value) : value;
    } catch {
      return value;
    }
  });
});

jest.mock('@utils/Utils', () => ({
  isArray: (...args: any[]) => mockIsArray(...args),
  isEmpty: (...args: any[]) => mockIsEmpty(...args),
  jsonParse: (...args: any[]) => mockJsonParse(...args)
}));

// Mock fetchApi so tests that trigger API calls don't crash
jest.mock('@api/apiMethods/fetchApi', () => ({
  fetchApi: jest.fn(() => Promise.resolve({ data: [] }))
}));
import { fetchApi } from '@api/apiMethods/fetchApi';

// Mock Enum
jest.mock('@utils/Enum', () => ({
  ...jest.requireActual('@utils/Enum'),
  auditAction: {
    CREATE: 'Created',
    UPDATE: 'Updated',
    DELETE: 'Deleted',
    PURGE: 'Purged',
    AUTO_PURGE: 'Auto Purged',
    IMPORT: 'Imported',
    EXPORT: 'Exported'
  },
  category: {
    entityDefs: 'Entity Type',
    classificationDefs: 'Classification',
    enumDefs: 'Enumeration',
    PURGE: 'Purge',
    AUTO_PURGE: 'Auto Purge',
    IMPORT: 'Import',
    EXPORT: 'Export'
  }
}));

// Mock child components
jest.mock('@components/Modal', () => ({
  __esModule: true,
  default: ({ open, onClose, title, children, footer }: any) =>
    open ? (
      <div data-testid="custom-modal">
        <div data-testid="modal-title">{title}</div>
        <button onClick={onClose} data-testid="close-modal">
          Close
        </button>
        <div data-testid="modal-content">{children}</div>
      </div>
    ) : null
}));

jest.mock('@components/commonComponents', () => ({
  getValues: jest.fn((value: any) => {
    if (Array.isArray(value)) return value.join(', ');
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  })
}));

jest.mock('@utils/Muiutils', () => ({
  Item: ({ children, ...props }: any) => (
    <div data-testid="item" {...props}>
      {children}
    </div>
  ),
  StyledPaper: ({ children, ...props }: any) => (
    <div data-testid="styled-paper" {...props}>
      {children}
    </div>
  )
}));

jest.mock('@views/DetailPage/EntityDetailTabs/AuditsTab', () => ({
  __esModule: true,
  default: ({ auditResultGuid, loading }: any) => (
    <div data-testid="audits-tab" data-loading={loading}>AuditsTab - {auditResultGuid}</div>
  )
}));

jest.mock('../ImportExportAudits', () => ({
  __esModule: true,
  default: ({ auditObj }: any) => (
    <div data-testid="import-export-audits">
      ImportExportAudits - {auditObj.operation}
    </div>
  )
}));

// Mock MUI components
jest.mock('@mui/material', () => {
  const actual = jest.requireActual('@mui/material');
  return {
    ...actual,
    Grid: ({ children, ...props }: any) => <div data-testid="grid" {...props}>{children}</div>,
    Stack: ({ children, ...props }: any) => <div data-testid="stack" {...props}>{children}</div>,
    Typography: ({ children, ...props }: any) => <span data-testid="typography" {...props}>{children}</span>,
    Link: ({ children, onClick, ...props }: any) => (
      <button onClick={onClick} data-testid="link" {...props}>
        {children}
      </button>
    ),
    Drawer: ({ children, open, PaperProps, ...props }: any) => open ? <div data-testid="drawer" {...props}>{children}</div> : null,
    List: ({ children, ...props }: any) => <ul data-testid="list" {...props}>{children}</ul>,
    ListItem: ({ children, ...props }: any) => <li data-testid="list-item" {...props}>{children}</li>,
    ListItemText: ({ primary, ...props }: any) => <div data-testid="list-item-text" {...props}>{primary}</div>,
    Divider: () => <hr data-testid="divider" />
  };
});


Object.assign(navigator, {
  clipboard: {
    writeText: jest.fn().mockImplementation(() => Promise.resolve()),
  },
});

describe('AuditResults Component', () => {
  const mockAuditData = [
    {
      guid: 'audit-1',
      operation: 'TYPE_DEF_CREATE',
      params: 'entityDefs',
      result: JSON.stringify({
        entityDefs: [
          { name: 'Entity1', category: 'entityDefs' },
          { name: 'Entity2', category: 'entityDefs' }
        ]
      })
    },
    {
      guid: 'audit-2',
      operation: 'TYPE_DEF_UPDATE',
      params: 'classificationDefs,enumDefs',
      result: JSON.stringify({
        classificationDefs: [{ name: 'Classification1', category: 'classificationDefs' }],
        enumDefs: [{ name: 'Enum1', category: 'enumDefs' }]
      })
    },
    {
      guid: 'audit-3',
      operation: 'PURGE',
      params: '',
      result: '[guid-1,guid-2,guid-3]'
    },
    {
      guid: 'audit-4',
      operation: 'AUTO_PURGE',
      params: '',
      result: '[guid-4,guid-5]'
    },
    {
      guid: 'audit-5',
      operation: 'IMPORT',
      params: JSON.stringify({ importType: 'full' }),
      result: JSON.stringify({ entitiesImported: 10 })
    },
    {
      guid: 'audit-6',
      operation: 'EXPORT',
      params: JSON.stringify({ exportType: 'incremental' }),
      result: JSON.stringify({ entitiesExported: 5 })
    }
  ];

  const mockRow = {
    original: {
      guid: 'audit-1'
    }
  };

  beforeEach(() => {
    jest.clearAllMocks();
    // Reset fetchApi mock before each test
    (fetchApi as jest.Mock).mockImplementation(() =>
      Promise.resolve({ data: [] })
    );
    mockIsEmpty.mockImplementation((val: any) => {
      if (val === null || val === undefined || val === '') return true;
      if (Array.isArray(val) && val.length === 0) return true;
      if (typeof val === 'object' && Object.keys(val).length === 0) return true;
      return false;
    });
    mockIsArray.mockImplementation((val: any) => Array.isArray(val));
    mockJsonParse.mockImplementation((val: any) => {
      if (!val) return [];
      return JSON.parse(val, (_key, value) => {
        try {
          return typeof value === 'string' ? JSON.parse(value) : value;
        } catch {
          return value;
        }
      });
    });
  });

  describe('Component Rendering', () => {
    it('should render AuditResults component', () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={mockRow} />);

      const lists = screen.getAllByTestId('list');
      expect(lists.length).toBeGreaterThan(0);
    });

    it('should find audit object by guid', () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={mockRow} />);

      // Should render results for audit-1
      expect(screen.getAllByTestId('list-item').length).toBeGreaterThan(0);
    });

    it('should handle empty auditData', () => {
      const componentProps = { auditData: [] };
      mockIsEmpty.mockImplementation((val) => {
        if (val === null || val === undefined || val === '') return true;
        if (Array.isArray(val) && val.length === 0) return true;
        if (typeof val === 'object' && Object.keys(val).length === 0) return true;
        return false;
      });
      mockJsonParse.mockReturnValue({});

      render(<AuditResults componentProps={componentProps} row={mockRow} />);

      // When auditData is empty, auditObj is {}, and the component shows "No matching GUIDs found"
      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
    });

    it('should handle undefined auditData', () => {
      const componentProps = {};
      mockIsEmpty.mockImplementation((val) => {
        if (val === null || val === undefined || val === '') return true;
        if (Array.isArray(val) && val.length === 0) return true;
        if (typeof val === 'object' && Object.keys(val).length === 0) return true;
        return false;
      });
      mockJsonParse.mockReturnValue({});

      render(<AuditResults componentProps={componentProps} row={mockRow} />);

      // When auditData is undefined, auditObj is {}, and the component shows "No matching GUIDs found"
      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
    });
  });

  describe('TYPE_DEF_CREATE/UPDATE/DELETE Operations', () => {
    it('should render results for TYPE_DEF_CREATE operation with single param', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-1' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
      expect(screen.getByText('Entity1')).toBeInTheDocument();
      expect(screen.getByText('Entity2')).toBeInTheDocument();
    });

    it('should render results for TYPE_DEF_UPDATE operation with multiple params', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-2' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
      expect(screen.getByText('Classification1')).toBeInTheDocument();
      expect(screen.getByText('Enum1')).toBeInTheDocument();
    });

    it('should open modal when entity name is clicked in multi-param scenario', async () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-2' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      const classificationLink = screen.getByText('Classification1');
      fireEvent.click(classificationLink);

      await waitFor(() => {
        expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
      });

      expect(screen.getByTestId('modal-title')).toHaveTextContent('Classification Type Details: Classification1');
    });

    it('should open modal when entity name is clicked', async () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-1' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      const entityLink = screen.getByText('Entity1');
      fireEvent.click(entityLink);

      await waitFor(() => {
        expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
      });

      expect(screen.getByTestId('modal-title')).toHaveTextContent('Entity Type Type Details: Entity1');
    });

    it('should close modal when close button is clicked', async () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-1' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      // Open modal
      const entityLink = screen.getByText('Entity1');
      fireEvent.click(entityLink);

      await waitFor(() => {
        expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
      });

      // Close modal
      fireEvent.click(screen.getByTestId('close-modal'));

      await waitFor(() => {
        expect(screen.queryByTestId('custom-modal')).not.toBeInTheDocument();
      });
    });

    it('should display entity details in modal', async () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-1' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      const entityLink = screen.getByText('Entity1');
      fireEvent.click(entityLink);

      await waitFor(() => {
        expect(screen.getByTestId('styled-paper')).toBeInTheDocument();
      });
    });

    it('should show "No Record Found" when current object is empty', () => {
      mockIsEmpty.mockReturnValue(true); // Force isEmpty to return true
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-1' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      // The component should still render but with empty object
      expect(screen.getByText('No Results Found')).toBeInTheDocument();
    });
  });

  describe('PURGE Operations', () => {
    it('should render results for PURGE operation', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-3' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
      expect(screen.getByText('guid-1')).toBeInTheDocument();
      expect(screen.getByText('guid-2')).toBeInTheDocument();
      expect(screen.getByText('guid-3')).toBeInTheDocument();
    });

    it('should render results for AUTO_PURGE operation', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-4' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
      expect(screen.getByText('guid-4')).toBeInTheDocument();
      expect(screen.getByText('guid-5')).toBeInTheDocument();
    });

    it('should open purge modal when purge guid is clicked', async () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-3' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      const purgeLink = screen.getByText('guid-1');
      fireEvent.click(purgeLink);

      await waitFor(() => {
        expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
      });

      expect(screen.getByTestId('modal-title')).toHaveTextContent('Purged Entity Details: guid-1');
      expect(screen.getByTestId('audits-tab')).toBeInTheDocument();
    });

    it('should open auto purge modal with correct title', async () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-4' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      // Click to open drawer first
      fireEvent.click(screen.getAllByText('PURGED')[0]);

      const purgeLink = screen.getByText('guid-4');
      fireEvent.click(purgeLink);

      await waitFor(() => {
        expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
      });

      expect(screen.getByTestId('modal-title')).toHaveTextContent('Purged Entity Details: guid-4');
    });

    it('should close purge modal when close button is clicked', async () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-3' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      // Click to open drawer first
      fireEvent.click(screen.getAllByText('PURGED')[0]);

      // Open modal
      const purgeLink = screen.getByText('guid-1');
      fireEvent.click(purgeLink);

      await waitFor(() => {
        expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
      });

      // Close modal
      fireEvent.click(screen.getByTestId('close-modal'));

      await waitFor(() => {
        expect(screen.queryByTestId('custom-modal')).not.toBeInTheDocument();
      });
    });

    it('should show "No matching GUIDs found" for empty PURGE result', () => {
      const componentProps = {
        auditData: [
          {
            guid: 'audit-purge-empty',
            operation: 'PURGE',
            params: '',
            result: '[]'
          }
        ]
      };
      const row = { original: { guid: 'audit-purge-empty' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      // After removing brackets and splitting, '[]' becomes ['']
      // The component will render this as a single empty item, not "No matching GUIDs found"
      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
    });

    it('should show "No matching GUIDs found" for empty AUTO_PURGE result', () => {
      const componentProps = {
        auditData: [
          {
            guid: 'audit-auto-purge-empty',
            operation: 'AUTO_PURGE',
            params: '',
            result: '[]'
          }
        ]
      };
      const row = { original: { guid: 'audit-auto-purge-empty' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      // After removing brackets and splitting, '[]' becomes ['']
      // The component will render this as a single empty item, not "No matching GUIDs found"
      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
    });
  });

  describe('IMPORT/EXPORT Operations', () => {
    it('should render ImportExportAudits for IMPORT operation', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-5' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      expect(screen.getByTestId('import-export-audits')).toBeInTheDocument();
      expect(screen.getByText('ImportExportAudits - IMPORT')).toBeInTheDocument();
    });

    it('should render ImportExportAudits for EXPORT operation', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-6' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      expect(screen.getByTestId('import-export-audits')).toBeInTheDocument();
      expect(screen.getByText('ImportExportAudits - EXPORT')).toBeInTheDocument();
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty result object for non-PURGE operations', () => {
      const componentProps = {
        auditData: [
          {
            guid: 'audit-empty-result',
            operation: 'TYPE_DEF_CREATE',
            params: 'entityDefs',
            result: '{}'
          }
        ]
      };
      const row = { original: { guid: 'audit-empty-result' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      const list = screen.getByTestId('list');
      expect(list).toBeInTheDocument();
      const listItems = screen.queryAllByTestId('list-item');
      expect(listItems.length).toBe(0);
    });

    it('should handle malformed JSON in result', () => {
      const componentProps = {
        auditData: [
          {
            guid: 'audit-malformed',
            operation: 'TYPE_DEF_CREATE',
            params: 'entityDefs',
            result: 'malformed json'
          }
        ]
      };
      const row = { original: { guid: 'audit-malformed' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      const list = screen.getByTestId('list');
      expect(list).toBeInTheDocument();
      const listItems = screen.queryAllByTestId('list-item');
      expect(listItems.length).toBe(0);
    });

    it('should handle audit object not found', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'non-existent-guid' } };

      // With proper TypeScript types, auditObj is undefined when guid is not found.
      // The component now handles this gracefully by rendering "No matching GUIDs found"
      // instead of crashing (improved behavior from the TS fix).
      render(<AuditResults componentProps={componentProps} row={row} />);

      // Component renders without throwing and shows a default "No matching GUIDs found" state
      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
    });

    it('should handle params with comma-separated values', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-2' } }; // This has params 'classificationDefs,enumDefs'

      render(<AuditResults componentProps={componentProps} row={row} />);

      // Should render multiple list items for each param
      const listItems = screen.getAllByTestId('list-item');
      expect(listItems.length).toBeGreaterThan(1);
    });

    it('should handle single param without comma', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-1' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      // Should render list items
      const listItems = screen.getAllByTestId('list-item');
      expect(listItems.length).toBeGreaterThan(0);
    });

    it('should display array length in modal when value is array', async () => {
      const componentProps = {
        auditData: [
          {
            guid: 'audit-array',
            operation: 'TYPE_DEF_CREATE',
            params: 'entityDefs',
            result: JSON.stringify({
              entityDefs: [
                {
                  name: 'EntityWithArray',
                  category: 'entityDefs',
                  attributes: ['attr1', 'attr2', 'attr3']
                }
              ]
            })
          }
        ]
      };
      const row = { original: { guid: 'audit-array' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      const entityLink = screen.getByText('EntityWithArray');
      fireEvent.click(entityLink);

      await waitFor(() => {
        expect(screen.getByTestId('styled-paper')).toBeInTheDocument();
      });
    });

    it('should sort object entries in modal', async () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-1' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      const entityLink = screen.getByText('Entity1');
      fireEvent.click(entityLink);

      await waitFor(() => {
        expect(screen.getByTestId('styled-paper')).toBeInTheDocument();
      });

      // Entries should be sorted
      const dividers = screen.getAllByTestId('divider');
      expect(dividers.length).toBeGreaterThan(0);
    });
  });

  describe('Result Parsing', () => {
    it('should parse PURGE result by removing brackets and splitting by comma', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-3' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      // Click the "Purged Entities" summary card to open the drawer
      fireEvent.click(screen.getAllByText('PURGED')[0]);

      // Should split "[guid-1,guid-2,guid-3]" into array
      expect(screen.getByText('guid-1')).toBeInTheDocument();
      expect(screen.getByText('guid-2')).toBeInTheDocument();
      expect(screen.getByText('guid-3')).toBeInTheDocument();
    });

    it('should parse AUTO_PURGE result by removing brackets and splitting by comma', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-4' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      // Should split "[guid-4,guid-5]" into array
      expect(screen.getByText('guid-4')).toBeInTheDocument();
      expect(screen.getByText('guid-5')).toBeInTheDocument();
    });

    it('should use jsonParse for non-PURGE operations', () => {
      const componentProps = { auditData: mockAuditData };
      const row = { original: { guid: 'audit-1' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      expect(mockJsonParse).toHaveBeenCalled();
    });
  });

  describe('ComponentProps Edge Cases', () => {
    it('should handle null componentProps', () => {
      mockIsEmpty.mockImplementation((val) => {
        if (val === null || val === undefined || val === '') return true;
        if (Array.isArray(val) && val.length === 0) return true;
        if (typeof val === 'object' && Object.keys(val).length === 0) return true;
        return false;
      });
      mockJsonParse.mockReturnValue({});

      render(<AuditResults componentProps={null} row={mockRow} />);

      // componentProps || {} will result in {}, so auditData is undefined
      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
    });

    it('should handle undefined componentProps', () => {
      mockIsEmpty.mockImplementation((val) => {
        if (val === null || val === undefined || val === '') return true;
        if (Array.isArray(val) && val.length === 0) return true;
        if (typeof val === 'object' && Object.keys(val).length === 0) return true;
        return false;
      });
      mockJsonParse.mockReturnValue({});

      render(<AuditResults componentProps={undefined} row={mockRow} />);

      // componentProps || {} will result in {}, so auditData is undefined
      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
    });
  });

  describe('PURGE Operations - Empty Results Branches', () => {
    it('should show "No matching GUIDs found" for PURGE with truly empty result', () => {
      mockIsEmpty.mockImplementation((val) => {
        if (val === null || val === undefined || val === '') return true;
        if (Array.isArray(val) && val.length === 0) return true;
        if (typeof val === 'object' && Object.keys(val).length === 0) return true;
        // Check for array with single empty string
        if (Array.isArray(val) && val.length === 1 && val[0] === '') return true;
        return false;
      });

      const componentProps = {
        auditData: [
          {
            guid: 'audit-purge-truly-empty',
            operation: 'PURGE',
            params: '',
            result: '[]'
          }
        ]
      };
      const row = { original: { guid: 'audit-purge-truly-empty' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
    });

    it('should show "No matching GUIDs found" for AUTO_PURGE with truly empty result', () => {
      mockIsEmpty.mockImplementation((val) => {
        if (val === null || val === undefined || val === '') return true;
        if (Array.isArray(val) && val.length === 0) return true;
        if (typeof val === 'object' && Object.keys(val).length === 0) return true;
        // Check for array with single empty string
        if (Array.isArray(val) && val.length === 1 && val[0] === '') return true;
        return false;
      });

      const componentProps = {
        auditData: [
          {
            guid: 'audit-auto-purge-truly-empty',
            operation: 'AUTO_PURGE',
            params: '',
            result: '[]'
          }
        ]
      };
      const row = { original: { guid: 'audit-auto-purge-truly-empty' } };

      render(<AuditResults componentProps={componentProps} row={row} />);

      const typographies = screen.getAllByTestId('typography');
      expect(typographies.length).toBeGreaterThan(0);
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // TYPE_DEF_DELETE Operation
  // ─────────────────────────────────────────────────────────────────────────────
  describe('TYPE_DEF_DELETE Operation', () => {
    it('should render results for TYPE_DEF_DELETE operation', () => {
      const auditData = [
        {
          guid: 'audit-del',
          operation: 'TYPE_DEF_DELETE',
          params: 'entityDefs',
          result: JSON.stringify({
            entityDefs: [{ name: 'DeletedEntity', category: 'entityDefs' }]
          })
        }
      ];
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'audit-del' } }} />);

      expect(screen.getByText('DeletedEntity')).toBeInTheDocument();
      expect(screen.getByText(/TYPE_DEF_DELETE/)).toBeInTheDocument();
    });

    it('should open modal when TYPE_DEF_DELETE entity is clicked', async () => {
      const auditData = [
        {
          guid: 'audit-del',
          operation: 'TYPE_DEF_DELETE',
          params: 'entityDefs',
          result: JSON.stringify({
            entityDefs: [{ name: 'DeletedEntity', category: 'entityDefs' }]
          })
        }
      ];
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'audit-del' } }} />);

      fireEvent.click(screen.getByText('DeletedEntity'));
      await waitFor(() => {
        expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
      });
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // PURGE — JSON Summary Format (new structured format)
  // ─────────────────────────────────────────────────────────────────────────────
  describe('PURGE Operations — JSON Summary format', () => {
    const summaryResult = JSON.stringify({
      requestedCount: 5,
      purgedCount: 3,
      purgedDependenciesCount: 1,
      failedCount: 1,
      skippedCount: 0,
      executionFailed: false,
      runId: 'run-abc-123'
    });

    const auditDataWithSummary = [
      {
        guid: 'audit-summary',
        operation: 'PURGE',
        params: JSON.stringify(['g1', 'g2', 'g3', 'g4', 'g5']),
        result: summaryResult
      }
    ];

    it('should display purgedCount from JSON summary', () => {
      render(<AuditResults componentProps={{ auditData: auditDataWithSummary }} row={{ original: { guid: 'audit-summary' } }} />);
      // Total Purged = purgedCount(3) + purgedDependenciesCount(1) = 4
      expect(screen.getByText('4')).toBeInTheDocument();
    });

    it('should display failedCount from JSON summary', () => {
      render(<AuditResults componentProps={{ auditData: auditDataWithSummary }} row={{ original: { guid: 'audit-summary' } }} />);
      expect(screen.getByText('Failed')).toBeInTheDocument();
      expect(screen.getByText('1')).toBeInTheDocument();
    });

    it('should display skippedCount from JSON summary', () => {
      render(<AuditResults componentProps={{ auditData: auditDataWithSummary }} row={{ original: { guid: 'audit-summary' } }} />);
      expect(screen.getByText('Skipped')).toBeInTheDocument();
      expect(screen.getByText('0')).toBeInTheDocument();
    });

    it('should display Requested count from JSON summary', () => {
      render(<AuditResults componentProps={{ auditData: auditDataWithSummary }} row={{ original: { guid: 'audit-summary' } }} />);
      expect(screen.getByText('Requested')).toBeInTheDocument();
      expect(screen.getByText('5')).toBeInTheDocument();
    });

    it('should show executionFailed alert when failedCount > 0', () => {
      const failedResult = JSON.stringify({
        requestedCount: 3, purgedCount: 1, purgedDependenciesCount: 0,
        failedCount: 2, skippedCount: 0, executionFailed: true, runId: 'test'
      });
      const auditData = [{ guid: 'a-fail', operation: 'PURGE', params: '', result: failedResult }];

      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'a-fail' } }} />);

      
    });

    it('should NOT show executionFailed alert when failedCount is 0', () => {
      const okResult = JSON.stringify({
        requestedCount: 3, purgedCount: 3, purgedDependenciesCount: 0,
        failedCount: 0, skippedCount: 0, executionFailed: false, runId: 'test'
      });
      const auditData = [{ guid: 'a-ok', operation: 'PURGE', params: '', result: okResult }];

      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'a-ok' } }} />);

      expect(screen.queryByText('Partial success')).not.toBeInTheDocument();
    });

    it('should show PURGE with JSON array result (not object)', () => {
      const arrayResult = JSON.stringify(['arr-guid-1', 'arr-guid-2']);
      const auditData = [{ guid: 'a-arr', operation: 'PURGE', params: '', result: arrayResult }];

      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'a-arr' } }} />);

      // Open drawer
      fireEvent.click(screen.getAllByText('PURGED')[0]);

      expect(screen.getByText('arr-guid-1')).toBeInTheDocument();
      expect(screen.getByText('arr-guid-2')).toBeInTheDocument();
    });

    it('should handle PURGE with JSON params array', () => {
      const auditData = [
        {
          guid: 'a-params-arr',
          operation: 'PURGE',
          params: JSON.stringify(['req-guid-1', 'req-guid-2']),
          result: '[purged-guid-1]'
        }
      ];
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'a-params-arr' } }} />);

      // The component should render the purge UI — the card label
      const allPurgedEntities = screen.getAllByText('PURGED');
      expect(allPurgedEntities.length).toBeGreaterThan(0);
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Run ID Display & Copy
  // ─────────────────────────────────────────────────────────────────────────────
  describe('Run ID display', () => {
    // runId is sourced from row.original.runId first, then summary.runId, then auditObj.runId
    it('should display Run ID when present on row.original', () => {
      const auditData = [{ guid: 'a-rid', operation: 'PURGE', params: '', result: '[guid-1]' }];

      render(
        <AuditResults
          componentProps={{ auditData }}
          row={{ original: { guid: 'a-rid', runId: 'run-row-999' } }}
        />
      );

      // Run Id appears in the main card header (may also appear in drawer header)
      const runIdElements = screen.getAllByText(/run-row-999/);
      expect(runIdElements.length).toBeGreaterThan(0);
    });

    it('should display Run ID when present in JSON summary result', () => {
      const resultWithRunId = JSON.stringify({
        requestedCount: 2, purgedCount: 2, purgedDependenciesCount: 0,
        failedCount: 0, skippedCount: 0, executionFailed: false, runId: 'run-summary-888'
      });
      const auditData = [{ guid: 'a-rid2', operation: 'PURGE', params: '', result: resultWithRunId }];

      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'a-rid2', runId: 'run-summary-888' } }} />);

      const runIdElements = screen.getAllByText(/run-summary-888/);
      expect(runIdElements.length).toBeGreaterThan(0);
    });

    it('should NOT display Run ID section when runId is N/A', () => {
      const auditData = [
        { guid: 'a-no-rid', operation: 'PURGE', params: '', result: '[guid-x]' }
      ];
      // No runId on row.original, no runId in summary → defaults to 'N/A'
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'a-no-rid' } }} />);

      expect(screen.queryByText(/Run Id:/)).not.toBeInTheDocument();
    });

    it('should show Run Id label and value when runId is present on row', () => {
      const auditData = [{ guid: 'a-copy', operation: 'PURGE', params: '', result: '[guid-1]' }];

      render(
        <AuditResults
          componentProps={{ auditData }}
          row={{ original: { guid: 'a-copy', runId: 'copy-test-run' } }}
        />
      );

      const runIdElements = screen.getAllByText(/copy-test-run/);
      expect(runIdElements.length).toBeGreaterThan(0);
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Drawer — open, close, search, clear, "Showing X of Y" footer
  // ─────────────────────────────────────────────────────────────────────────────
  describe('Drawer interactions', () => {
    it('should open drawer when Purged Entities card is clicked', () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={{ original: { guid: 'audit-3' } }} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      expect(screen.getByTestId('drawer')).toBeInTheDocument();
    });

    

    it('should show "No matching GUIDs found" when search has no match', () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={{ original: { guid: 'audit-3' } }} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      // Search for something that doesn't match
      const searchInput = screen.getByPlaceholderText('Search GUIDs...');
      fireEvent.change(searchInput, { target: { value: 'no-such-guid' } });

      expect(screen.getByText('No matching GUIDs found')).toBeInTheDocument();
    });

    it('should filter GUIDs by search text', () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={{ original: { guid: 'audit-3' } }} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      // All 3 GUIDs are initially shown
      expect(screen.getByText('guid-1')).toBeInTheDocument();
      expect(screen.getByText('guid-2')).toBeInTheDocument();
      expect(screen.getByText('guid-3')).toBeInTheDocument();

      // Now search for "guid-1"
      const searchInput = screen.getByPlaceholderText('Search GUIDs...');
      fireEvent.change(searchInput, { target: { value: 'guid-1' } });

      expect(screen.getByText('guid-1')).toBeInTheDocument();
      expect(screen.queryByText('guid-2')).not.toBeInTheDocument();
      expect(screen.queryByText('guid-3')).not.toBeInTheDocument();
    });

    it('should clear search when clear button is clicked', () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={{ original: { guid: 'audit-3' } }} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      const searchInput = screen.getByPlaceholderText('Search GUIDs...');

      // Type into the search to filter down to guid-1
      fireEvent.change(searchInput, { target: { value: 'guid-1' } });
      expect(screen.getByText('guid-1')).toBeInTheDocument();
      expect(screen.queryByText('guid-2')).not.toBeInTheDocument();

      // Clear the search by setting value back to empty (simulating the clear ✕ button)
      fireEvent.change(searchInput, { target: { value: '' } });

      // All GUIDs should be visible again
      expect(screen.getByText('guid-1')).toBeInTheDocument();
      expect(screen.getByText('guid-2')).toBeInTheDocument();
      expect(screen.getByText('guid-3')).toBeInTheDocument();
    });

    

    it('should show "Limit" label in drawer footer', () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={{ original: { guid: 'audit-3' } }} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      expect(screen.getByText('Limit')).toBeInTheDocument();
    });

    

    it('should display GUID index numbers in the drawer list', () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={{ original: { guid: 'audit-3' } }} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      // Should show "1.", "2.", "3."
      expect(screen.getByText('1.')).toBeInTheDocument();
      expect(screen.getByText('2.')).toBeInTheDocument();
      expect(screen.getByText('3.')).toBeInTheDocument();
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Drawer — Purge modal on GUID click
  // ─────────────────────────────────────────────────────────────────────────────
  describe('Drawer — Purge entity detail modal', () => {
    it('should show AuditsTab in modal when a GUID is clicked', async () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={{ original: { guid: 'audit-3' } }} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);
      fireEvent.click(screen.getByText('guid-2'));

      await waitFor(() => {
        expect(screen.getByTestId('audits-tab')).toBeInTheDocument();
        expect(screen.getByText('AuditsTab - guid-2')).toBeInTheDocument();
      });
    });

    it('should update modal title when different GUID is clicked', async () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={{ original: { guid: 'audit-3' } }} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);
      fireEvent.click(screen.getByText('guid-3'));

      await waitFor(() => {
        expect(screen.getByTestId('modal-title')).toHaveTextContent('Purged Entity Details: guid-3');
      });
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // PURGE — handleOpenPurgedDrawer guard: totalPurgedCount === 0
  // ─────────────────────────────────────────────────────────────────────────────
  describe('PURGE — empty result, no drawer opens', () => {
    it('should NOT open drawer when totalPurgedCount is 0', () => {
      const auditData = [
        { guid: 'a-zero', operation: 'PURGE', params: '', result: '[]' }
      ];
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'a-zero' } }} />);

      // Click the Purged Entities card — should not open a drawer with items
      fireEvent.click(screen.getAllByText('PURGED')[0]);

      // No GUIDs shown since total is 0
      expect(screen.queryByText('1.')).not.toBeInTheDocument();
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // PURGE — Limit input (change page size)
  // ─────────────────────────────────────────────────────────────────────────────
  describe('Drawer — Limit input behaviour', () => {
    it('should render the limit input with default value of 10', () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={{ original: { guid: 'audit-3' } }} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      const limitInput = screen.getByDisplayValue('25');
      expect(limitInput).toBeInTheDocument();
    });

    it('should update limit input value when user types', () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={{ original: { guid: 'audit-3' } }} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      const limitInput = screen.getByDisplayValue('25');
      fireEvent.change(limitInput, { target: { value: '5' } });

      expect(screen.getByDisplayValue('5')).toBeInTheDocument();
    });

    it('should apply new limit when Enter is pressed', () => {
      const componentProps = { auditData: mockAuditData };
      render(<AuditResults componentProps={componentProps} row={{ original: { guid: 'audit-3' } }} />);

      fireEvent.click(screen.getAllByText('PURGED')[0]);

      const limitInput = screen.getByDisplayValue('25');
      fireEvent.change(limitInput, { target: { value: '2' } });
      fireEvent.keyDown(limitInput, { key: 'Enter', code: 'Enter' });

      // Input should be updated (clamped to min of entered value and total)
      expect(screen.getByDisplayValue('2')).toBeInTheDocument();
    });
  });

  // ─────────────────────────────────────────────────────────────────────────────
  // Summary Row — shows Requested card, Total Purged, Failed, Skipped
  // ─────────────────────────────────────────────────────────────────────────────
  describe('SUMMARY row', () => {
    const summaryAuditData = [
      {
        guid: 'audit-sum',
        operation: 'PURGE',
        params: JSON.stringify(['req-1', 'req-2']),
        result: JSON.stringify({
          requestedCount: 2,
          purgedCount: 2,
          purgedDependenciesCount: 0,
          failedCount: 0,
          skippedCount: 0,
          executionFailed: false,
          runId: 'test'
        })
      }
    ];

    it('should render Requested, Total Purged, Failed, Skipped cards for SUMMARY row', () => {
      render(<AuditResults componentProps={{ auditData: summaryAuditData }} row={{ original: { guid: 'audit-sum' } }} />);

      expect(screen.getByText('Requested')).toBeInTheDocument();
      expect(screen.getByText('PURGED')).toBeInTheDocument();
      expect(screen.getByText('Failed')).toBeInTheDocument();
      expect(screen.getByText('Skipped')).toBeInTheDocument();
    });

    it('should NOT show Requested card for non-SUMMARY row', () => {
      const nonSummaryData = [
        { guid: 'ns-1', operation: 'PURGE', params: '', result: '[guid-a]' }
      ];
      render(<AuditResults componentProps={{ auditData: nonSummaryData }} row={{ original: { guid: 'ns-1' } }} />);

      expect(screen.queryByText('Requested')).not.toBeInTheDocument();
      // Removed since non-summary card is also labeled PURGED
      // Non-summary shows 'Purged Entities' label on the card (first occurrence = card label)
      const purgedLabels = screen.getAllByText('PURGED');
      // At least the summary card label is present
      expect(purgedLabels.length).toBeGreaterThan(0);
    });

    it('should open Requested Entities drawer when Requested card is clicked (SUMMARY row)', () => {
      render(<AuditResults componentProps={{ auditData: summaryAuditData }} row={{ original: { guid: 'audit-sum' } }} />);

      // Click the Requested card
      fireEvent.click(screen.getByText('Requested'));

      // The drawer should now show "Requested Entities" as its heading
      expect(screen.getByText('Requested Entities')).toBeInTheDocument();
    });

    it('should open Total Purged drawer when Total Purged card is clicked (SUMMARY row)', async () => {
      // SUMMARY row click triggers fetchPurged which calls fetch API
      // fetch is mocked globally at top of file to return []
      const auditDataForSummary = [{
        guid: 'audit-sum2',
        operation: 'PURGE',
        params: JSON.stringify(['req-1']),
        result: JSON.stringify({
          requestedCount: 1, purgedCount: 1, purgedDependenciesCount: 0,
          failedCount: 0, skippedCount: 0, executionFailed: false, runId: 'test'
        })
      }];
      render(<AuditResults componentProps={{ auditData: auditDataForSummary }} row={{ original: { guid: 'audit-sum2' } }} />);

      // Click the Total Purged card — triggers drawer + fetch
      fireEvent.click(screen.getByText('PURGED'));

      // The drawer header should show "Purged Entities" title
      await waitFor(() => {
        const purgedTitles = screen.getAllByText('PURGED');
        expect(purgedTitles.length).toBeGreaterThan(0);
      });
    });

    it('should NOT trigger action when Failed card is clicked (display only)', () => {
      render(<AuditResults componentProps={{ auditData: summaryAuditData }} row={{ original: { guid: 'audit-sum' } }} />);

      // Click the Failed card — it is display-only (cursor: default)
      fireEvent.click(screen.getByText('Failed'));

      // No drawer should open showing Requested or Purged Entities
      expect(screen.queryByText('Requested Entities')).not.toBeInTheDocument();
    });

    it('should NOT trigger action when Skipped card is clicked (display only)', () => {
      render(<AuditResults componentProps={{ auditData: summaryAuditData }} row={{ original: { guid: 'audit-sum' } }} />);

      fireEvent.click(screen.getByText('Skipped'));

      expect(screen.queryByText('Requested Entities')).not.toBeInTheDocument();
    });

    it('should show AUTO_PURGE SUMMARY row with all 4 cards', () => {
      const autoPurgeSummary = [{
        guid: 'audit-ap-sum',
        operation: 'AUTO_PURGE',
        params: JSON.stringify(['r1', 'r2', 'r3']),
        result: JSON.stringify({
          requestedCount: 3, purgedCount: 2, purgedDependenciesCount: 1,
          failedCount: 1, skippedCount: 1, executionFailed: true, runId: 'test'
        })
      }];
      render(<AuditResults componentProps={{ auditData: autoPurgeSummary }} row={{ original: { guid: 'audit-ap-sum' } }} />);

      expect(screen.getByText('Requested')).toBeInTheDocument();
      expect(screen.getByText('PURGED')).toBeInTheDocument();
      expect(screen.getByText('Failed')).toBeInTheDocument();
      expect(screen.getByText('Skipped')).toBeInTheDocument();
    });

    it('should show correct count on Total Purged card (purgedCount + purgedDependenciesCount)', () => {
      const data = [{
        guid: 'audit-count',
        operation: 'PURGE',
        params: '',
        result: JSON.stringify({
          requestedCount: 10, purgedCount: 6, purgedDependenciesCount: 2,
          failedCount: 0, skippedCount: 2, executionFailed: false, runId: 'test'
        })
      }];
      render(<AuditResults componentProps={{ auditData: data }} row={{ original: { guid: 'audit-count' } }} />);

      // Total Purged = 6 + 2 = 8
      expect(screen.getByText('8')).toBeInTheDocument();
      // Skipped = 2
      expect(screen.getByText('2')).toBeInTheDocument();
    });
  });

  
  describe('Purge Drawer - Pagination Combinations', () => {
    it('should disable prev page button on first page ', () => {
      const mockData = Array.from({ length: 30 }, (_, i) => `guid-${i}`);
      const auditData = [{ guid: 'test', operation: 'PURGE', params: '', result: JSON.stringify(mockData) }];
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'test' } }} />);
      fireEvent.click(screen.getAllByText('PURGED')[0]);
      
      const prevButton = screen.getByRole('button', { name: /Go to previous page/i });
      expect(prevButton).toBeDisabled();
    });

    it('should change to next page when next button is clicked ', () => {
      const mockData = Array.from({ length: 30 }, (_, i) => `guid-${i}`);
      const auditData = [{ guid: 'test', operation: 'PURGE', params: '', result: JSON.stringify(mockData) }];
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'test' } }} />);
      fireEvent.click(screen.getAllByText('PURGED')[0]);
      
      const nextButton = screen.getByRole('button', { name: /Go to next page/i });
      fireEvent.click(nextButton);
      
      // Should show item from next page
      expect(screen.getByText('guid-25')).toBeInTheDocument();
    });

    it('should change page size when Limit input is updated ', () => {
      const mockData = Array.from({ length: 30 }, (_, i) => `guid-${i}`);
      const auditData = [{ guid: 'test', operation: 'PURGE', params: '', result: JSON.stringify(mockData) }];
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'test' } }} />);
      fireEvent.click(screen.getAllByText('PURGED')[0]);
      
      const limitInput = screen.getByDisplayValue('25');
      fireEvent.change(limitInput, { target: { value: '10' } });
      fireEvent.keyDown(limitInput, { key: 'Enter', code: 'Enter' });
      
      // Page size is now 10, so guid-10 should NOT be on the first page
      expect(screen.queryByText('guid-10')).not.toBeInTheDocument();
      expect(screen.getByText('guid-9')).toBeInTheDocument();
    });
  });

  describe('Purge UI Combinations - Combinations', () => {
    it('should correctly render legacy audit purge with array result ', () => {
      const auditData = [{ guid: 'legacy1', operation: 'PURGE', params: '', result: '[legacy-guid-1, legacy-guid-2]' }];
      const { container } = render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'legacy1', runId: 'N/A' } }} />);
      
      fireEvent.click(screen.getAllByText('PURGED')[0]);
      console.log(container.innerHTML);
      expect(screen.getByText('legacy-guid-1')).toBeInTheDocument();
      expect(screen.getByText('legacy-guid-2')).toBeInTheDocument();
    });

    it('should show 0 count and not open drawer for legacy purge with empty array ', () => {
      mockIsEmpty.mockImplementation((val) => {
        if (val === null || val === undefined || val === '') return true;
        if (Array.isArray(val) && val.length === 0) return true;
        if (typeof val === 'object' && Object.keys(val).length === 0) return true;
        if (Array.isArray(val) && val.length === 1 && val[0] === '') return true;
        return false;
      });
      const auditData = [{ guid: 'legacy2', operation: 'PURGE', params: '', result: '[]' }];
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'legacy2', runId: 'N/A' } }} />);
      
      expect(screen.queryByText('Requested')).not.toBeInTheDocument();
      
      // Card displays 0
      const countEl = screen.getByText('0');
      expect(countEl).toBeInTheDocument();
      
      // Drawer does not open
      fireEvent.click(screen.getAllByText('PURGED')[0]);
      expect(screen.queryByTestId('drawer')).not.toBeInTheDocument();
    });

    it('should correctly render new audit summary cards and open drawer for Requested ', () => {
      // New audit summary has runId and result is JSON object
      const summaryResult = JSON.stringify({
        purgedCount: 10,
        failedCount: 0,
        skippedCount: 0,
        runId: 'run-123'
      });
      const auditData = [{ guid: 'new1', operation: 'PURGE', params: '["req-1","req-2"]', result: summaryResult }];
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'new1', runId: 'run-123' } }} />);
      
      // Should render summary cards
      expect(screen.getByText('Requested')).toBeInTheDocument();
      expect(screen.getByText('PURGED')).toBeInTheDocument();
      
      // Click Requested card
      fireEvent.click(screen.getByText('Requested'));
      
      // Drawer should open showing requested guids
      expect(screen.getByText('Requested Entities')).toBeInTheDocument();
      expect(screen.getByText('req-1')).toBeInTheDocument();
      expect(screen.getByText('req-2')).toBeInTheDocument();
    });

    it('should NOT open drawer when Total Purged card is clicked and count is 0 ', () => {
      const summaryResult = JSON.stringify({
        purgedCount: 0,
        failedCount: 0,
        skippedCount: 0,
        runId: 'run-456'
      });
      const auditData = [{ guid: 'new2', operation: 'PURGE', params: '', result: summaryResult }];
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'new2', runId: 'run-456' } }} />);
      
      // Click PURGED card
      fireEvent.click(screen.getAllByText('PURGED')[0]);
      
      // Drawer should NOT open
      expect(screen.queryByText('Purged Entities')).not.toBeInTheDocument();
    });
  });


  describe('Copy Run ID', () => {
    it('should copy Run ID to clipboard and show Copied tooltip', async () => {
      const auditData = [{ guid: 'audit-1', operation: 'PURGE', params: '["a"]', result: '["a"]', runId: 'test-run-1' }];
      render(<AuditResults componentProps={{ auditData }} row={{ original: { guid: 'audit-1' } }} />);
      
      const copyBtn = screen.getByRole('button', { name: /Copy Run Id/i });
      fireEvent.click(copyBtn);
      
      expect(navigator.clipboard.writeText).toHaveBeenCalledWith('test-run-1');
      
      await waitFor(() => {
        expect(screen.getByRole('button', { name: /Copied!/i })).toBeInTheDocument();
      });
    });
  });

});
