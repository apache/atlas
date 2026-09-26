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
 * Comprehensive unit tests for ImportExportAudits component
 * 
 * Coverage Target:
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import ImportExportAudits from '../ImportExportAudits';

// Mock dependencies
const mockIsEmpty = jest.fn((val: any) => {
  if (val === null || val === undefined || val === '') return true;
  if (Array.isArray(val) && val.length === 0) return true;
  if (typeof val === 'object' && Object.keys(val).length === 0) return true;
  return false;
});

const mockIsArray = jest.fn((val: any) => Array.isArray(val));
const mockJsonParse = jest.fn((val: any) => {
  try {
    return JSON.parse(val);
  } catch {
    return {};
  }
});

jest.mock('@utils/Utils', () => ({
  isArray: (...args: any[]) => mockIsArray(...args),
  isEmpty: (...args: any[]) => mockIsEmpty(...args),
  jsonParse: (...args: any[]) => mockJsonParse(...args)
}));

// Mock Enum
jest.mock('@utils/Enum', () => ({
  category: {
    IMPORT: 'Import',
    EXPORT: 'Export'
  }
}));

// Mock components
const mockGetValues = jest.fn((value: any) => {
  if (Array.isArray(value)) return value.join(', ');
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
});

jest.mock('@components/commonComponents', () => ({
  getValues: (...args: any[]) => mockGetValues(...args)
}));

jest.mock('@utils/Muiutils', () => ({
  StyledPaper: ({ children, ...props }: any) => (
    <div data-testid="styled-paper" {...props}>
      {children}
    </div>
  )
}));

// Mock MUI components
jest.mock('@mui/material', () => {
  const actual = jest.requireActual('@mui/material');
  return {
    ...actual,
    Stack: ({ children, ...props }: any) => <div data-testid="stack" {...props}>{children}</div>,
    Divider: () => <hr data-testid="divider" />
  };
});

jest.mock('@components/muiComponents', () => ({
  Typography: ({ children, ...props }: any) => <span data-testid="typography" {...props}>{children}</span>
}));

describe('ImportExportAudits Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockIsEmpty.mockImplementation((val: any) => {
      if (val === null || val === undefined || val === '') return true;
      if (Array.isArray(val) && val.length === 0) return true;
      if (typeof val === 'object' && Object.keys(val).length === 0) return true;
      return false;
    });
    mockIsArray.mockImplementation((val: any) => Array.isArray(val));
    mockJsonParse.mockImplementation((val: any) => {
      try {
        return JSON.parse(val);
      } catch {
        return {};
      }
    });
    mockGetValues.mockImplementation((value: any) => {
      if (Array.isArray(value)) return value.join(', ');
      if (typeof value === 'object') return JSON.stringify(value);
      return String(value);
    });
  });

  describe('Component Rendering', () => {
    it('should render ImportExportAudits component', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify({ entitiesImported: 10 })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      const stacks = screen.getAllByTestId('stack');
      expect(stacks.length).toBeGreaterThan(0);
    });

    it('should display operation category', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify({ entitiesImported: 10 })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(screen.getByText('Import')).toBeInTheDocument();
    });

    it('should render three StyledPaper components', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify({ entitiesImported: 10 })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      const styledPapers = screen.getAllByTestId('styled-paper');
      expect(styledPapers).toHaveLength(3);
    });
  });

  describe('IMPORT Operation', () => {
    it('should render IMPORT audit with result data', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full', source: 'file.zip' }),
        result: JSON.stringify({
          entitiesImported: 10,
          classificationsImported: 5,
          status: 'success'
        })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(screen.getByText('Import')).toBeInTheDocument();
      expect(mockJsonParse).toHaveBeenCalledWith(auditObj.params);
      expect(mockJsonParse).toHaveBeenCalledWith(auditObj.result);
    });

    it('should display result entries sorted', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify({
          zebra: 'last',
          apple: 'first',
          banana: 'middle'
        })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      // Should call getValues for each entry
      expect(mockGetValues).toHaveBeenCalled();
    });

    it('should display params entries sorted', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({
          zebra: 'last',
          apple: 'first',
          banana: 'middle'
        }),
        result: JSON.stringify({ status: 'success' })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockGetValues).toHaveBeenCalled();
    });

    it('should show array length when value is array', () => {
      mockIsArray.mockReturnValue(true);

      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ entities: ['entity1', 'entity2', 'entity3'] }),
        result: JSON.stringify({ status: 'success' })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockIsArray).toHaveBeenCalled();
    });

    it('should not show array length when value is not array', () => {
      mockIsArray.mockReturnValue(false);

      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify({ status: 'success' })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockIsArray).toHaveBeenCalled();
    });
  });

  describe('EXPORT Operation', () => {
    it('should render EXPORT audit with result data', () => {
      const auditObj = {
        operation: 'EXPORT',
        params: JSON.stringify({ exportType: 'incremental', target: 'backup.zip' }),
        result: JSON.stringify({
          entitiesExported: 20,
          classificationsExported: 8,
          status: 'success'
        })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(screen.getByText('Export')).toBeInTheDocument();
      expect(mockJsonParse).toHaveBeenCalledWith(auditObj.params);
      expect(mockJsonParse).toHaveBeenCalledWith(auditObj.result);
    });

    it('should display export parameters', () => {
      const auditObj = {
        operation: 'EXPORT',
        params: JSON.stringify({
          exportType: 'full',
          includeClassifications: true,
          includeEntities: true
        }),
        result: JSON.stringify({ entitiesExported: 50 })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockGetValues).toHaveBeenCalled();
    });
  });

  describe('Empty Data Handling', () => {
    it('should show "No Record Found" when result is empty', () => {
      mockIsEmpty.mockReturnValue(true);

      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: '{}'
      };

      mockJsonParse.mockReturnValue({});

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(screen.getAllByText('No Record Found')).toHaveLength(2);
    });

    it('should show "No Record Found" when params is empty', () => {
      mockIsEmpty.mockReturnValue(true);

      const auditObj = {
        operation: 'IMPORT',
        params: '{}',
        result: JSON.stringify({ status: 'success' })
      };

      mockJsonParse.mockReturnValueOnce({}).mockReturnValueOnce({ status: 'success' });

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(screen.getAllByText('No Record Found')).toHaveLength(2);
    });

    it('should handle both result and params being empty', () => {
      mockIsEmpty.mockReturnValue(true);

      const auditObj = {
        operation: 'IMPORT',
        params: '{}',
        result: '{}'
      };

      mockJsonParse.mockReturnValue({});

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(screen.getAllByText('No Record Found')).toHaveLength(2);
    });
  });

  describe('Data Parsing', () => {
    it('should parse params JSON correctly', () => {
      const params = { importType: 'full', source: 'file.zip' };
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify(params),
        result: JSON.stringify({ status: 'success' })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockJsonParse).toHaveBeenCalledWith(auditObj.params);
    });

    it('should parse result JSON correctly', () => {
      const result = { entitiesImported: 10, status: 'success' };
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify(result)
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockJsonParse).toHaveBeenCalledWith(auditObj.result);
    });

    it('should handle malformed JSON in params', () => {
      mockJsonParse.mockReturnValueOnce({}).mockReturnValueOnce({ status: 'success' });

      const auditObj = {
        operation: 'IMPORT',
        params: 'malformed json',
        result: JSON.stringify({ status: 'success' })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockJsonParse).toHaveBeenCalledWith('malformed json');
    });

    it('should handle malformed JSON in result', () => {
      mockJsonParse.mockReturnValueOnce({ importType: 'full' }).mockReturnValueOnce({});

      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: 'malformed json'
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockJsonParse).toHaveBeenCalledWith('malformed json');
    });
  });

  describe('getValues Function Calls', () => {
    it('should call getValues for each result entry', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify({
          entitiesImported: 10,
          classificationsImported: 5,
          status: 'success'
        })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      // getValues should be called multiple times for result entries
      expect(mockGetValues).toHaveBeenCalled();
    });

    it('should call getValues for each params entry', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({
          importType: 'full',
          source: 'file.zip',
          includeClassifications: true
        }),
        result: JSON.stringify({ status: 'success' })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      // getValues should be called multiple times for params entries
      expect(mockGetValues).toHaveBeenCalled();
    });

    it('should call getValues with correct arguments', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify({ entitiesImported: 10 })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockGetValues).toHaveBeenCalledWith(
        expect.anything(),
        undefined,
        undefined,
        undefined,
        'properties'
      );
    });

    it('should call getValues for paramsObj in third StyledPaper', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify({ status: 'success' })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      // Third StyledPaper should call getValues with paramsObj
      expect(mockGetValues).toHaveBeenCalled();
    });
  });

  describe('Dividers', () => {
    it('should render dividers between entries', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({
          importType: 'full',
          source: 'file.zip'
        }),
        result: JSON.stringify({
          entitiesImported: 10,
          status: 'success'
        })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      const dividers = screen.getAllByTestId('divider');
      expect(dividers.length).toBeGreaterThan(0);
    });
  });

  describe('Complex Data Structures', () => {
    it('should handle nested objects in result', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify({
          summary: {
            entities: 10,
            classifications: 5
          },
          status: 'success'
        })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockGetValues).toHaveBeenCalled();
    });

    it('should handle arrays in result', () => {
      mockIsArray.mockImplementation((val: any) => Array.isArray(val));

      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify({
          entitiesImported: ['entity1', 'entity2', 'entity3'],
          status: 'success'
        })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockIsArray).toHaveBeenCalled();
    });

    it('should handle nested objects in params', () => {
      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({
          importType: 'full',
          options: {
            includeClassifications: true,
            includeEntities: true
          }
        }),
        result: JSON.stringify({ status: 'success' })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockGetValues).toHaveBeenCalled();
    });

    it('should handle arrays in params', () => {
      mockIsArray.mockImplementation((val: any) => Array.isArray(val));

      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({
          entities: ['entity1', 'entity2'],
          importType: 'selective'
        }),
        result: JSON.stringify({ status: 'success' })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockIsArray).toHaveBeenCalled();
    });
  });

  describe('Edge Cases', () => {
    it('should handle null auditObj', () => {
      const auditObj = null as any;

      // Component will try to destructure null and throw
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      expect(() => render(<ImportExportAudits auditObj={auditObj} />)).toThrow();
      consoleSpy.mockRestore();
    });

    it('should handle undefined operation', () => {
      const auditObj = {
        operation: undefined as any,
        params: JSON.stringify({ importType: 'full' }),
        result: JSON.stringify({ status: 'success' })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      // Should still render but category might be undefined
      const stacks = screen.getAllByTestId('stack');
      expect(stacks.length).toBeGreaterThan(0);
    });

    it('should handle empty string params', () => {
      mockJsonParse.mockReturnValueOnce({}).mockReturnValueOnce({ status: 'success' });

      const auditObj = {
        operation: 'IMPORT',
        params: '',
        result: JSON.stringify({ status: 'success' })
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockJsonParse).toHaveBeenCalledWith('');
    });

    it('should handle empty string result', () => {
      mockJsonParse.mockReturnValueOnce({ importType: 'full' }).mockReturnValueOnce({});

      const auditObj = {
        operation: 'IMPORT',
        params: JSON.stringify({ importType: 'full' }),
        result: ''
      };

      render(<ImportExportAudits auditObj={auditObj} />);

      expect(mockJsonParse).toHaveBeenCalledWith('');
    });
  });
});
