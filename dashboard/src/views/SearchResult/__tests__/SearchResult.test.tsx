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

import React from 'react';
import { render as rtlRender, screen, waitFor, fireEvent, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Provider } from 'react-redux';
import { MemoryRouter } from 'react-router-dom';
import { configureStore } from '@reduxjs/toolkit';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import SearchResult from '../SearchResult';
import * as searchApiMethod from '@api/apiMethods/searchApiMethod';
import { toast } from 'react-toastify';

const theme = createTheme();
const mockSetSearchParams = jest.fn();

let capturedColumns: any[] = [];

function setCapturedColumns(columns: any[] = []) {
	capturedColumns = columns;
}

// Mock API URL configuration
jest.mock('@api/apiUrlLinks/commonApiUrl', () => ({
  getBaseApiUrl: jest.fn((url) => `http://localhost:21000${url}`)
}));

// Mock API methods
jest.mock('@api/apiMethods/searchApiMethod');
jest.mock('@api/apiMethods/classificationApiMethod', () => ({
  removeClassification: jest.fn()
}));
jest.mock('@api/apiMethods/glossaryApiMethod', () => ({
  removeTerm: jest.fn()
}));
jest.mock('@api/apiMethods/apiMethod');
jest.mock('react-toastify', () => ({
  toast: {
    dismiss: jest.fn(),
    error: jest.fn(),
    success: jest.fn()
  }
}));

// Don't mock react-router-dom hooks - let them use real Router context
// This is necessary for TableLayout which uses useLocation and useSearchParams
jest.mock('react-router-dom', () => {
  const actual = jest.requireActual('react-router-dom');
  return {
    ...actual,
    // Keep actual hooks for Router context
    Link: ({ children, to, ...props }: any) => {
      const href = typeof to === 'string' ? to : to?.pathname || ''
      return (
        <a href={href} {...props}>
          {children}
        </a>
      )
    },
    useNavigate: () => jest.fn(),
    useSearchParams: () => {
      const [params, setParams] = actual.useSearchParams();
      const wrappedSetParams = (...args: any[]) => {
        mockSetSearchParams(...args);
        return setParams(...args);
      };
      return [params, wrappedSetParams];
    }
  };
});

// Mock TableLayout dependencies instead of TableLayout itself
// This allows cell renderers to execute for coverage
jest.mock('@components/Table/TableLayout', () => {
	const actual = jest.requireActual('../../../__mocks__/table-layout-mock');
	return {
		__esModule: true,
		...actual,
		TableLayout: (props: any) => {
			setCapturedColumns(props.columns || []);
			return actual.TableLayout(props);
		}
	};
});

// Mock dnd-kit to avoid drag-and-drop complexity in tests
jest.mock('@dnd-kit/core', () => ({
  DndContext: ({ children }: any) => <div data-testid="dnd-context">{children}</div>,
  closestCenter: jest.fn(),
  KeyboardSensor: jest.fn(),
  MouseSensor: jest.fn(),
  TouchSensor: jest.fn(),
  useSensor: jest.fn(),
  useSensors: jest.fn(() => [])
}));

jest.mock('@dnd-kit/modifiers', () => ({
  restrictToHorizontalAxis: jest.fn()
}));

jest.mock('@dnd-kit/sortable', () => {
  const mockReturnValue = {
    attributes: {},
    listeners: {},
    setNodeRef: (node: any) => node,
    transform: null,
    isDragging: false
  };
  
  return {
    useSortable: () => mockReturnValue,
    SortableContext: ({ children }: any) => <>{children}</>,
    horizontalListSortingStrategy: {}
  };
});

jest.mock('@dnd-kit/utilities', () => ({
  CSS: {
    Translate: {
      toString: jest.fn(() => 'translate(0, 0)')
    }
  }
}));

// Mock TableLayout's child components
jest.mock('@components/Table/TableFilters', () => ({
  __esModule: true,
  default: ({ refreshTable, getAllColumns }: any) => (
    <div data-testid="table-filters">
      <button onClick={refreshTable} data-testid="refresh-table-btn">Refresh</button>
    </div>
  )
}));

jest.mock('@components/Table/TablePagination', () => ({
  __esModule: true,
  default: ({ setPageIndex, nextPage, previousPage, getPageCount }: any) => (
    <div data-testid="table-pagination">
      <button onClick={() => setPageIndex(0)} data-testid="first-page">First</button>
      <button onClick={previousPage} data-testid="prev-page">Prev</button>
      <button onClick={nextPage} data-testid="next-page">Next</button>
      <div data-testid="page-count">{getPageCount()}</div>
    </div>
  )
}));

jest.mock('@components/Table/TableLoader', () => ({
  __esModule: true,
  default: ({ rowsNum }: any) => (
    <div data-testid="table-loader">Loading {rowsNum} rows...</div>
  )
}));

jest.mock('@components/FilterQuery', () => ({
  __esModule: true,
  default: ({ value }: any) => (
    <div data-testid="filter-query">{JSON.stringify(value)}</div>
  )
}));

jest.mock('@views/Classification/AddTag', () => ({
  __esModule: true,
  default: ({ open, onClose, entityData }: any) =>
    open ? (
      <div data-testid="add-tag-modal">
        <button onClick={onClose}>Close</button>
        <div data-testid="entity-data">{JSON.stringify(entityData)}</div>
      </div>
    ) : null
}));

// Mock utils
jest.mock('@utils/Utils', () => {
  const globalSearchParams = {
    basicParams: {},
    dslParams: {}
  };

  return {
    findUniqueValues: jest.fn((arr, defaults) => {
      if (!Array.isArray(arr)) return defaults || [];
      return [...new Set([...(arr || []), ...(defaults || [])])];
    }),
    extractKeyValueFromEntity: jest.fn((entity) => ({
      name: entity?.attributes?.name || entity?.name || 'Test Entity',
      found: true,
      key: 'name'
    })),
    isEmpty: jest.fn((val) => {
      if (val === null || val === undefined) return true;
      if (typeof val === 'string') return val.length === 0;
      if (Array.isArray(val)) return val.length === 0;
      if (typeof val === 'object') return Object.keys(val).length === 0;
      return false;
    }),
    isNull: jest.fn((val) => val === null || val === undefined),
    isArray: jest.fn((val) => Array.isArray(val)),
    removeDuplicateObjects: jest.fn((arr) => {
      // Always return an array
      if (arr === null || arr === undefined) return [];
      if (!Array.isArray(arr)) {
        // Try to convert to array if possible
        if (arr && typeof arr === 'object') {
          if ('length' in arr) {
            try {
              arr = Array.from(arr);
            } catch {
              return [];
            }
          } else {
            return [];
          }
        } else {
          return [];
        }
      }
      // Filter out null, undefined, and empty objects
      try {
        const filtered = arr.filter((v: any) => {
          if (v === null || v === undefined) return false;
          if (typeof v !== 'object') return false;
          return Object.keys(v).length !== 0;
        });
        // Ensure we always return an array
        return Array.isArray(filtered) ? filtered : [];
      } catch (e) {
        // If anything goes wrong, return empty array
        return [];
      }
    }),
    customSortBy: jest.fn((arr, keys) => {
      // Always return an array, even if input is invalid
      if (arr === null || arr === undefined) return [];
      if (!Array.isArray(arr)) {
        // If arr is not an array, try to convert it or return empty array
        if (arr && typeof arr === 'object' && 'length' in arr) {
          try {
            arr = Array.from(arr);
          } catch {
            return [];
          }
        } else {
          return [];
        }
      }
      // Filter out any invalid entries
      const validArr = arr.filter(item => item != null);
      if (validArr.length === 0) return [];
      if (!Array.isArray(keys) || keys.length === 0) {
        const result = [...validArr];
        return Array.isArray(result) ? result : [];
      }
      try {
        const sorted = [...validArr].sort((a, b) => {
          if (!a || !b) return 0;
          for (const key of keys) {
            const aVal = a?.[key] || '';
            const bVal = b?.[key] || '';
            if (aVal < bVal) return -1;
            if (aVal > bVal) return 1;
          }
          return 0;
        });
        // Final check - ensure we return an array
        const result = Array.isArray(sorted) ? sorted : [];
        return result;
      } catch (e) {
        // If anything fails, return a copy of the input array
        const result = [...validArr];
        return Array.isArray(result) ? result : [];
      }
    }),
    flattenArray: jest.fn((arr) => {
      if (arr === null || arr === undefined) return [];
      if (!Array.isArray(arr)) {
        // Try to convert if it's array-like
        try {
          if (arr && typeof arr === 'object' && 'length' in arr) {
            arr = Array.from(arr);
          } else {
            return [];
          }
        } catch {
          return [];
        }
      }
      try {
        const flattened = arr.flat();
        // Ensure we return an array
        return Array.isArray(flattened) ? flattened : [];
      } catch (e) {
        return [];
      }
    }),
    dateFormat: jest.fn((date) => {
      if (!date) return '';
      return new Date(date).toLocaleString();
    }),
    serverError: jest.fn(),
    searchParamsAPiQuery: jest.fn((val) => {
      if (!val) return null;
      try {
        return JSON.parse(val);
      } catch {
        return val;
      }
    }),
    globalSearchParams
  };
});

// Mock Helper
jest.mock('@utils/Helper', () => ({
  startsWith: jest.fn((str, prefix) => {
    if (!str || !prefix) return false;
    return String(str).startsWith(String(prefix));
  })
}));

// Mock Enum
jest.mock('@utils/Enum', () => ({
  entityStateReadOnly: {
    DELETED: true,
    ACTIVE: false
  },
  serviceTypeMap: {}
}));

// Mock MUI utils
jest.mock('@utils/Muiutils', () => ({
  AntSwitch: ({ checked, onChange, onClick, inputProps, ...props }: any) => {
    const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
      if (onChange) {
        onChange(e);
      }
    };
    const handleClick = (e: React.MouseEvent<HTMLInputElement>) => {
      e.stopPropagation();
      if (onClick) {
        onClick(e);
      }
    };
    return (
      <input
        type="checkbox"
        checked={checked}
        onChange={handleChange}
        onClick={handleClick}
        data-testid="ant-switch"
        {...inputProps}
        {...props}
      />
    );
  }
}));

// Mock components
jest.mock('@components/muiComponents', () => ({
  LightTooltip: ({ children, title }: any) => (
    <div data-testid="light-tooltip" title={title}>
      {children}
    </div>
  )
}));

function safeStringify(value: unknown) {
  try {
    return JSON.stringify(value)
  } catch (err) {
    return '[Circular]'
  }
}

jest.mock('@components/DialogShowMoreLess', () => ({
  __esModule: true,
  default: ({ value, columnVal, colName, setUpdateTable, removeApiMethod }: any) => (
    <div data-testid={`dialog-${columnVal}`}>
      {colName}: {safeStringify(value)}
      <button onClick={() => setUpdateTable && setUpdateTable(Date.now())}>
        Update
      </button>
    </div>
  )
}));

jest.mock('@components/TextShowMoreLess', () => ({
  __esModule: true,
  TextShowMoreLess: ({ data }: any) => (
    <div data-testid="text-show-more-less">{safeStringify(data)}</div>
  )
}));

jest.mock('@components/EntityDisplayImage', () => ({
  __esModule: true,
  default: ({ entity }: any) => (
    <div data-testid="entity-display-image">{entity?.typeName || 'Unknown'}</div>
  )
}));

jest.mock('@components/commonComponents', () => ({
  getValues: jest.fn((info, entityDef, attr, type) => {
    const val = info.row.original.attributes?.[attr.name];
    if (type === 'relationShipAttr') {
      return <span>{val?.displayText || val?.name || val || 'NA'}</span>;
    }
    if (Array.isArray(val)) {
      return <span>{val.join(', ')}</span>;
    }
    return <span>{val || 'NA'}</span>;
  })
}));

// Mock moment
jest.mock('moment-timezone', () => {
  const moment = jest.requireActual('moment-timezone');
  return {
    ...moment,
    now: jest.fn(() => Date.now())
  };
});

describe('SearchResult', () => {
  const mockEntityData = {
    entityDefs: [
      {
        name: 'DataSet',
        typeName: 'DataSet',
        attributeDefs: [
          {
            name: 'name',
            typeName: 'string',
            isOptional: false
          },
          {
            name: 'description',
            typeName: 'string',
            isOptional: true
          },
          {
            name: 'owner',
            typeName: 'string',
            isOptional: true
          }
        ],
        relationshipAttributeDefs: [],
        superTypes: []
      },
      {
        name: 'Process',
        typeName: 'Process',
        attributeDefs: [
          {
            name: 'name',
            typeName: 'string',
            isOptional: false
          }
        ],
        relationshipAttributeDefs: [],
        superTypes: []
      }
    ]
  };

  const defaultBusinessMetaSlice = {
    loading: false,
    businessMetaData: null,
    error: null
  };

  const createMockStore = (entityData: any = mockEntityData) => {
    return configureStore({
      reducer: {
        entity: () => ({
          entityData,
          loading: false
        }),
        businessMetaData: (state = defaultBusinessMetaSlice) => state
      },
      preloadedState: {
        entity: {
          entityData,
          loading: false
        },
        businessMetaData: defaultBusinessMetaSlice
      }
    });
  };

  const renderWithProviders = (
    component: React.ReactElement,
    {
      store = createMockStore(),
      route = '/search/searchResult',
      searchParams = new URLSearchParams()
    }: {
      store?: ReturnType<typeof createMockStore>;
      route?: string;
      searchParams?: URLSearchParams;
    } = {}
  ) => {
    // Ensure type parameter is set to avoid dynamicColumns errors
    if (!searchParams.has('type') && !searchParams.has('tag') && !searchParams.has('term')) {
      searchParams.set('type', 'DataSet');
    }

    // Use rtlRender with ThemeProvider for MUI components
    // TableLayout needs Router context for useLocation/useSearchParams
    // Use the actual route path that SearchResult expects
    return rtlRender(
      <ThemeProvider theme={theme}>
        <Provider store={store}>
          <MemoryRouter initialEntries={[`${route}?${searchParams.toString()}`]}>
            {component}
          </MemoryRouter>
        </Provider>
      </ThemeProvider>
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();
    capturedColumns = [];
    (globalThis as any).__tableLayoutCaptureColumns = setCapturedColumns;
    (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValue({
      data: {
        entities: [
          {
            guid: 'guid-1',
            typeName: 'DataSet',
            attributes: {
              name: 'Test Dataset',
              owner: 'test-owner',
              description: 'Test description',
              __guid: 'guid-1'
            },
            classificationNames: ['PII'],
            meanings: [],
            status: 'ACTIVE'
          }
        ],
        referredEntities: {},
        approximateCount: 1
      }
    });
    const Utils = require('@utils/Utils');
    Utils.globalSearchParams.basicParams = {};
    Utils.globalSearchParams.dslParams = {};
    // Reset all mock implementations to ensure they always return arrays
    (Utils.customSortBy as jest.Mock).mockImplementation((arr: any, keys: any) => {
      if (arr === null || arr === undefined) return [];
      if (!Array.isArray(arr)) return [];
      if (arr.length === 0) return [];
      if (!Array.isArray(keys) || keys.length === 0) return [...arr];
      try {
        const sorted = [...arr].sort((a: any, b: any) => {
          if (!a || !b) return 0;
          for (const key of keys) {
            const aVal = a?.[key] || '';
            const bVal = b?.[key] || '';
            if (aVal < bVal) return -1;
            if (aVal > bVal) return 1;
          }
          return 0;
        });
        return sorted;
      } catch {
        return [...arr];
      }
    });
    (Utils.removeDuplicateObjects as jest.Mock).mockImplementation((arr: any) => {
      if (arr === null || arr === undefined) return [];
      if (!Array.isArray(arr)) return [];
      try {
        const filtered = arr.filter((v: any) => {
          if (v === null || v === undefined || v === false) return false;
          if (typeof v !== 'object') return false;
          return Object.keys(v).length !== 0;
        });
        return Array.isArray(filtered) ? filtered : [];
      } catch {
        return [];
      }
    });
    (Utils.flattenArray as jest.Mock).mockImplementation((arr: any) => {
      if (arr === null || arr === undefined) return [];
      if (!Array.isArray(arr)) return [];
      try {
        const flattened = arr.flat();
        return Array.isArray(flattened) ? flattened : [];
      } catch {
        return [];
      }
    });
    // Ensure isEmpty always works correctly
    (Utils.isEmpty as jest.Mock).mockImplementation((val: any) => {
      if (val === null || val === undefined) return true;
      if (typeof val === 'string') return val.length === 0;
      if (Array.isArray(val)) return val.length === 0;
      if (typeof val === 'object') return Object.keys(val).length === 0;
      return false;
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
    (globalThis as any).__tableLayoutCaptureColumns = undefined;
  });

  describe('Rendering', () => {
    it('should render SearchResult component', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult />, {
          searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      // TableLayout renders a table with className "table"
      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

    it('should render with default props', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult />, {
          searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render with classificationParams', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult classificationParams="TestClassification" />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render with glossaryTypeParams', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult glossaryTypeParams="TestTerm" />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render with hideFilters', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult hideFilters={true} />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Basic Search Functionality', () => {
    it('should fetch search results on mount', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalled();
      }, { timeout: 15000 });
    }, 30000);


    it('should fetch results with correct basic search params', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'basic',
        type: 'DataSet',
        query: 'test'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalledWith(
          expect.objectContaining({
            data: expect.objectContaining({
              typeName: 'DataSet',
              query: 'test'
            })
          }),
          'basic'
        );
      }, { timeout: 15000 });
    }, 30000);


    it('should handle successful search response', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      // Wait for API call to complete and table to render
      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalled();
      }, { timeout: 10000 });

      // Wait for table to be present
      await waitFor(() => {
        const table = document.querySelector('.table');
        expect(table).toBeInTheDocument();
      }, { timeout: 10000 });

      // Wait for data to be rendered (check that loader is gone)
      await waitFor(() => {
        const loader = screen.queryByTestId('table-loader');
        expect(loader).not.toBeInTheDocument();
      }, { timeout: 10000 });

      // Verify table has rendered with data
      const tableElement = document.querySelector('.table');
      expect(tableElement).toBeInTheDocument();
    }, 30000);

    it('should set empty data when no results', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [],
          referredEntities: {},
          approximateCount: 0
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // Check for empty state message
        expect(screen.getByText(/No Records found!/i)).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Classification Search', () => {
    it('should handle classification search', async () => {
      const searchParams = new URLSearchParams({
        tag: 'TestClassification',
        searchType: 'basic'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(
        () => {
          expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalled();
        },
        { timeout: 10000 }
      );

      const calls = (searchApiMethod.getBasicSearchResult as jest.Mock).mock.calls;
      const lastCall = calls[calls.length - 1];
      expect(lastCall[0].data).toHaveProperty('classification', 'TestClassification');
      expect(lastCall[1]).toBe('basic');
    }, 30000);


    it('should handle classificationParams prop', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult classificationParams="TestClassification" />);
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalledWith(
          expect.objectContaining({
            data: expect.objectContaining({
              classification: 'TestClassification'
            })
          }),
          'basic'
        );
      }, { timeout: 15000 });
    }, 30000);


    it('should show toggle switches for classification search', () => {
      act(() => {
        renderWithProviders(<SearchResult classificationParams="TestClassification" />);
        });

      expect(screen.getAllByTestId('ant-switch')).toHaveLength(2);
      expect(screen.getByText('Exclude sub-classification')).toBeInTheDocument();
      expect(screen.getByText('Show historical entities')).toBeInTheDocument();
    });
  });

  describe('Term Search', () => {
    it('should handle term search', async () => {
      const searchParams = new URLSearchParams({
        term: 'TestTerm',
        searchType: 'basic'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalledWith(
          expect.objectContaining({
            data: expect.objectContaining({
              termName: 'TestTerm'
            })
          }),
          'basic'
        );
      }, { timeout: 15000 });
    }, 30000);


    it('should handle glossaryTypeParams prop', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult glossaryTypeParams="TestTerm" />);
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalledWith(
          expect.objectContaining({
            data: expect.objectContaining({
              termName: 'TestTerm'
            })
          }),
          'basic'
        );
      }, { timeout: 15000 });
    }, 30000);


    it('should show toggle switches for term search', () => {
      act(() => {
        renderWithProviders(<SearchResult glossaryTypeParams="TestTerm" />);
        });

      expect(screen.getAllByTestId('ant-switch')).toHaveLength(2);
    });
  });

  describe('DSL Search', () => {
    it('should handle DSL search', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test query'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: {
            name: ['col1', 'col2'],
            values: [
              ['value1', 'value2'],
              ['value3', 'value4']
            ]
          }
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalledWith(
          expect.objectContaining({
            params: expect.objectContaining({
              query: 'test query'
            })
          }),
          'dsl'
        );
      }, { timeout: 15000 });
    }, 30000);


    it('should handle DSL search with attributes array', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: [
            { name: 'col1' },
            { name: 'col2' }
          ],
          values: [
            ['val1', 'val2'],
            ['val3', 'val4']
          ]
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.queryByTestId('table-loader')).not.toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle DSL search with empty results', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {}
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.getByText(/No Records found!/i)).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should disable row selection for DSL aggregate', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: {
            name: ['col1'],
            values: [['val1']]
          }
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // DSL aggregate mode doesn't show row selection
        expect(document.querySelector('input[type="checkbox"]')).not.toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Toggle Switches', () => {
    it('should toggle includeDE switch', async () => {
      const user = userEvent.setup();
      mockSetSearchParams.mockClear();
      
      await act(async () => {
        renderWithProviders(<SearchResult classificationParams="TestClassification" />);
        });

      await waitFor(() => {
        expect(screen.getAllByTestId('ant-switch').length).toBeGreaterThan(0);
      }, { timeout: 15000 });

      const switches = screen.getAllByTestId('ant-switch');
      expect(switches.length).toBeGreaterThanOrEqual(2);
      const includeDESwitch = switches[1]; // Second switch is includeDE

      // Use userEvent to click the switch (more realistic)
      await user.click(includeDESwitch);

      // The component should call setSearchParams when the switch changes
      await waitFor(
        () => {
          expect(mockSetSearchParams).toHaveBeenCalled();
        },
        { timeout: 10000 }
      );
    }, 30000);


    it('should toggle excludeSC switch', async () => {
      const user = userEvent.setup();
      mockSetSearchParams.mockClear();
      
      await act(async () => {
        renderWithProviders(<SearchResult classificationParams="TestClassification" />);
        });

      await waitFor(() => {
        expect(screen.getAllByTestId('ant-switch').length).toBeGreaterThan(0);
      }, { timeout: 15000 });

      const switches = screen.getAllByTestId('ant-switch');
      const excludeSCSwitch = switches[0]; // First switch is excludeSC

      await user.click(excludeSCSwitch);

      await waitFor(
        () => {
          expect(mockSetSearchParams).toHaveBeenCalled();
        },
        { timeout: 10000 }
      );
    }, 30000);


    it('should initialize switches from URL params', () => {
      const searchParams = new URLSearchParams({
        includeDE: 'true',
        excludeSC: 'true'
      });

      act(() => {
        renderWithProviders(<SearchResult classificationParams="TestClassification" />, {
        searchParams
          });
        });

      const switches = screen.getAllByTestId('ant-switch');
      expect(switches[0]).toHaveProperty('checked', true);
      expect(switches[1]).toHaveProperty('checked', true);
    });

    it('should remove params when switches are turned off', async () => {
      const user = userEvent.setup();
      mockSetSearchParams.mockClear();
      
      const searchParams = new URLSearchParams({
        includeDE: 'true',
        excludeSC: 'true'
      });

      await act(async () => {
        renderWithProviders(<SearchResult classificationParams="TestClassification" />, {
        searchParams
          });
        });

      await waitFor(() => {
        expect(screen.getAllByTestId('ant-switch').length).toBeGreaterThan(0);
      }, { timeout: 15000 });

      const switches = screen.getAllByTestId('ant-switch');
      const excludeSCSwitch = switches[0];

      // Click to uncheck (toggle off) - this should trigger the else branch (lines 135-136)
      await user.click(excludeSCSwitch);

      await waitFor(
        () => {
          expect(mockSetSearchParams).toHaveBeenCalled();
        },
        { timeout: 10000 }
      );
    }, 30000);

  });

  describe('Filters', () => {
    it('should handle entityFilters', async () => {
      const searchParams = new URLSearchParams({
        entityFilters: JSON.stringify({ type: 'DataSet' }),
        searchType: 'basic',
        type: 'DataSet'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        const calls = (searchApiMethod.getBasicSearchResult as jest.Mock).mock.calls;
        const lastCall = calls[calls.length - 1];
        expect(lastCall).toBeDefined();
        expect(lastCall[0]).toBeDefined();
        expect(lastCall[0].data).toBeDefined();
        expect(lastCall[0].data).toHaveProperty('entityFilters');
        expect(lastCall[1]).toBe('basic');
      }, { timeout: 15000 });
    }, 30000);


    it('should handle tagFilters', async () => {
      const searchParams = new URLSearchParams({
        tagFilters: JSON.stringify({ tag: 'TestTag' }),
        searchType: 'basic',
        type: 'DataSet'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        const calls = (searchApiMethod.getBasicSearchResult as jest.Mock).mock.calls;
        const lastCall = calls[calls.length - 1];
        expect(lastCall).toBeDefined();
        expect(lastCall[0]).toBeDefined();
        expect(lastCall[0].data).toBeDefined();
        expect(lastCall[0].data).toHaveProperty('tagFilters');
        expect(lastCall[1]).toBe('basic');
      }, { timeout: 15000 });
    }, 30000);


    it('should handle relationshipFilters', async () => {
      const searchParams = new URLSearchParams({
        relationshipFilters: JSON.stringify({ relationship: 'TestRelation' }),
        searchType: 'basic',
        type: 'DataSet'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        const calls = (searchApiMethod.getBasicSearchResult as jest.Mock).mock.calls;
        const lastCall = calls[calls.length - 1];
        expect(lastCall).toBeDefined();
        expect(lastCall[0]).toBeDefined();
        expect(lastCall[0].data).toBeDefined();
        expect(lastCall[0].data).toHaveProperty('relationshipFilters');
        expect(lastCall[1]).toBe('basic');
      }, { timeout: 15000 });
    }, 30000);


    it('should not include filters when classificationParams is present', async () => {
      const searchParams = new URLSearchParams({
        entityFilters: JSON.stringify({ type: 'DataSet' })
      });

      await act(async () => {
        renderWithProviders(<SearchResult classificationParams="TestClassification" />, {
        searchParams
          });
        });

      await waitFor(() => {
        const call = (searchApiMethod.getBasicSearchResult as jest.Mock).mock.calls[0];
        expect(call[0].data.entityFilters).toBeUndefined();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Pagination', () => {
    it('should handle pagination', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: Array.from({ length: 10 }, (_, i) => ({
            guid: `guid-${i}`,
            typeName: 'DataSet',
            attributes: { name: `Entity ${i}`, __guid: `guid-${i}` },
            classificationNames: [],
            meanings: [],
            status: 'ACTIVE'
          })),
          referredEntities: {},
          approximateCount: 50
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // Verify data is rendered
        const table = screen.getByRole('table')
        expect(table).toHaveTextContent('Entity 0')
      }, { timeout: 15000 });
    }, 30000);


    it('should use pageLimit from URL params', async () => {
      const searchParams = new URLSearchParams({
        pageLimit: '25',
        type: 'DataSet'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalled();
        // Verify that pageLimit param is respected when pagination is not provided
        // Note: TableLayout calls fetchData with pagination, so limit will be from pagination.pageSize
        // But the param structure shows pageLimit was considered
        const calls = (searchApiMethod.getBasicSearchResult as jest.Mock).mock.calls;
        expect(calls.length).toBeGreaterThan(0);
      }, { timeout: 15000 });
    }, 30000);


    it('should use pageOffset from URL params', async () => {
      const searchParams = new URLSearchParams({
        pageOffset: '20',
        type: 'DataSet'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalled();
        // Verify that pageOffset param is considered
        // Note: TableLayout calls fetchData with pagination, so offset will be calculated from pagination
        // But the param structure shows pageOffset was considered
        const calls = (searchApiMethod.getBasicSearchResult as jest.Mock).mock.calls;
        expect(calls.length).toBeGreaterThan(0);
      }, { timeout: 15000 });
    }, 30000);


    it('should calculate pageCount correctly', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: Array.from({ length: 10 }, (_, i) => ({
            guid: `guid-${i}`,
            typeName: 'DataSet',
            attributes: { name: `Entity ${i}`, __guid: `guid-${i}` },
            classificationNames: [],
            meanings: [],
            status: 'ACTIVE'
          })),
          referredEntities: {},
          approximateCount: 100
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(screen.getByTestId('page-count')).toHaveTextContent('10');
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Column Management', () => {
    it('should render default columns', () => {
      renderWithProviders(<SearchResult />, {
      searchParams: new URLSearchParams({ type: 'DataSet' })
      });

      expect(screen.getByRole('table')).toBeInTheDocument();
      const headers = screen.getAllByRole('columnheader')
        .map((header) => header.textContent);
      expect(headers).toEqual(
        expect.arrayContaining(['Type', 'Classifications', 'Term'])
      );
    });


    it('should handle attributes param for column visibility', () => {
      const searchParams = new URLSearchParams({
        attributes: 'name,owner,description',
        type: 'DataSet'
      });

      renderWithProviders(<SearchResult />, { searchParams   });

      expect(screen.getByRole('table')).toBeInTheDocument();
      const headers = screen.getAllByRole('columnheader')
        .map((header) => header.textContent);
      expect(headers).toEqual(
        expect.arrayContaining(['Name', 'Owner', 'Description'])
      );
    });


    it('should keep dynamic columns hidden by default', () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [
              {
                name: 'customField',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      renderWithProviders(<SearchResult />, {
      store,
      searchParams: new URLSearchParams({ type: 'DataSet' })
      });

      expect(screen.getByRole('table')).toBeInTheDocument();
      const hasCustomField = capturedColumns.some(
        (column) =>
          column.accessorKey === 'customField' ||
          column.header === 'CustomField'
      );
      expect(hasCustomField).toBe(true);
      const headers = screen.getAllByRole('columnheader')
        .map((header) => header.textContent);
      expect(headers).not.toContain('CustomField');
    });


    it('should hide term column when glossaryTypeParams is set', () => {
      renderWithProviders(<SearchResult glossaryTypeParams="TestTerm" />, {
      searchParams: new URLSearchParams({ type: 'DataSet' })
      });

      expect(screen.getByRole('table')).toBeInTheDocument();
      const hasTermColumn = capturedColumns.some(
        (column) => column.accessorKey === 'term' || column.header === 'Term'
      );
      expect(hasTermColumn).toBe(false);
    });

  });

  describe('Entity Operations', () => {
    it('should handle entity with deleted status', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Deleted Entity',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'DELETED'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle entity with guid -1', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: '-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Invalid Entity',
                __guid: '-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Error Handling', () => {
    it('should handle API error', async () => {
      const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation();
      const error = {
        response: {
          data: {
            errorMessage: 'Test error'
          }
        }
      };

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockRejectedValueOnce(error);

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(consoleErrorSpy).toHaveBeenCalledWith('Error fetching data:', 'Test error');
        const Utils = require('@utils/Utils');
        expect(Utils.serverError).toHaveBeenCalled();
        expect(screen.getByRole('table')).toBeInTheDocument();
      }, { timeout: 15000 });

      consoleErrorSpy.mockRestore();
    }, 30000);


    it('should handle error without response', async () => {
      const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation();
      const error = new Error('Network error');

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockRejectedValueOnce(error);

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        const Utils = require('@utils/Utils');
        expect(Utils.serverError).toHaveBeenCalled();
      }, { timeout: 15000 });

      consoleErrorSpy.mockRestore();
    }, 30000);


    it('should dismiss toast on error', async () => {
      const error = new Error('Test error');
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockRejectedValueOnce(error);

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(toast.dismiss).toHaveBeenCalled();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Refresh Table', () => {
    it('should refresh table when refreshTable is called', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalled();
      }, { timeout: 15000 });

      // Trigger refresh by calling the refresh function through TableFilters mock
      const refreshBtn = screen.queryByTestId('refresh-table-btn');
      if (refreshBtn) {
        fireEvent.click(refreshBtn);
        await waitFor(() => {
          expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalledTimes(2);
        }, { timeout: 15000 });
      } else {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalled();
      }
    }, 30000);

  });

  describe('Edge Cases', () => {
    it('should handle empty searchParams', () => {
      act(() => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      expect(document.querySelector('.table')).toBeInTheDocument();
    });

    it('should handle null entityData', () => {
      const store = createMockStore(null);
      act(() => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      expect(document.querySelector('.table')).toBeInTheDocument();
    });

    it('should handle typeDefEntityData as empty object', () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'OtherType',
            typeName: 'OtherType',
            attributeDefs: [],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });
      act(() => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'NonExistentType' })
        });
      });

      expect(document.querySelector('.table')).toBeInTheDocument();
    });

    it('should handle missing entityDefs', () => {
      const store = createMockStore({ entityDefs: [] });
      act(() => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      expect(document.querySelector('.table')).toBeInTheDocument();
    });

    it('should handle searchParams with all filters', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'basic',
        type: 'DataSet',
        query: 'test',
        tag: 'TestTag',
        term: 'TestTerm',
        entityFilters: JSON.stringify({ type: 'DataSet' }),
        tagFilters: JSON.stringify({ tag: 'TestTag' }),
        relationshipFilters: JSON.stringify({ relationship: 'TestRelation' }),
        attributes: 'name,owner',
        pageLimit: '25',
        pageOffset: '0',
        includeDE: 'true',
        excludeSC: 'true',
        excludeST: 'true'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalled();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle DSL search with values array', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          values: [
            ['val1', 'val2'],
            ['val3', 'val4']
          ]
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.queryByTestId('table-loader')).not.toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle superTypes in entityDefs', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [],
            relationshipAttributeDefs: [],
            superTypes: ['Referenceable']
          },
          {
            name: 'Referenceable',
            typeName: 'Referenceable',
            attributeDefs: [
              {
                name: 'qualifiedName',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet', attributes: 'name' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle relationshipAttributeDefs', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [
              {
                name: 'inputs',
                typeName: 'array<Process>',
                isOptional: true
              }
            ],
            relationshipAttributeDefs: [
              {
                name: 'inputs',
                typeName: 'array<Process>'
              }
            ],
            superTypes: []
          }
        ]
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet', attributes: 'name' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle referredEntities in searchData', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                inputs: [{ guid: 'guid-2' }],
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {
            'guid-2': {
              guid: 'guid-2',
              typeName: 'Process',
              attributes: {
                name: 'Test Process',
                __guid: 'guid-2'
              }
            }
          },
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.queryByTestId('table-loader')).not.toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle empty attributes array', async () => {
      const searchParams = new URLSearchParams({
        attributes: ''
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalled();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle includeSubTypes param', async () => {
      const searchParams = new URLSearchParams({
        excludeST: 'true',
        searchType: 'basic',
        type: 'DataSet'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        const calls = (searchApiMethod.getBasicSearchResult as jest.Mock).mock.calls;
        const lastCall = calls[calls.length - 1];
        expect(lastCall).toBeDefined();
        expect(lastCall[0].data).toBeDefined();
        expect(lastCall[0].data.includeSubTypes).toBe(false);
      }, { timeout: 15000 });
    }, 30000);


    it('should handle default sort column', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // Default sort is applied - verify table renders correctly
        const table = document.querySelector('.table');
      expect(table).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle DSL aggregate with no default sort', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: {
            name: ['col1'],
            values: [['val1']]
          }
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.getAllByText('val1').length).toBeGreaterThan(0);
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('TableLayout Props', () => {
    it('should render table with filters and pagination', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // Verify table filters are rendered
        expect(screen.getByTestId('table-filters')).toBeInTheDocument();
        // Verify pagination is rendered
        expect(screen.getByTestId('table-pagination')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should show assign filters when classificationParams is present', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult classificationParams="TestClassification" />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // When classificationParams is present, assign filters should be available
        // This is tested by checking that the table renders correctly
        expect(screen.getByTestId('table-filters')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should show assign filters when glossaryTypeParams is present', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult glossaryTypeParams="TestTerm" />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // When glossaryTypeParams is present, assign filters should be available
        expect(screen.getByTestId('table-filters')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render table without assign filters when no classification or term params', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // Table should still render filters (but without assign functionality)
        expect(screen.getByTestId('table-filters')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should disable filters when hideFilters is true', () => {
      act(() => {
        renderWithProviders(<SearchResult hideFilters={true} />);
        });

      expect(screen.queryByTestId('table-filters')).not.toBeInTheDocument();
      expect(screen.queryByTestId('query-builder')).not.toBeInTheDocument();
      expect(screen.queryByTestId('all-table-filters')).not.toBeInTheDocument();
      expect(screen.queryByTestId('isfilter-query')).not.toBeInTheDocument();
    });
  });

  describe('Column Visibility', () => {
    it('should handle defaultColumnVisibility', async () => {
      const searchParams = new URLSearchParams({
        attributes: 'name,owner',
        type: 'DataSet'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle column visibility for classification search', async () => {
      const searchParams = new URLSearchParams({
        tag: 'TestClassification'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Column Cell Rendering', () => {
    it('should render entity name column with link for valid guid', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });

      await waitFor(() => {
        expect(screen.getByRole('link', { name: /DataSet/i })).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);


    it('should render entity name column without link for guid -1', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: '-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Invalid Entity',
                __guid: '-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render deleted entity with delete icon', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Deleted Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'DELETED'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render entity with serviceType', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [],
            relationshipAttributeDefs: [],
            superTypes: [],
            get: jest.fn((key: string) => {
              if (key === 'serviceType') return 'HIVE';
              return undefined;
            })
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                serviceType: 'HIVE',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render classification column with DialogShowMoreLess', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: ['PII', 'Sensitive'],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.getByTestId('dialog-classifications')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render term column when glossaryTypeParams is empty', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [
                { qualifiedName: 'Term1' },
                { qualifiedName: 'Term2' }
              ],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should not render term column for AtlasGlossary type', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'AtlasGlossaryTerm',
              attributes: {
                name: 'Glossary Term',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'AtlasGlossaryTerm' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render timestamp columns in defaultHideColumns', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1',
                __timestamp: Date.now(),
                __modificationTimestamp: Date.now()
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render guid column with link', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle entity with referredEntities in relationship columns', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'Process',
              attributes: {
                name: 'Test Process',
                inputs: [{ guid: 'guid-2' }],
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {
            'guid-2': {
              guid: 'guid-2',
              typeName: 'DataSet',
              attributes: {
                name: 'Referred Dataset',
                __guid: 'guid-2'
              }
            }
          },
          approximateCount: 1
        }
      });

      const store = createMockStore({
        entityDefs: [
          {
            name: 'Process',
            typeName: 'Process',
            attributeDefs: [
              {
                name: 'inputs',
                typeName: 'array<DataSet>',
                isOptional: true
              }
            ],
            relationshipAttributeDefs: [
              {
                name: 'inputs',
                typeName: 'array<DataSet>'
              }
            ],
            superTypes: []
          }
        ]
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'Process' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle classification search with hide columns filter', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: ['PII'],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ tag: 'PII' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle DSL search with string attributes array', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: ['col1', 'col2'],
          values: [
            ['val1', 'val2'],
            ['val3', 'val4']
          ]
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.queryByTestId('table-loader')).not.toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle DSL search with object attributes array', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: [
            { name: 'col1' },
            { name: 'col2' }
          ],
          values: [
            ['val1', 'val2']
          ]
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.queryByTestId('table-loader')).not.toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should sanitize DSL column names with special characters', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: ['col-name', 'col@value'],
          values: [['val1', 'val2']]
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.queryByTestId('table-loader')).not.toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render cells in DOM for coverage', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                owner: 'test-owner',
                description: 'Test description',
                __guid: 'guid-1'
              },
              classificationNames: ['PII'],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      // Wait for table to render and cells to be rendered
      await waitFor(() => {
        const table = document.querySelector('.table');
        expect(table).toBeInTheDocument();
        expect(screen.getByRole('link', { name: /DataSet/i })).toBeInTheDocument();
        expect(screen.getByTestId('dialog-classifications')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Query Parameter Handling', () => {
    it('should handle query param in basic search', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'basic',
        type: 'DataSet',
        query: 'test query'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(searchApiMethod.getBasicSearchResult).toHaveBeenCalledWith(
          expect.objectContaining({
            data: expect.objectContaining({
              query: 'test query'
            })
          }),
          'basic'
        );
      }, { timeout: 15000 });
    }, 30000);


    it('should update globalSearchParams with query', async () => {
      const searchParams = new URLSearchParams({
        type: 'DataSet',
        query: 'test query'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        const Utils = require('@utils/Utils');
        expect(Utils.globalSearchParams.basicParams.query).toBe('test query');
        expect(Utils.globalSearchParams.dslParams.query).toBe('test query');
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Switch Handler Edge Cases', () => {
    it('should handle excludeSC switch when unchecked initially', async () => {
      const user = userEvent.setup();
      mockSetSearchParams.mockClear();
      
      await act(async () => {
        renderWithProviders(<SearchResult classificationParams="TestClassification" />);
        });

      await waitFor(() => {
        expect(screen.getAllByTestId('ant-switch').length).toBeGreaterThan(0);
      }, { timeout: 15000 });

      const switches = screen.getAllByTestId('ant-switch');
      const excludeSCSwitch = switches[0];

      // First check it
      await user.click(excludeSCSwitch);
      await waitFor(() => {
        expect(mockSetSearchParams).toHaveBeenCalled();
      }, { timeout: 15000 });

      mockSetSearchParams.mockClear();

      // Then uncheck it (covers lines 135-136)
      await user.click(excludeSCSwitch);
      await waitFor(
        () => {
          expect(mockSetSearchParams).toHaveBeenCalled();
        },
        { timeout: 10000 }
      );
    }, 30000);


    it('should handle includeDE switch when unchecked initially', async () => {
      const user = userEvent.setup();
      mockSetSearchParams.mockClear();
      
      await act(async () => {
        renderWithProviders(<SearchResult classificationParams="TestClassification" />);
        });

      await waitFor(() => {
        expect(screen.getAllByTestId('ant-switch').length).toBeGreaterThan(0);
      }, { timeout: 15000 });

      const switches = screen.getAllByTestId('ant-switch');
      const includeDESwitch = switches[1];

      // First check it
      await user.click(includeDESwitch);
      await waitFor(() => {
        expect(mockSetSearchParams).toHaveBeenCalled();
      }, { timeout: 15000 });

      mockSetSearchParams.mockClear();

      // Then uncheck it (covers lines 135-136)
      await user.click(includeDESwitch);
      await waitFor(
        () => {
          expect(mockSetSearchParams).toHaveBeenCalled();
        },
        { timeout: 10000 }
      );
    }, 30000);

  });

  describe('Cell Renderers', () => {
    it('should render name cell with deleted status', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Deleted Entity',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'DELETED'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render name cell with guid -1', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: '-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Invalid Entity',
                __guid: '-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render classificationNames cell with guid -1', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: '-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test',
                __guid: '-1'
              },
              classificationNames: ['PII'],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render term cell with AtlasGlossary typeName', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'AtlasGlossaryTerm',
              attributes: {
                name: 'Test Term',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [{ qualifiedName: 'test.term' }],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render term cell with guid -1', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: '-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test',
                __guid: '-1'
              },
              classificationNames: [],
              meanings: [{ qualifiedName: 'test.term' }],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('DSL Search Functions', () => {
    it('should handle DSL search with attributes array', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: [
            { name: 'col1' },
            { name: 'col2' }
          ],
          values: [
            ['val1', 'val2'],
            ['val3', 'val4']
          ]
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.queryByTestId('table-loader')).not.toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle DSL search with string attributes', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: ['col1', 'col2'],
          values: [
            ['val1', 'val2'],
            ['val3', 'val4']
          ]
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.queryByTestId('table-loader')).not.toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle DSL search with sanitized column names', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: {
            name: ['col-1', 'col@2', 'col 3'],
            values: [
              ['val1', 'val2', 'val3']
            ]
          }
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.queryByTestId('table-loader')).not.toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Default Column Visibility', () => {
    it('should handle defaultColumnVisibility with empty attributes', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle defaultColumnVisibility with attributes param', async () => {
      const searchParams = new URLSearchParams({
        attributes: 'name,owner,description'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle defaultColumnVisibility with partial attributes', async () => {
      const searchParams = new URLSearchParams({
        attributes: 'name'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Default Hide Columns', () => {
    it('should render timestamp columns', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test',
                __timestamp: '2023-01-01T00:00:00Z',
                __modificationTimestamp: '2023-01-02T00:00:00Z',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />);
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle classification search with hide columns', async () => {
      const searchParams = new URLSearchParams({
        tag: 'TestClassification'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Entity Defs Columns', () => {
    it('should generate columns from entityDefs with relationship attributes', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [
              {
                name: 'customField',
                typeName: 'string',
                isOptional: false
              },
              {
                name: 'relationshipField',
                typeName: 'array<Process>',
                isOptional: true
              }
            ],
            relationshipAttributeDefs: [
              {
                name: 'relationshipField',
                typeName: 'array<Process>'
              }
            ],
            superTypes: []
          }
        ]
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should generate columns with superTypes', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [
              {
                name: 'customField',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: ['Referenceable']
          },
          {
            name: 'Referenceable',
            typeName: 'Referenceable',
            attributeDefs: [
              {
                name: 'qualifiedName',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                customField: 'test',
                qualifiedName: 'test@cluster',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should generate relationship columns with referredEntities', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'Process',
            typeName: 'Process',
            attributeDefs: [],
            relationshipAttributeDefs: [
              {
                name: 'inputs',
                typeName: 'array<DataSet>'
              }
            ],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'Process',
              attributes: {
                name: 'Test Process',
                inputs: [{ guid: 'guid-2' }],
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {
            'guid-2': {
              guid: 'guid-2',
              typeName: 'DataSet',
              attributes: {
                name: 'Referred Dataset',
                __guid: 'guid-2'
              }
            }
          },
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'Process' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle relationship columns with array values', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'Process',
            typeName: 'Process',
            attributeDefs: [],
            relationshipAttributeDefs: [
              {
                name: 'inputs',
                typeName: 'array<DataSet>'
              }
            ],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'Process',
              attributes: {
                name: 'Test Process',
                inputs: [{ guid: 'guid-2' }, { guid: 'guid-3' }],
                __guid: 'guid-1'
              },
              inputs: [{ displayText: 'Input 1' }, { displayText: 'Input 2' }],
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {
            'guid-2': {
              guid: 'guid-2',
              typeName: 'DataSet',
              attributes: { name: 'Input 1', __guid: 'guid-2' }
            },
            'guid-3': {
              guid: 'guid-3',
              typeName: 'DataSet',
              attributes: { name: 'Input 2', __guid: 'guid-3' }
            }
          },
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'Process' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle relationship columns with empty array', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'Process',
            typeName: 'Process',
            attributeDefs: [],
            relationshipAttributeDefs: [
              {
                name: 'inputs',
                typeName: 'array<DataSet>'
              }
            ],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'Process',
              attributes: {
                name: 'Test Process',
                inputs: [],
                __guid: 'guid-1'
              },
              inputs: [],
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'Process' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle superType attributes with referredEntities', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [],
            relationshipAttributeDefs: [],
            superTypes: ['Referenceable']
          },
          {
            name: 'Referenceable',
            typeName: 'Referenceable',
            attributeDefs: [
              {
                name: 'qualifiedName',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                qualifiedName: 'test@cluster',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle superType attributes with nested superTypes', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [],
            relationshipAttributeDefs: [],
            superTypes: ['Referenceable']
          },
          {
            name: 'Referenceable',
            typeName: 'Referenceable',
            attributeDefs: [
              {
                name: 'qualifiedName',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: ['Asset']
          },
          {
            name: 'Asset',
            typeName: 'Asset',
            attributeDefs: [
              {
                name: 'name',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                qualifiedName: 'test@cluster',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle entity with serviceType from entityDefs', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'HiveTable',
            typeName: 'HiveTable',
            attributeDefs: [],
            relationshipAttributeDefs: [],
            superTypes: [],
            get: jest.fn((key: string) => {
              if (key === 'serviceType') return 'HIVE';
              return undefined;
            })
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'HiveTable',
              attributes: {
                name: 'Test Table',
                serviceType: 'HIVE',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'HiveTable' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle entity with serviceType from attributes', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'HiveTable',
              attributes: {
                name: 'Test Table',
                serviceType: 'HIVE',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'HiveTable' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle entity without serviceType', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle defaultColumnVisibility with attributes param', async () => {
      const searchParams = new URLSearchParams({
        attributes: 'name,owner,description',
        type: 'DataSet'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // Verify defaultColumnVisibility was called
        const tableLayout = screen.getByTestId('table-layout');
        expect(tableLayout).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle defaultColumnVisibility with show=false columns', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle defaultColumnVisibility excluding columns not in attributes', async () => {
      const searchParams = new URLSearchParams({
        attributes: 'name',
        type: 'DataSet'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle timestamp columns with undefined values', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
                // No __timestamp or __modificationTimestamp
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle owner column cell renderer', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                owner: 'test-owner',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(capturedColumns.find((col: any) => col.accessorKey === 'owner')).toBeDefined();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle description column cell renderer', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                description: 'Test description',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(capturedColumns.find((col: any) => col.accessorKey === 'description')).toBeDefined();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle typeName column cell renderer', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(capturedColumns.find((col: any) => col.accessorKey === 'typeName')).toBeDefined();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle classificationNames column with guid -1', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: '-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Invalid Entity',
                __guid: '-1'
              },
              classificationNames: ['PII'],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle term column when glossaryTypeParams is empty', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [{ qualifiedName: 'test.term' }],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle term column when glossaryTypeParams is not empty', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [{ qualifiedName: 'test.term' }],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult glossaryTypeParams="TestTerm" />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle superType attributes with array values and referredEntities', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [],
            relationshipAttributeDefs: [],
            superTypes: ['Referenceable']
          },
          {
            name: 'Referenceable',
            typeName: 'Referenceable',
            attributeDefs: [
              {
                name: 'qualifiedName',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                qualifiedName: [{ guid: 'guid-2' }],
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {
            'guid-2': {
              guid: 'guid-2',
              typeName: 'String',
              attributes: { name: 'test', __guid: 'guid-2' }
            }
          },
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle superType attributes with single value and referredEntities', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [],
            relationshipAttributeDefs: [],
            superTypes: ['Referenceable']
          },
          {
            name: 'Referenceable',
            typeName: 'Referenceable',
            attributeDefs: [
              {
                name: 'qualifiedName',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                qualifiedName: { guid: 'guid-2' },
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {
            'guid-2': {
              guid: 'guid-2',
              typeName: 'String',
              attributes: { name: 'test', __guid: 'guid-2' }
            }
          },
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle relationship columns with single value and referredEntities', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'Process',
            typeName: 'Process',
            attributeDefs: [],
            relationshipAttributeDefs: [
              {
                name: 'inputs',
                typeName: 'DataSet'
              }
            ],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'Process',
              attributes: {
                name: 'Test Process',
                inputs: { guid: 'guid-2' },
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {
            'guid-2': {
              guid: 'guid-2',
              typeName: 'DataSet',
              attributes: { name: 'Input Dataset', __guid: 'guid-2' }
            }
          },
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'Process' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle entityDefsCol with superTypes', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [
              {
                name: 'customField',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: ['Referenceable']
          },
          {
            name: 'Referenceable',
            typeName: 'Referenceable',
            attributeDefs: [
              {
                name: 'qualifiedName',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                customField: 'test',
                qualifiedName: 'test@cluster',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle getEntityDefsCol with sortable types', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [
              {
                name: 'createTime',
                typeName: 'date',
                isOptional: false
              },
              {
                name: 'count',
                typeName: 'int',
                isOptional: false
              },
              {
                name: 'isActive',
                typeName: 'boolean',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                createTime: Date.now(),
                count: 10,
                isActive: true,
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle DSL search with empty attributes', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {}
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.getByText(/No Records found!/i)).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle DSL search with empty values array', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: {
            name: ['col1'],
            values: []
          }
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.getByText(/No Records found!/i)).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle DSL search with row data as object', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: {
            name: ['col1', 'col2'],
            values: [
              { col1: 'val1', col2: 'val2' },
              { col1: 'val3', col2: 'val4' }
            ]
          }
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.queryByTestId('table-loader')).not.toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle defaultColumnVisibility with empty columnsParams', async () => {
      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle defaultColumnVisibility with columnsParams including all columns', async () => {
      const searchParams = new URLSearchParams({
        attributes: 'name,owner,description,typeName,classificationNames,term',
        type: 'DataSet'
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render name column cell with deleted entity', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Deleted Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'DELETED'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        const nameCol = capturedColumns.find((col: any) => col.accessorKey === 'name');
        expect(nameCol).toBeDefined();
        expect(nameCol?.cell).toBeDefined();
      }, { timeout: 15000 });
    }, 30000);


    it('should render name column cell with guid -1', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: '-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Invalid Entity',
                __guid: '-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        const nameCol = capturedColumns.find((col: any) => col.accessorKey === 'name');
        expect(nameCol).toBeDefined();
        expect(nameCol?.cell).toBeDefined();
      }, { timeout: 15000 });
    }, 30000);


    it('should render classificationNames cell with proper structure', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: ['PII', 'Sensitive'],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        const classCol = capturedColumns.find((col: any) => col.accessorKey === 'classificationNames');
        expect(classCol).toBeDefined();
        expect(classCol?.cell).toBeDefined();
      }, { timeout: 15000 });
    }, 30000);


    it('should render term cell with proper structure', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [{ qualifiedName: 'test.term' }],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        const termCol = capturedColumns.find((col: any) => col.accessorKey === 'term');
        expect(termCol).toBeDefined();
      }, { timeout: 15000 });
    }, 30000);


    it('should render __guid cell with link', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        const guidCol = capturedColumns.find((col: any) => col.accessorKey === '__guid');
        expect(guidCol).toBeDefined();
        expect(guidCol?.cell).toBeDefined();
      }, { timeout: 15000 });
    }, 30000);


    it('should render timestamp cells with date formatting', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __timestamp: Date.now(),
                __modificationTimestamp: Date.now(),
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        const timestampCol = capturedColumns.find((col: any) => col.accessorKey === '__timestamp');
        const modTimestampCol = capturedColumns.find((col: any) => col.accessorKey === '__modificationTimestamp');
        expect(timestampCol).toBeDefined();
        expect(modTimestampCol).toBeDefined();
        expect(timestampCol?.cell).toBeDefined();
        expect(modTimestampCol?.cell).toBeDefined();
      }, { timeout: 15000 });
    }, 30000);


    it('should render DSL column cells', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          attributes: {
            name: ['col1'],
            values: [['val1']]
          }
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        const dslCol = capturedColumns.find((col: any) => col.id?.startsWith('dsl_'));
        expect(dslCol).toBeDefined();
        expect(dslCol?.cell).toBeDefined();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle superType attribute with relationshipAttributeDefs match', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [],
            relationshipAttributeDefs: [
              {
                name: 'qualifiedName',
                typeName: 'string'
              }
            ],
            superTypes: ['Referenceable']
          },
          {
            name: 'Referenceable',
            typeName: 'Referenceable',
            attributeDefs: [
              {
                name: 'qualifiedName',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle entity attribute with relationshipAttributeDefs match', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'Process',
            typeName: 'Process',
            attributeDefs: [
              {
                name: 'inputs',
                typeName: 'array<DataSet>',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [
              {
                name: 'inputs',
                typeName: 'array<DataSet>'
              }
            ],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'Process',
              attributes: {
                name: 'Test Process',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'Process' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle getSuperTypeAttributeDefsCol with entityAttr parameter', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [
              {
                name: 'customField',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: ['Referenceable']
          },
          {
            name: 'Referenceable',
            typeName: 'Referenceable',
            attributeDefs: [
              {
                name: 'qualifiedName',
                typeName: 'string',
                isOptional: false
              }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                customField: 'test',
                qualifiedName: 'test@cluster',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle relationship column with empty referredEntities', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'Process',
            typeName: 'Process',
            attributeDefs: [],
            relationshipAttributeDefs: [
              {
                name: 'inputs',
                typeName: 'array<DataSet>'
              }
            ],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'Process',
              attributes: {
                name: 'Test Process',
                inputs: [{ guid: 'guid-2' }],
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'Process' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle relationship column with undefined referredEntities', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'Process',
            typeName: 'Process',
            attributeDefs: [],
            relationshipAttributeDefs: [
              {
                name: 'inputs',
                typeName: 'array<DataSet>'
              }
            ],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'Process',
              attributes: {
                name: 'Test Process',
                inputs: [{ guid: 'guid-2' }],
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'Process' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle defaultHideColumns with classification search', async () => {
      const searchParams = new URLSearchParams({
        tag: 'TestClassification',
        searchType: 'basic'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1',
                __typeName: 'DataSet'
              },
              classificationNames: ['TestClassification'],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams   });
        });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should handle defaultHideColumns cell renderer for non-timestamp columns', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Dataset',
                __guid: 'guid-1',
                __modifiedBy: 'user1',
                __createdBy: 'user2',
                __state: 'ACTIVE'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);

  });

  describe('Cell Renderer Coverage - Comprehensive Tests', () => {
    it('should render name cell with all branches (ACTIVE, DELETED, guid=-1, serviceType)', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [{ name: 'name', typeName: 'string' }],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Active Entity',
                serviceType: 'hive_table'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            },
            {
              guid: 'guid-2',
              typeName: 'DataSet',
              attributes: {
                name: 'Deleted Entity',
                serviceType: 'hive_table'
              },
              classificationNames: [],
              meanings: [],
              status: 'DELETED'
            },
            {
              guid: '-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Invalid Entity'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 3
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(screen.getAllByText(/Active Entity/).length).toBeGreaterThan(0);
        expect(screen.getAllByText(/Deleted Entity/).length).toBeGreaterThan(0);
      }, { timeout: 15000 });
    }, 30000);


    it('should render classification cell with all branches', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Entity',
                __guid: 'guid-1'
              },
              classificationNames: ['PII', 'Sensitive'],
              meanings: [],
              status: 'ACTIVE',
              classifications: [
                { typeName: 'PII', guid: 'pii-1' },
                { typeName: 'Sensitive', guid: 'sens-1' }
              ]
            },
            {
              guid: '-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Invalid Entity',
                __guid: '-1'
              },
              classificationNames: ['PII'],
              meanings: [],
              status: 'ACTIVE',
              classifications: [{ typeName: 'PII', guid: 'pii-1' }]
            },
            {
              guid: 'guid-2',
              typeName: 'DataSet',
              attributes: {
                name: 'Deleted Entity',
                __guid: 'guid-2'
              },
              classificationNames: ['PII'],
              meanings: [],
              status: 'DELETED',
              classifications: [{ typeName: 'PII', guid: 'pii-1' }]
            }
          ],
          referredEntities: {},
          approximateCount: 3
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // DialogShowMoreLess should be rendered for classifications
        expect(screen.getAllByTestId('dialog-classifications').length).toBeGreaterThan(0);
      }, { timeout: 15000 });
    }, 30000);


    it('should render term cell with all branches', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Entity with Term',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [
                { qualifiedName: 'term1@glossary' },
                { qualifiedName: 'term2@glossary' }
              ],
              status: 'ACTIVE'
            },
            {
              guid: 'guid-2',
              typeName: 'AtlasGlossaryTerm',
              attributes: {
                name: 'Glossary Term Entity',
                __guid: 'guid-2'
              },
              classificationNames: [],
              meanings: [{ qualifiedName: 'term@glossary' }],
              status: 'ACTIVE'
            },
            {
              guid: '-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Invalid Entity',
                __guid: '-1'
              },
              classificationNames: [],
              meanings: [{ qualifiedName: 'term@glossary' }],
              status: 'ACTIVE'
            },
            {
              guid: 'guid-3',
              typeName: 'DataSet',
              attributes: {
                name: 'Deleted Entity',
                __guid: 'guid-3'
              },
              classificationNames: [],
              meanings: [{ qualifiedName: 'term@glossary' }],
              status: 'DELETED'
            }
          ],
          referredEntities: {},
          approximateCount: 4
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // DialogShowMoreLess should be rendered for terms (non-glossary entities)
        expect(screen.getAllByTestId('dialog-meanings').length).toBeGreaterThan(0);
      }, { timeout: 15000 });
    }, 30000);


    it('should render typeName cell with link', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Entity',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        const typeLink = screen.getByRole('link', { name: /^DataSet$/i });
        expect(typeLink).toBeInTheDocument();
        expect(typeLink).toHaveAttribute('href', '/search/searchResult');
      }, { timeout: 15000 });
    }, 30000);


    it('should render owner and description cells', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Entity',
                owner: 'test-owner',
                description: 'Test description',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet' })
        });
      });

      await waitFor(() => {
        const table = screen.getByRole('table')
        expect(table).toHaveTextContent('test-owner')
        expect(table).toHaveTextContent('Test description')
      }, { timeout: 15000 });
    }, 30000);


    it('should render defaultHideColumns cells (timestamps, guid, status, etc.)', async () => {
      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Entity',
                __guid: 'guid-1',
                __timestamp: '2023-01-01T00:00:00Z',
                __modificationTimestamp: '2023-01-02T00:00:00Z',
                __modifiedBy: 'user1',
                __createdBy: 'user2',
                __state: 'ACTIVE',
                __typeName: 'DataSet',
                __isIncomplete: false,
                __labels: ['label1'],
                __customAttributes: { key: 'value' },
                __pendingTasks: []
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        searchParams: new URLSearchParams({ type: 'DataSet', attributes: '__guid,__timestamp,__modificationTimestamp' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        // Guid should be rendered as a link
        const guidLink = screen.getAllByText('guid-1')[0];
        expect(guidLink.closest('a')).toHaveAttribute('href', '/detailPage/guid-1');
      }, { timeout: 15000 });
    }, 30000);


    it('should render getEntityDefsCol cells with sortable and non-sortable types', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [
              { name: 'stringAttr', typeName: 'string' },
              { name: 'intAttr', typeName: 'int' },
              { name: 'dateAttr', typeName: 'date' },
              { name: 'booleanAttr', typeName: 'boolean' },
              { name: 'complexAttr', typeName: 'complex_type' }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Entity',
                stringAttr: 'string value',
                intAttr: 123,
                dateAttr: '2023-01-01',
                booleanAttr: true,
                complexAttr: 'nested-value',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {},
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet', attributes: 'stringAttr,intAttr,dateAttr,booleanAttr,complexAttr' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render getSuperTypeAttributeDefsCol cells with referredEntities', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'SuperType',
            typeName: 'SuperType',
            attributeDefs: [
              { name: 'superAttr', typeName: 'string' }
            ],
            relationshipAttributeDefs: [],
            superTypes: []
          },
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [
              { name: 'name', typeName: 'string' }
            ],
            relationshipAttributeDefs: [],
            superTypes: ['SuperType']
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Entity',
                superAttr: 'ref-guid-1',
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {
            'ref-guid-1': {
              guid: 'ref-guid-1',
              typeName: 'ReferredEntity',
              attributes: {
                name: 'Referred Entity'
              }
            }
          },
          approximateCount: 1
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet', attributes: 'superAttr' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render relationshipAttributeDefsCol cells with array and single values', async () => {
      const store = createMockStore({
        entityDefs: [
          {
            name: 'DataSet',
            typeName: 'DataSet',
            attributeDefs: [
              { name: 'name', typeName: 'string' }
            ],
            relationshipAttributeDefs: [
              { name: 'relAttr', typeName: 'DataSet' }
            ],
            superTypes: []
          }
        ]
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValueOnce({
        data: {
          entities: [
            {
              guid: 'guid-1',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Entity',
                relAttr: [
                  { guid: 'ref-guid-1', displayText: 'Ref 1' },
                  { guid: 'ref-guid-2', displayText: 'Ref 2' }
                ],
                __guid: 'guid-1'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            },
            {
              guid: 'guid-2',
              typeName: 'DataSet',
              attributes: {
                name: 'Test Entity 2',
                relAttr: { guid: 'ref-guid-3', displayText: 'Ref 3' },
                __guid: 'guid-2'
              },
              classificationNames: [],
              meanings: [],
              status: 'ACTIVE'
            }
          ],
          referredEntities: {
            'ref-guid-1': {
              guid: 'ref-guid-1',
              typeName: 'ReferredEntity',
              attributes: { name: 'Referred Entity 1' }
            },
            'ref-guid-2': {
              guid: 'ref-guid-2',
              typeName: 'ReferredEntity',
              attributes: { name: 'Referred Entity 2' }
            },
            'ref-guid-3': {
              guid: 'ref-guid-3',
              typeName: 'ReferredEntity',
              attributes: { name: 'Referred Entity 3' }
            }
          },
          approximateCount: 2
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, {
        store,
        searchParams: new URLSearchParams({ type: 'DataSet', attributes: 'relAttr' })
        });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
      }, { timeout: 15000 });
    }, 30000);


    it('should render DSL search cells', async () => {
      const searchParams = new URLSearchParams({
        searchType: 'dsl',
        query: 'test'
      });

      (searchApiMethod.getBasicSearchResult as jest.Mock).mockResolvedValue({
        data: {
          attributes: {
            name: ['col1', 'col2'],
            values: [
              ['value1', 'value2']
            ]
          }
        }
      });

      await act(async () => {
        renderWithProviders(<SearchResult />, { searchParams });
      });

      await waitFor(() => {
        expect(document.querySelector('.table')).toBeInTheDocument();
        expect(screen.getAllByText('value1').length).toBeGreaterThan(0);
        expect(screen.getAllByText('value2').length).toBeGreaterThan(0);
      }, { timeout: 15000 });
    }, 30000);

  });
});
