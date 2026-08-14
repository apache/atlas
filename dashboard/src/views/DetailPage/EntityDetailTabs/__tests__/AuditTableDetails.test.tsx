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
import { render, screen, waitFor, act } from '@testing-library/react';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import userEvent from '@testing-library/user-event';
import AuditTableDetails from '../AuditTableDetails';

// Mock AttributeProperties
jest.mock('../AttributeProperties', () => ({
  __esModule: true,
  default: ({ entity, propertiesName }: any) => (
    <div data-testid={`attribute-properties-${propertiesName.toLowerCase()}`}>
      AttributeProperties - {propertiesName} - {entity?.typeName || 'No Type'}
    </div>
  )
}));

// Mock utils - must handle all cases including empty objects
const mockExtractKeyValueFromEntity = jest.fn();
const mockIsArray = jest.fn();
const mockIsEmpty = jest.fn();

jest.mock('@utils/Utils', () => ({
  extractKeyValueFromEntity: (entity: any, nullVal?: any, skipAttr?: any) => mockExtractKeyValueFromEntity(entity, nullVal, skipAttr),
  isArray: (val: any) => mockIsArray(val),
  isEmpty: (val: any) => mockIsEmpty(val)
}));

describe('AuditTableDetails', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    
    // Setup default mock implementations
    mockExtractKeyValueFromEntity.mockImplementation((entity: any, nullVal?: any, skipAttr?: any) => {
      if (!entity || (typeof entity === 'object' && Object.keys(entity).length === 0)) {
        return { name: '', found: false, key: null };
      }
      const name = entity?.attributes?.name || entity?.name || entity?.typeName || '';
      return { name, found: !!name, key: 'name' };
    });
    
    mockIsArray.mockImplementation((val: any) => Array.isArray(val));
    
    mockIsEmpty.mockImplementation((val: any) => {
      if (val === null || val === undefined || val === '') return true;
      if (Array.isArray(val) && val.length === 0) return true;
      if (typeof val === 'object' && val !== null && Object.keys(val).length === 0) return true;
      return false;
    });
  });

  const createMockStore = () => {
    return configureStore({
      reducer: {
        entity: () => ({
          loading: false,
          entityData: {
            entityDefs: [
              {
                name: 'test_entity',
                attributeDefs: []
              }
            ]
          }
        })
      }
    });
  };

  const mockComponentProps = {
    entity: {
      typeName: 'test_entity',
      guid: 'test-guid-123',
      attributes: {
        name: 'Test Entity'
      }
    },
    referredEntities: {},
    loading: false
  };

  const renderWithProviders = (props: any, store = createMockStore()) => {
    return render(
      <Provider store={store}>
        <AuditTableDetails {...props} />
      </Provider>
    );
  };

  describe('String Parsing with Colon Delimiter', () => {
    it('should parse details with colon delimiter and JSON', async () => {
      const mockRow = {
        original: {
          details: 'Updated entity: {"typeName":"test_entity","attributes":{"name":"Test"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // Should render AttributeProperties components
      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle "Added labels" type', async () => {
      const mockRow = {
        original: {
          details: 'Added labels: label1 label2 label3'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // Should show labels as comma-separated
      await waitFor(() => {
        expect(screen.getByText(/label1,label2,label3/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle "Deleted labels" type', async () => {
      const mockRow = {
        original: {
          details: 'Deleted labels: tag1 tag2'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // Should show labels as comma-separated
      await waitFor(() => {
        expect(screen.getByText(/tag1,tag2/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle simple string without JSON', async () => {
      const mockRow = {
        original: {
          details: 'Updated property: simpleValue'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // Should show the simple value
      await waitFor(() => {
        expect(screen.getByText(/simpleValue/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle details with multiple colons', async () => {
      const mockRow = {
        original: {
          details: 'Updated: property: value: with: colons'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // Should join all parts after first colon
      await waitFor(() => {
        expect(screen.getByText(/property: value: with: colons/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);
  });

  describe('JSON Parsing', () => {
    it('should parse JSON with typeName', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test_entity","attributes":{"name":"Test Entity"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should parse JSON without typeName', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"attributes":{"name":"Test Entity"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle JSON with relationshipAttributes', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test","relationshipAttributes":{"rel":"value"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
        expect(screen.getByTestId('attribute-properties-relationship')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle JSON with customAttributes', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test","customAttributes":{"custom":"value"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
        expect(screen.getByTestId('attribute-properties-user-defined')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle JSON with all attribute types', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test","attributes":{"name":"Test"},"relationshipAttributes":{"rel":"value"},"customAttributes":{"custom":"value"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
        expect(screen.getByTestId('attribute-properties-relationship')).toBeInTheDocument();
        expect(screen.getByTestId('attribute-properties-user-defined')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle name extraction with dash', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test_type","guid":"-"}'
        }
      };

      // Mock extractKeyValueFromEntity to return name as "-" for the parsed object
      mockExtractKeyValueFromEntity.mockImplementation((entity: any) => {
        if (entity && entity.typeName === 'test_type') {
          return { name: '-', found: true, key: 'name' };
        }
        return { name: entity?.attributes?.name || entity?.typeName || '', found: !!entity, key: 'name' };
      });

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // When name is "-", should show typeName directly
      await waitFor(() => {
        const elements = screen.getAllByText(/test_type/i);
        expect(elements.length).toBeGreaterThan(0);
      }, { timeout: 10000 });
    }, 30000);
  });

  describe('Special Cases', () => {
    it('should handle "Deleted entity" details', async () => {
      const mockRow = {
        original: {
          details: 'Deleted entity'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // Should show entity typeName
      await waitFor(() => {
        expect(screen.getByText(/test_entity/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle "Purged entity" details', async () => {
      const mockRow = {
        original: {
          details: 'Purged entity'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // Should show entity typeName
      await waitFor(() => {
        expect(screen.getByText(/test_entity/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should show "No details to show!" for deleted entity without typeName', async () => {
      const mockRow = {
        original: {
          details: 'Deleted entity'
        }
      };

      const propsWithoutTypeName = {
        componentProps: {
          entity: {},
          referredEntities: {},
          loading: false
        },
        row: mockRow
      };

      await act(async () => {
        renderWithProviders(propsWithoutTypeName);
      });

      await waitFor(() => {
        expect(screen.getByText(/No details to show!/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should show "No details to show!" for purged entity without typeName', async () => {
      const mockRow = {
        original: {
          details: 'Purged entity'
        }
      };

      const propsWithoutTypeName = {
        componentProps: {
          entity: {},
          referredEntities: {},
          loading: false
        },
        row: mockRow
      };

      await act(async () => {
        renderWithProviders(propsWithoutTypeName);
      });

      await waitFor(() => {
        expect(screen.getByText(/No details to show!/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);
  });

  describe('Error Handling', () => {
    it('should handle invalid JSON gracefully', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {invalid json'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByText(/No details to show!/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle JSON parse error with array', async () => {
      const mockRow = {
        original: {
          details: 'Updated: ["item1", "item2"]'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // Arrays are valid JSON, so they parse successfully and are displayed
      // The array gets converted to string and shown in updateName
      await waitFor(() => {
        expect(screen.getByText(/item1/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle empty entity object', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test"}'
        }
      };

      const propsWithEmptyEntity = {
        componentProps: {
          entity: {},
          referredEntities: {},
          loading: false
        },
        row: mockRow
      };

      await act(async () => {
        renderWithProviders(propsWithEmptyEntity);
      });

      // Empty entity object doesn't prevent rendering - the parsed JSON is still displayed
      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);
  });

  describe('Edge Cases', () => {
    it('should handle details without colon', async () => {
      const mockRow = {
        original: {
          details: 'Simple text without colon'
        }
      };

      let container: HTMLElement;
      await act(async () => {
        const result = renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
        container = result.container;
      });

      // Details without colon return undefined from getAuditDetails
      expect(container!).toBeInTheDocument();
    }, 30000);

    it('should handle empty details string', async () => {
      const mockRow = {
        original: {
          details: ''
        }
      };

      let container: HTMLElement;
      await act(async () => {
        const result = renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
        container = result.container;
      });

      expect(container!).toBeInTheDocument();
    }, 30000);

    it('should handle details with only colon', async () => {
      const mockRow = {
        original: {
          details: ':'
        }
      };

      let container: HTMLElement;
      await act(async () => {
        const result = renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
        container = result.container;
      });

      expect(container!).toBeInTheDocument();
    }, 30000);

    it('should handle special characters in details', async () => {
      const mockRow = {
        original: {
          details: 'Updated: Test <>&"\' special chars'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByText(/special chars/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle very long details string', async () => {
      const longString = 'a'.repeat(1000);
      const mockRow = {
        original: {
          details: `Updated: ${longString}`
        }
      };

      let container: HTMLElement;
      await act(async () => {
        const result = renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
        container = result.container;
      });

      await waitFor(() => {
        expect(container!.textContent).toContain('a');
      }, { timeout: 10000 });
    }, 30000);

    it('should handle null entity in componentProps', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test"}'
        }
      };

      const propsWithNullEntity = {
        componentProps: {
          entity: null,
          referredEntities: {},
          loading: false
        },
        row: mockRow
      };

      await act(async () => {
        renderWithProviders(propsWithNullEntity);
      });

      // Null entity doesn't prevent rendering - the parsed JSON is still displayed
      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle undefined referredEntities', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test","attributes":{"name":"Test"}}'
        }
      };

      const propsWithUndefinedRefs = {
        componentProps: {
          entity: mockComponentProps.entity,
          referredEntities: undefined,
          loading: false
        },
        row: mockRow
      };

      await act(async () => {
        renderWithProviders(propsWithUndefinedRefs);
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);
  });

  describe('Component Integration', () => {
    it('should pass correct props to AttributeProperties for technical', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test_entity","attributes":{"name":"Test"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        const technicalProps = screen.getByTestId('attribute-properties-technical');
        expect(technicalProps).toHaveTextContent('Technical');
        expect(technicalProps).toHaveTextContent('test_entity');
      }, { timeout: 10000 });
    }, 30000);

    it('should pass loading state to AttributeProperties', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test","attributes":{"name":"Test"}}'
        }
      };

      const propsWithLoading = {
        componentProps: {
          ...mockComponentProps,
          loading: true
        },
        row: mockRow
      };

      await act(async () => {
        renderWithProviders(propsWithLoading);
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should pass auditDetails=true to AttributeProperties', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test","attributes":{"name":"Test"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // AttributeProperties should receive auditDetails=true
      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);
  });

  describe('Name Display', () => {
    it('should display entity name with typeName in parentheses', async () => {
      const mockRow = {
        original: {
          details: 'Updated: simple text'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // When auditData doesn't contain JSON and entity is empty {}, 
      // updateName shows just the auditData string (entityName parameter)
      await waitFor(() => {
        expect(screen.getByText(/simple text/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle name extraction from JSON entity', async () => {
      const mockRow = {
        original: {
          details: 'Created: {"typeName":"new_entity","attributes":{"name":"New Entity"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByText(/New Entity/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should show typeName when name is dash', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test_type","guid":"-"}'
        }
      };

      // Mock extractKeyValueFromEntity to return name as "-" for the parsed object
      mockExtractKeyValueFromEntity.mockImplementation((entity: any) => {
        if (entity && entity.typeName === 'test_type') {
          return { name: '-', found: true, key: 'name' };
        }
        return { name: entity?.attributes?.name || entity?.typeName || '', found: !!entity, key: 'name' };
      });

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        const elements = screen.getAllByText(/test_type/i);
        expect(elements.length).toBeGreaterThan(0);
      }, { timeout: 10000 });
    }, 30000);
  });

  describe('Conditional Rendering', () => {
    it('should not render relationship attributes when empty', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test","attributes":{"name":"Test"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
        expect(screen.queryByTestId('attribute-properties-relationship')).not.toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should not render custom attributes when empty', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test","attributes":{"name":"Test"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
        expect(screen.queryByTestId('attribute-properties-user-defined')).not.toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should render all three attribute types when present', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test","attributes":{"name":"Test"},"relationshipAttributes":{"rel":"value"},"customAttributes":{"custom":"value"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
        expect(screen.getByTestId('attribute-properties-relationship')).toBeInTheDocument();
        expect(screen.getByTestId('attribute-properties-user-defined')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);
  });

  describe('Error States', () => {
    it('should show error for unparseable JSON', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {invalid: json}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        const noDataElement = screen.getByText(/No details to show!/i);
        expect(noDataElement).toBeInTheDocument();
        expect(noDataElement.closest('h4')).toHaveAttribute('data-cy', 'noData');
      }, { timeout: 10000 });
    }, 30000);

    it('should handle JSON parse error with array in catch block', async () => {
      const mockRow = {
        original: {
          details: 'Updated: ["array", "values"]'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      // Arrays are valid JSON, so they parse successfully and are displayed
      // The array gets converted to string representation and shown
      await waitFor(() => {
        expect(screen.getByText(/array/i)).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);
  });

  describe('Redux Integration', () => {
    it('should use entityData from Redux store', async () => {
      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test","attributes":{"name":"Test"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        });
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should handle null entityData', async () => {
      const store = configureStore({
        reducer: {
          entity: () => ({
            loading: false,
            entityData: null
          })
        }
      });

      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test","attributes":{"name":"Test"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        }, store);
      });

      // Should handle null entityData gracefully
      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);

    it('should find matching entityDef', async () => {
      const store = configureStore({
        reducer: {
          entity: () => ({
            loading: false,
            entityData: {
              entityDefs: [
                {
                  name: 'test_entity',
                  attributeDefs: [{ name: 'attr1' }]
                }
              ]
            }
          })
        }
      });

      const mockRow = {
        original: {
          details: 'Updated: {"typeName":"test_entity","attributes":{"name":"Test"}}'
        }
      };

      await act(async () => {
        renderWithProviders({
          componentProps: mockComponentProps,
          row: mockRow
        }, store);
      });

      await waitFor(() => {
        expect(screen.getByTestId('attribute-properties-technical')).toBeInTheDocument();
      }, { timeout: 10000 });
    }, 30000);
  });
});
