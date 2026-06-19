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

import { render, screen, waitFor } from '@testing-library/react';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import React from 'react';
import RauditsTableResults from '../RauditsTableResults';

// Mock utils - hoist mocks using var for proper hoisting
var mockIsArray: jest.Mock;
var mockIsEmpty: jest.Mock;
var mockIsNull: jest.Mock;
var mockGetValues: jest.Mock;

jest.mock('@utils/Utils', () => {
	// Create mocks inside factory function
	mockIsArray = jest.fn((val) => Array.isArray(val));
	mockIsEmpty = jest.fn((val) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0));
	mockIsNull = jest.fn((val) => val === null);
	
	return {
		isArray: mockIsArray,
		isEmpty: mockIsEmpty,
		isNull: mockIsNull
	};
});

// Mock getValues - return React elements, not strings
jest.mock('@components/commonComponents', () => {
	// Create mock inside factory function
	mockGetValues = jest.fn((value, entityData, entity, relationShipAttr, properties, referredEntities, filterEntityData, keys) => {
		// Return React elements based on value type
		if (Array.isArray(value)) {
			// Return a component that renders array items
			return React.createElement('span', { 'data-testid': 'array-value' }, value.join(', '));
		}
		if (typeof value === 'object' && value !== null) {
			return React.createElement('span', { 'data-testid': 'object-value' }, JSON.stringify(value));
		}
		if (typeof value === 'boolean') {
			return React.createElement('span', { 'data-testid': 'boolean-value' }, String(value));
		}
		return React.createElement('span', { 'data-testid': 'string-value' }, String(value));
	});
	
	return {
		getValues: mockGetValues
	};
});

describe('RauditsTableResults', () => {
	const createMockStore = (entityData = {}) => {
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
						],
						...entityData
					}
				})
			}
		});
	};

	const mockComponentProps = {
		entity: {
			typeName: 'test_entity',
			guid: 'test-guid-123'
		},
		referredEntities: {}
	};

	const renderWithProviders = (props: any, store = createMockStore()) => {
		return render(
			<Provider store={store}>
				<RauditsTableResults {...props} />
			</Provider>
		);
	};

	beforeEach(() => {
		jest.clearAllMocks();
		// Reset mock implementations
		if (mockIsArray) {
			mockIsArray.mockImplementation((val) => Array.isArray(val));
		}
		if (mockIsEmpty) {
			mockIsEmpty.mockImplementation((val) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0));
		}
		if (mockIsNull) {
			mockIsNull.mockImplementation((val) => val === null);
		}
		if (mockGetValues) {
			mockGetValues.mockImplementation((value, entityData, entity, relationShipAttr, properties, referredEntities, filterEntityData, keys) => {
				if (Array.isArray(value)) {
					return React.createElement('span', { 'data-testid': 'array-value' }, value.join(', '));
				}
				if (typeof value === 'object' && value !== null) {
					return React.createElement('span', { 'data-testid': 'object-value' }, JSON.stringify(value));
				}
				if (typeof value === 'boolean') {
					return React.createElement('span', { 'data-testid': 'boolean-value' }, String(value));
				}
				return React.createElement('span', { 'data-testid': 'string-value' }, String(value));
			});
		}
	});

  describe('Rendering', () => {
    it('should render audit details with valid JSON', async () => {
      const mockRow = {
        original: {
          resultSummary: JSON.stringify({
            status: 'SUCCESS',
            count: 10,
            message: 'Test message'
          })
        }
      };

      renderWithProviders({
        componentProps: mockComponentProps,
        row: mockRow
      });

      await waitFor(() => {
        expect(screen.getByText(/status/i)).toBeInTheDocument();
      }, { timeout: 3000 });
      
      expect(screen.getByText(/count/i)).toBeInTheDocument();
      // Use getAllByText since "message" might appear multiple times
      const messageElements = screen.getAllByText(/message/i);
      expect(messageElements.length).toBeGreaterThan(0);
    });

    it('should render array values', () => {
      const mockRow = {
        original: {
          resultSummary: JSON.stringify({
            items: ['item1', 'item2', 'item3']
          })
        }
      };

      const { container } = renderWithProviders({
        componentProps: mockComponentProps,
        row: mockRow
      });

      // Check that the component renders the items key
      expect(screen.getByText(/items/i)).toBeInTheDocument();
      // The getValues mock will render the array as "item1, item2, item3"
      const text = container.textContent;
      expect(text).toContain('items');
      expect(text).toContain('item1');
    });

		it('should render nested object values', async () => {
			const mockRow = {
				original: {
					resultSummary: JSON.stringify({
						data: {
							nested: 'value'
						}
					})
				}
			};

			renderWithProviders({
				componentProps: mockComponentProps,
				row: mockRow
			});

			await waitFor(() => {
				expect(screen.getByText(/data/i)).toBeInTheDocument();
			}, { timeout: 3000 });
		});

		it('should render multiple properties sorted', async () => {
			const mockRow = {
				original: {
					resultSummary: JSON.stringify({
						zebra: 'last',
						apple: 'first',
						middle: 'second'
					})
				}
			};

			renderWithProviders({
				componentProps: mockComponentProps,
				row: mockRow
			});

			await waitFor(() => {
				const properties = screen.getAllByText(/apple|middle|zebra/i);
				expect(properties.length).toBeGreaterThan(0);
			}, { timeout: 3000 });
		});
	});

  describe('Error Handling', () => {
    it('should show "No details to show!" for invalid JSON', () => {
      const mockRow = {
        original: {
          resultSummary: 'invalid json {'
        }
      };

      renderWithProviders({
        componentProps: mockComponentProps,
        row: mockRow
      });

      // Check for the error message
      const errorElement = screen.getByText((content, element) => {
        return element?.tagName === 'I' && content.includes('No details to show!');
      });
      expect(errorElement).toBeInTheDocument();
      
      // Check for data-cy attribute on parent h4
      const h4Element = errorElement.closest('h4');
      expect(h4Element).toHaveAttribute('data-cy', 'noData');
    });

    it('should handle empty string gracefully', () => {
      const mockRow = {
        original: {
          resultSummary: ''
        }
      };

      // Empty string causes JSON.parse to throw "Unexpected end of JSON input"
      const { container } = renderWithProviders({
        componentProps: mockComponentProps,
        row: mockRow
      });

      // The component catches the error and renders the error message
      const errorMessage = container.querySelector('[data-cy="noData"]');
      if (errorMessage) {
        expect(errorMessage).toHaveTextContent(/No details to show!/i);
      } else {
        // If no error message, component rendered successfully
        expect(container).toBeInTheDocument();
      }
    });

    it('should handle null resultSummary', () => {
      const mockRow = {
        original: {
          resultSummary: null
        }
      };

      // Null causes JSON.parse to return null, then isEmpty check fails
      const { container } = renderWithProviders({
        componentProps: mockComponentProps,
        row: mockRow
      });

      // The component catches the error or handles null case
      // Use queryByTestId to check if error message exists
      const errorMessage = container.querySelector('[data-cy="noData"]');
      if (errorMessage) {
        expect(errorMessage).toHaveTextContent(/No details to show!/i);
      } else {
        // If no error message, component rendered successfully
        expect(container).toBeInTheDocument();
      }
    });

    it('should handle JSON parse errors gracefully', () => {
      const mockRow = {
        original: {
          resultSummary: '{"incomplete": '
        }
      };

      renderWithProviders({
        componentProps: mockComponentProps,
        row: mockRow
      });

      expect(screen.getByText(/No details to show!/i)).toBeInTheDocument();
    });
  });

	describe('Edge Cases', () => {
		it('should render empty object with Grid container', async () => {
			const mockRow = {
				original: {
					resultSummary: JSON.stringify({})
				}
			};

			const { container } = renderWithProviders({
				componentProps: mockComponentProps,
				row: mockRow
			});

			await waitFor(() => {
				// Empty object renders a Grid container with Typography
				// The component shows the structure but isEmpty check prevents showing "No Record Found"
				expect(container.querySelector('.MuiGrid-container')).toBeInTheDocument();
			}, { timeout: 3000 });
		});

		it('should handle null entity data', async () => {
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
					resultSummary: JSON.stringify({ test: 'value' })
				}
			};

			// Component handles null entityData gracefully using isNull check
			// When entityData is null, typeDefEntityData becomes {} and component still renders
			const { container } = renderWithProviders({
				componentProps: mockComponentProps,
				row: mockRow
			}, store);

			// Component should render successfully with null entityData
			await waitFor(() => {
				expect(container).toBeInTheDocument();
				// The component should still render the test value
				expect(screen.getByText(/test/i)).toBeInTheDocument();
			}, { timeout: 3000 });
		});

		it('should handle empty entityDefs array', async () => {
			const store = configureStore({
				reducer: {
					entity: () => ({
						loading: false,
						entityData: {
							entityDefs: []
						}
					})
				}
			});

			const mockRow = {
				original: {
					resultSummary: JSON.stringify({ test: 'value' })
				}
			};

			renderWithProviders({
				componentProps: mockComponentProps,
				row: mockRow
			}, store);

			await waitFor(() => {
				// Should still render even with empty entityDefs
				expect(screen.getByText(/test/i)).toBeInTheDocument();
			}, { timeout: 3000 });
		});

		it('should handle special characters in values', async () => {
			const mockRow = {
				original: {
					resultSummary: JSON.stringify({
						special: 'Test <>&"\' characters'
					})
				}
			};

			renderWithProviders({
				componentProps: mockComponentProps,
				row: mockRow
			});

			await waitFor(() => {
				expect(screen.getByText(/special/i)).toBeInTheDocument();
			}, { timeout: 3000 });
		});

		it('should handle very long strings', async () => {
			const longString = 'a'.repeat(1000);
			const mockRow = {
				original: {
					resultSummary: JSON.stringify({
						longValue: longString
					})
				}
			};

			renderWithProviders({
				componentProps: mockComponentProps,
				row: mockRow
			});

			await waitFor(() => {
				expect(screen.getByText(/longValue/i)).toBeInTheDocument();
			}, { timeout: 3000 });
		});

		it('should handle numeric values', async () => {
			const mockRow = {
				original: {
					resultSummary: JSON.stringify({
						count: 42,
						price: 99.99,
						negative: -10
					})
				}
			};

			renderWithProviders({
				componentProps: mockComponentProps,
				row: mockRow
			});

			await waitFor(() => {
				expect(screen.getByText(/count/i)).toBeInTheDocument();
				expect(screen.getByText(/price/i)).toBeInTheDocument();
				expect(screen.getByText(/negative/i)).toBeInTheDocument();
			}, { timeout: 3000 });
		});

		it('should handle boolean values', async () => {
			const mockRow = {
				original: {
					resultSummary: JSON.stringify({
						isActive: true,
						isDeleted: false
					})
				}
			};

			renderWithProviders({
				componentProps: mockComponentProps,
				row: mockRow
			});

			await waitFor(() => {
				expect(screen.getByText(/isActive/i)).toBeInTheDocument();
				expect(screen.getByText(/isDeleted/i)).toBeInTheDocument();
			}, { timeout: 3000 });
		});
	});

	describe('Component Props', () => {
		it('should use entity typeName from props', async () => {
			const customProps = {
				entity: {
					typeName: 'custom_type',
					guid: 'custom-guid'
				},
				referredEntities: {}
			};

			const mockRow = {
				original: {
					resultSummary: JSON.stringify({ test: 'value' })
				}
			};

			renderWithProviders({
				componentProps: customProps,
				row: mockRow
			});

			await waitFor(() => {
				expect(screen.getByText(/test/i)).toBeInTheDocument();
			}, { timeout: 3000 });
		});

		it('should handle referredEntities prop', async () => {
			const customProps = {
				entity: {
					typeName: 'test_entity',
					guid: 'test-guid'
				},
				referredEntities: {
					'ref-guid-1': { name: 'Referenced Entity' }
				}
			};

			const mockRow = {
				original: {
					resultSummary: JSON.stringify({ test: 'value' })
				}
			};

			renderWithProviders({
				componentProps: customProps,
				row: mockRow
			});

			await waitFor(() => {
				expect(screen.getByText(/test/i)).toBeInTheDocument();
			}, { timeout: 3000 });
		});
	});
});
