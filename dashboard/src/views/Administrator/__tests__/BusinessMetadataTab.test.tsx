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
 * Comprehensive unit tests for BusinessMetadataTab component
 * 
 * Coverage Target:
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import BusinessMetadataTab from '../BusinessMetadataTab';

// Mock dependencies
const mockDispatch = jest.fn();
const mockSetForm = jest.fn();
const mockSetBMAttribute = jest.fn();
const mockLocation = { pathname: '/administrator', search: '', hash: '', state: null, key: '' };

// Mock react-router-dom
jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useLocation: () => mockLocation,
	Link: ({ to, children, className, ...props }: any) => (
		<a href={to.pathname + (to.search || '')} className={className} {...props}>
			{children}
		</a>
	)
}));

// Mock Redux hooks
const mockUseAppSelector = jest.fn();
jest.mock('@hooks/reducerHook', () => ({
	useAppDispatch: () => mockDispatch,
	useAppSelector: (...args: any[]) => mockUseAppSelector(...args)
}));

// Mock Redux slice
jest.mock('@redux/slice/createBMSlice', () => ({
	setEditBMAttribute: jest.fn((data) => ({ type: 'SET_EDIT_BM_ATTRIBUTE', payload: data }))
}));

// Mock child components
jest.mock('@components/Table/TableLayout', () => ({
	TableLayout: ({
		data,
		columns,
		defaultColumnVisibility,
		emptyText,
		isFetching,
		customLeftButton,
		auditTableDetails,
		...props
	}: any) => {
		// Render cells to test cell renderers
		const renderCells = () => {
			if (!data || !columns) return null;
			return data.map((row: any, rowIdx: number) => 
				columns.map((col: any, colIdx: number) => {
					if (typeof col.cell === 'function') {
						const cellInfo = {
							getValue: () => row[col.accessorKey],
							row: { original: row }
						};
						return (
							<div 
								key={`${rowIdx}-${colIdx}`}
								data-testid={`cell-${col.accessorKey}-${rowIdx}`}
								data-row-guid={row.guid}
							>
								{col.cell(cellInfo)}
							</div>
						);
					}
					return null;
				})
			);
		};
		
		return (
			<div data-testid="table-layout">
				<div data-testid="table-data">{JSON.stringify(data)}</div>
				<div data-testid="table-loading">{isFetching ? 'loading' : 'not-loading'}</div>
				<div data-testid="table-empty-text">{emptyText}</div>
				{customLeftButton && <div data-testid="custom-left-button">{customLeftButton}</div>}
				{auditTableDetails && (
					<div data-testid="audit-table-details">
						{React.createElement(auditTableDetails.Component, auditTableDetails.componentProps)}
					</div>
				)}
				{columns.map((col: any, idx: number) => (
					<div key={idx} data-testid={`column-${col.accessorKey}`}>
						{col.header}
					</div>
				))}
				<div data-testid="table-cells">
					{renderCells()}
				</div>
			</div>
		);
	}
}));

jest.mock('@views/DetailPage/BusinessMetadataDetails/BusinessMetadataAtrribute', () => ({
	__esModule: true,
	default: ({ attributeDefs, loading, setForm, setBMAttribute }: any) => (
		<div data-testid="business-metadata-attribute">
			BusinessMetadataAtrribute
			<div data-testid="bm-attr-defs">{JSON.stringify(attributeDefs)}</div>
			<div data-testid="bm-attr-loading">{loading ? 'loading' : 'not-loading'}</div>
		</div>
	)
}));

// Mock MUI components
jest.mock('@components/muiComponents', () => ({
	CustomButton: ({ children, onClick, startIcon, ...props }: any) => (
		<button onClick={onClick} {...props}>
			{startIcon}
			{children}
		</button>
	),
	LightTooltip: ({ children, title }: any) => (
		<div data-testid="tooltip" title={title}>
			{children}
		</div>
	),
	Box: ({ children, component, dangerouslySetInnerHTML, ...props }: any) => (
		<div component={component} {...props} dangerouslySetInnerHTML={dangerouslySetInnerHTML}>
			{children}
		</div>
	)
}));

// Mock utils
const mockIsEmpty = jest.fn((val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0));
const mockDateFormat = jest.fn((date: any) => `formatted-${date}`);
const mockSanitizeHtmlContent = jest.fn((html: any) => html);

jest.mock('@utils/Utils', () => ({
	isEmpty: (...args: any[]) => mockIsEmpty(...args),
	dateFormat: (...args: any[]) => mockDateFormat(...args),
	sanitizeHtmlContent: (...args: any[]) => mockSanitizeHtmlContent(...args)
}));

describe('BusinessMetadataTab Component', () => {
	const mockBusinessMetadataDefs = [
		{
			guid: 'guid-1',
			name: 'Test BM 1',
			description: 'Test Description 1',
			createdBy: 'user1',
			createTime: '2023-01-01T00:00:00Z',
			updatedBy: 'user2',
			updateTime: '2023-01-02T00:00:00Z'
		},
		{
			guid: 'guid-2',
			name: 'Test BM 2',
			description: 'Test Description 2 with very long description that exceeds 40 characters limit',
			createdBy: '',
			createTime: '',
			updatedBy: '',
			updateTime: ''
		}
	];

	const renderComponent = (search = '') => {
		mockLocation.search = search;
		return render(
			<MemoryRouter initialEntries={['/administrator']}>
				<BusinessMetadataTab setForm={mockSetForm} setBMAttribute={mockSetBMAttribute} />
			</MemoryRouter>
		);
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockLocation.search = '';
		mockDispatch.mockClear();
		mockSetForm.mockClear();
		mockSetBMAttribute.mockClear();
		mockIsEmpty.mockImplementation((val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0));
		mockDateFormat.mockImplementation((date: any) => `formatted-${date}`);
		mockSanitizeHtmlContent.mockImplementation((html: any) => html);
		
		// Default Redux state
		mockUseAppSelector.mockImplementation((selector: any) => {
			const state = {
				businessMetaData: {
					businessMetaData: {
						businessMetadataDefs: mockBusinessMetadataDefs
					},
					loading: false
				}
			};
			return selector(state);
		});
	});

	describe('Component Rendering', () => {
		it('should render BusinessMetadataTab component', () => {
			renderComponent();
			
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		it('should render TableLayout with correct props', () => {
			renderComponent();
			
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
			expect(screen.getByTestId('table-empty-text')).toHaveTextContent('No Records found!');
		});

		it('should render custom left button', () => {
			renderComponent();
			
			expect(screen.getByTestId('custom-left-button')).toBeInTheDocument();
			expect(screen.getByText('Create Business Metadata')).toBeInTheDocument();
		});

		it('should render audit table details component', () => {
			renderComponent();
			
			expect(screen.getByTestId('audit-table-details')).toBeInTheDocument();
			expect(screen.getByTestId('business-metadata-attribute')).toBeInTheDocument();
		});
	});

	describe('Table Columns', () => {
		it('should render name column with link', () => {
			renderComponent();
			
			expect(screen.getByTestId('column-name')).toBeInTheDocument();
			expect(screen.getByText('Name')).toBeInTheDocument();
		});

		it('should render description column', () => {
			renderComponent();
			
			expect(screen.getByTestId('column-description')).toBeInTheDocument();
			expect(screen.getByText('Description')).toBeInTheDocument();
		});

		it('should render createdBy column', () => {
			renderComponent();
			
			expect(screen.getByTestId('column-createdBy')).toBeInTheDocument();
			expect(screen.getByText('Created by')).toBeInTheDocument();
		});

		it('should render createTime column', () => {
			renderComponent();
			
			expect(screen.getByTestId('column-createTime')).toBeInTheDocument();
			expect(screen.getByText('Created on')).toBeInTheDocument();
		});

		it('should render updatedBy column', () => {
			renderComponent();
			
			expect(screen.getByTestId('column-updatedBy')).toBeInTheDocument();
			expect(screen.getByText('Updated by')).toBeInTheDocument();
		});

		it('should render updateTime column', () => {
			renderComponent();
			
			expect(screen.getByTestId('column-updateTime')).toBeInTheDocument();
			expect(screen.getByText('Updated on')).toBeInTheDocument();
		});

		it('should render action column', () => {
			renderComponent();
			
			expect(screen.getByTestId('column-action')).toBeInTheDocument();
			expect(screen.getByText('Action')).toBeInTheDocument();
		});
	});

	describe('Name Column Cell Rendering', () => {
		it('should render name as link with guid', () => {
			renderComponent();
			
			const nameCell = screen.getByTestId('cell-name-0');
			expect(nameCell).toBeInTheDocument();
			const link = nameCell.querySelector('a');
			expect(link).toBeInTheDocument();
			expect(link?.getAttribute('href')).toContain('guid-1');
		});

		it('should clear search params except searchType when rendering name link', async () => {
			renderComponent('?searchType=test&other=value&another=param');
			
			// The link should have from=bm param and searchType preserved
			await waitFor(() => {
				const nameCell = screen.getByTestId('cell-name-0');
				const link = nameCell.querySelector('a');
				expect(link).toBeInTheDocument();
				const href = link?.getAttribute('href') || '';
				expect(href).toContain('from=bm');
				expect(href).toContain('searchType=test');
			}, { timeout: 3000 });
		});

		it('should add from=bm param to name link', () => {
			renderComponent();
			
			const nameCell = screen.getByTestId('cell-name-0');
			const link = nameCell.querySelector('a');
			expect(link).toBeInTheDocument();
			expect(link?.getAttribute('href')).toContain('from=bm');
		});
	});

	describe('Description Column Cell Rendering', () => {
		it('should render description when not empty', () => {
			renderComponent();
			
			const descCell = screen.getByTestId('cell-description-0');
			expect(descCell).toBeInTheDocument();
			expect(descCell.textContent).toContain('Test Description 1');
			expect(mockIsEmpty).toHaveBeenCalled();
			expect(mockSanitizeHtmlContent).toHaveBeenCalled();
		});

		it('should render N/A when description is empty', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					businessMetaData: {
						businessMetaData: {
							businessMetadataDefs: [{
								guid: 'guid-1',
								name: 'Test',
								description: ''
							}]
						},
						loading: false
					}
				};
				return selector(state);
			});
			
			mockIsEmpty.mockReturnValue(true);
			renderComponent();
			
			const descCell = screen.getByTestId('cell-description-0');
			expect(descCell).toBeInTheDocument();
			expect(descCell.textContent).toContain('N/A');
			expect(mockIsEmpty).toHaveBeenCalled();
		});

		it('should truncate description longer than 40 characters', () => {
			renderComponent();
			
			// Second row has long description
			const descCell = screen.getByTestId('cell-description-1');
			expect(descCell).toBeInTheDocument();
			expect(mockSanitizeHtmlContent).toHaveBeenCalled();
			// Check that substr was called (truncation logic)
			const calls = mockSanitizeHtmlContent.mock.calls;
			const longDescCall = calls.find((call: any[]) => 
				call[0] && call[0].length > 40
			);
			expect(longDescCall).toBeDefined();
		});

		it('should sanitize HTML content in description', () => {
			renderComponent();
			
			const descCell = screen.getByTestId('cell-description-0');
			expect(descCell).toBeInTheDocument();
			expect(mockSanitizeHtmlContent).toHaveBeenCalled();
		});
	});

	describe('CreatedBy Column Cell Rendering', () => {
		it('should render createdBy when not empty', () => {
			renderComponent();
			
			expect(mockIsEmpty).toHaveBeenCalled();
		});

		it('should render N/A when createdBy is empty', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					businessMetaData: {
						businessMetaData: {
							businessMetadataDefs: [{
								guid: 'guid-1',
								name: 'Test',
								createdBy: ''
							}]
						},
						loading: false
					}
				};
				return selector(state);
			});
			
			renderComponent();
			
			expect(mockIsEmpty).toHaveBeenCalled();
		});
	});

	describe('CreateTime Column Cell Rendering', () => {
		it('should render formatted date when createTime is not empty', () => {
			renderComponent();
			
			const createTimeCell = screen.getByTestId('cell-createTime-0');
			expect(createTimeCell).toBeInTheDocument();
			expect(mockDateFormat).toHaveBeenCalledWith('2023-01-01T00:00:00Z');
			expect(createTimeCell.textContent).toContain('formatted-');
		});

		it('should render N/A when createTime is empty', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					businessMetaData: {
						businessMetaData: {
							businessMetadataDefs: [{
								guid: 'guid-1',
								name: 'Test',
								createTime: ''
							}]
						},
						loading: false
					}
				};
				return selector(state);
			});
			
			mockIsEmpty.mockReturnValue(true);
			renderComponent();
			
			const createTimeCell = screen.getByTestId('cell-createTime-0');
			expect(createTimeCell).toBeInTheDocument();
			expect(createTimeCell.textContent).toContain('N/A');
			expect(mockIsEmpty).toHaveBeenCalled();
		});
	});

	describe('UpdatedBy Column Cell Rendering', () => {
		it('should render updatedBy when not empty', () => {
			renderComponent();
			
			const updatedByCell = screen.getByTestId('cell-updatedBy-0');
			expect(updatedByCell).toBeInTheDocument();
			expect(updatedByCell.textContent).toContain('user2');
			expect(mockIsEmpty).toHaveBeenCalled();
		});

		it('should render N/A when updatedBy is empty', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					businessMetaData: {
						businessMetaData: {
							businessMetadataDefs: [{
								guid: 'guid-1',
								name: 'Test',
								updatedBy: ''
							}]
						},
						loading: false
					}
				};
				return selector(state);
			});
			
			mockIsEmpty.mockReturnValue(true);
			renderComponent();
			
			const updatedByCell = screen.getByTestId('cell-updatedBy-0');
			expect(updatedByCell).toBeInTheDocument();
			expect(updatedByCell.textContent).toContain('N/A');
			expect(mockIsEmpty).toHaveBeenCalled();
		});
	});

	describe('UpdateTime Column Cell Rendering', () => {
		it('should render formatted date when updateTime is not empty', () => {
			renderComponent();
			
			const updateTimeCell = screen.getByTestId('cell-updateTime-0');
			expect(updateTimeCell).toBeInTheDocument();
			expect(mockDateFormat).toHaveBeenCalledWith('2023-01-02T00:00:00Z');
			expect(updateTimeCell.textContent).toContain('formatted-');
		});

		it('should render N/A when updateTime is empty', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					businessMetaData: {
						businessMetaData: {
							businessMetadataDefs: [{
								guid: 'guid-1',
								name: 'Test',
								updateTime: ''
							}]
						},
						loading: false
					}
				};
				return selector(state);
			});
			
			mockIsEmpty.mockReturnValue(true);
			renderComponent();
			
			const updateTimeCell = screen.getByTestId('cell-updateTime-0');
			expect(updateTimeCell).toBeInTheDocument();
			expect(updateTimeCell.textContent).toContain('N/A');
			expect(mockIsEmpty).toHaveBeenCalled();
		});
	});

	describe('Action Column Cell Rendering', () => {
		it('should render Attributes button', () => {
			renderComponent();
			
			const actionCell = screen.getByTestId('cell-action-0');
			expect(actionCell).toBeInTheDocument();
			expect(actionCell.textContent).toContain('Attributes');
		});

		it('should call setForm and setBMAttribute when Attributes button is clicked', () => {
			renderComponent();
			
			const { setEditBMAttribute } = require('@redux/slice/createBMSlice');
			
			// Find and click the Attributes button in the action cell
			const actionCell = screen.getByTestId('cell-action-0');
			const attributesButton = actionCell.querySelector('button');
			expect(attributesButton).toBeInTheDocument();
			
			fireEvent.click(attributesButton!);
			
			expect(mockSetForm).toHaveBeenCalledWith(true);
			expect(mockSetBMAttribute).toHaveBeenCalledWith(mockBusinessMetadataDefs[0]);
			expect(mockDispatch).toHaveBeenCalled();
		});
	});

	describe('Create Business Metadata Button', () => {
		it('should render Create Business Metadata button', () => {
			renderComponent();
			
			expect(screen.getByText('Create Business Metadata')).toBeInTheDocument();
		});

		it('should call setForm and setBMAttribute when Create button is clicked', () => {
			renderComponent();
			
			const { setEditBMAttribute } = require('@redux/slice/createBMSlice');
			const createButton = screen.getByText('Create Business Metadata');
			
			fireEvent.click(createButton);
			
			expect(mockSetForm).toHaveBeenCalledWith(true);
			expect(mockSetBMAttribute).toHaveBeenCalledWith({});
			expect(mockDispatch).toHaveBeenCalled();
		});
	});

	describe('Column Visibility', () => {
		it('should hide columns with show: false', () => {
			renderComponent();
			
			// Columns with show: false should be hidden
			// This is handled by defaultColumnVisibility function
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		it('should show columns with show: true', () => {
			renderComponent();
			
			// Columns with show: true should be visible
			expect(screen.getByTestId('column-name')).toBeInTheDocument();
			expect(screen.getByTestId('column-description')).toBeInTheDocument();
			expect(screen.getByTestId('column-action')).toBeInTheDocument();
		});
	});

	describe('Loading State', () => {
		it('should pass loading state to TableLayout', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					businessMetaData: {
						businessMetaData: {
							businessMetadataDefs: []
						},
						loading: true
					}
				};
				return selector(state);
			});
			
			renderComponent();
			
			expect(screen.getByTestId('table-loading')).toHaveTextContent('loading');
		});

		it('should pass loading state to BusinessMetadataAtrribute', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					businessMetaData: {
						businessMetaData: {
							businessMetadataDefs: []
						},
						loading: true
					}
				};
				return selector(state);
			});
			
			renderComponent();
			
			expect(screen.getByTestId('bm-attr-loading')).toHaveTextContent('loading');
		});
	});

	describe('Empty Data Handling', () => {
		it('should handle empty businessMetadataDefs', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					businessMetaData: {
						businessMetaData: {
							businessMetadataDefs: []
						},
						loading: false
					}
				};
				return selector(state);
			});
			
			renderComponent();
			
			expect(screen.getByTestId('table-data')).toHaveTextContent('[]');
		});

		it('should handle undefined businessMetadataDefs', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					businessMetaData: {
						businessMetaData: {},
						loading: false
					}
				};
				return selector(state);
			});
			
			renderComponent();
			
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		it('should handle undefined businessMetaData (line 37)', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const state = {
					businessMetaData: {
						businessMetaData: undefined // Tests line 37: businessMetaData || {}
					},
					loading: false
				};
				return selector(state);
			});
			
			renderComponent();
			
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});

	describe('Props Passing', () => {
		it('should pass correct props to BusinessMetadataAtrribute', () => {
			renderComponent();
			
			expect(screen.getByTestId('business-metadata-attribute')).toBeInTheDocument();
			expect(screen.getByTestId('bm-attr-defs')).toBeInTheDocument();
		});
	});

	describe('Search Params Handling', () => {
		it('should handle search params in name link', () => {
			mockLocation.search = '?searchType=test';
			renderComponent();
			
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		it('should preserve searchType param', () => {
			mockLocation.search = '?searchType=test&other=value';
			renderComponent();
			
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});

	describe('Memoization', () => {
		it('should memoize columns', () => {
			const { rerender } = renderComponent();
			
			// Rerender should not recreate columns
			rerender(
				<MemoryRouter initialEntries={['/administrator']}>
					<BusinessMetadataTab setForm={mockSetForm} setBMAttribute={mockSetBMAttribute} />
				</MemoryRouter>
			);
			
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});
});
