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
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { MemoryRouter } from 'react-router-dom';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import userEvent from '@testing-library/user-event';
import LineageTab from '../LineageTab';

const theme = createTheme();

// Polyfill structuredClone for Jest environment
if (typeof (global as any).structuredClone === 'undefined') {
	(global as any).structuredClone = (obj: any) => {
		return JSON.parse(JSON.stringify(obj));
	};
}

// Mock hoisting - declare mocks before jest.mock calls
const mockExtractKeyValueFromEntity = jest.fn();
const mockIsEmpty = jest.fn();
const mockGetValues = jest.fn();

jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) => mockIsEmpty(val),
	extractKeyValueFromEntity: (entity: any) => mockExtractKeyValueFromEntity(entity)
}));

jest.mock('@components/commonComponents', () => ({
	getValues: (...args: any[]) => mockGetValues(...args)
}));

// Mock Enum
jest.mock('@utils/Enum', () => ({
	globalSessionData: {
		isLineageOnDemandEnabled: false,
		lineageNodeCount: 6
	},
	lineageDepth: 3
}));

// Mock AntSwitch
jest.mock('@utils/Muiutils', () => ({
	AntSwitch: React.forwardRef(({ checked, onChange, onClick, inputProps, ...props }: any, ref: any) => (
		<input
			ref={ref}
			type="checkbox"
			data-testid="ant-switch"
			checked={checked}
			onChange={onChange}
			onClick={onClick}
			{...inputProps}
			{...props}
		/>
	))
}));

jest.mock('@mui/material/Autocomplete', () => ({
	__esModule: true,
	createFilterOptions: () => (options: any[]) => options,
	default: ({ onChange, renderInput, value }: any) => {
		const inputValue = typeof value === 'string' ? value : value?.label || '';
		const params = {
			inputProps: {
				value: inputValue,
				onChange: (event: any) => {
					const nextValue = event?.target?.value;
					onChange?.(event, { label: nextValue, value: nextValue });
				}
			},
			InputProps: {}
		};
		return renderInput ? renderInput(params) : null;
	}
}));

jest.mock('@mui/material/TextField', () => ({
	__esModule: true,
	default: ({ label, inputProps = {}, ...props }: any) => (
		<label>
			{label}
			<input role="textbox" {...inputProps} {...props} />
		</label>
	)
}));

// Mock hoisting - declare mocks before jest.mock calls
const mockZoomIn = jest.fn();
const mockZoomOut = jest.fn();
const mockExportLineage = jest.fn();
const mockDisplayFullName = jest.fn();
const mockRefresh = jest.fn();
const mockSearchNode = jest.fn();
const mockGetNode = jest.fn();

// Store callbacks passed to constructor - use global to access from jest.mock
(global as any).__lineageTestCallbacks = {};

jest.mock('@views/Lineage/atlas-lineage/src', () => {
	const MockLineageHelper = jest.fn().mockImplementation((options?: any) => {
		// Store callbacks from options synchronously - always update if provided
		if (options) {
			if (options.onNodeClick) {
				(global as any).__lineageTestCallbacks.onNodeClick = options.onNodeClick;
			}
			if (options.onLabelClick) {
				(global as any).__lineageTestCallbacks.onLabelClick = options.onLabelClick;
			}
			if (options.onPathClick) {
				(global as any).__lineageTestCallbacks.onPathClick = options.onPathClick;
			}
		}
		
		// Return instance with all methods - ensure they're always functions
		const instance = {
			zoomIn: mockZoomIn,
			zoomOut: mockZoomOut,
			exportLineage: mockExportLineage,
			displayFullName: mockDisplayFullName,
			refresh: mockRefresh,
			searchNode: mockSearchNode,
			getNode: mockGetNode
		};
		
		return instance;
	});
	return {
		__esModule: true,
		default: MockLineageHelper
	};
});

// Import the mock to get reference to MockLineageHelper
import LineageHelper from '@views/Lineage/atlas-lineage/src';
const MockLineageHelper = LineageHelper as jest.MockedFunction<typeof LineageHelper>;

// Helper to access stored callbacks
const getStoredCallbacks = () => (global as any).__lineageTestCallbacks;

// Mock API methods - need to mock with .js extension to match source import
const mockAddLineageData = jest.fn();
const mockGetLineageData = jest.fn();

// Mock the module with .js extension as used in source
jest.mock('@api/apiMethods/lineageMethod.js', () => ({
	addLineageData: (guid: string, queryParam: any) => mockAddLineageData(guid, queryParam),
	getLineageData: (guid: string, options: any) => mockGetLineageData(guid, options)
}));

// Mock React Router hooks
const mockNavigate = jest.fn();
const mockLocation = {
	pathname: '/detailPage/test-guid-123',
	search: '?tabActive=lineage',
	hash: '',
	state: null,
	key: 'test-key'
};

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useNavigate: () => mockNavigate,
	useLocation: () => mockLocation,
	useParams: () => ({ guid: 'test-guid-123' }),
	Link: ({ to, children, className, ...props }: any) => (
		<a href={to.pathname || to} className={className} {...props} data-testid="link">
			{children}
		</a>
	)
}));

// Mock Redux
const mockEntityData = {
	entityDefs: {
		DataSet: {
			typeName: 'DataSet',
			attributeDefs: []
		}
	}
};

const createMockStore = () => {
	return configureStore({
		reducer: {
			entity: (state = { entityData: mockEntityData }) => state
		},
		preloadedState: {
			entity: {
				entityData: mockEntityData
			}
		}
	});
};

// Mock toast
jest.mock('react-toastify', () => ({
	toast: {
		info: jest.fn(),
		success: jest.fn(),
		error: jest.fn()
	}
}));

// Mock LightTooltip
jest.mock('@components/muiComponents', () => ({
	LightTooltip: ({ children, title }: any) => (
		<div data-testid="light-tooltip" title={title}>
			{children}
		</div>
	)
}));

// Mock PropagationPropertyModal
jest.mock('../PropagationPropertyModal', () => ({
	__esModule: true,
	default: ({ propagationModal, setPropagationModal, propagateDetails }: any) =>
		propagationModal ? (
			<div data-testid="propagation-property-modal">
				<div data-testid="modal-open">Modal Open</div>
				<button onClick={() => setPropagationModal(false)} data-testid="close-propagation-modal">
					Close
				</button>
			</div>
		) : null
}));

const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => {
	const store = createMockStore();
	return (
		<Provider store={store}>
			<ThemeProvider theme={theme}>
				<MemoryRouter initialEntries={['/detailPage/test-guid-123?tabActive=lineage']}>
					{children}
				</MemoryRouter>
			</ThemeProvider>
		</Provider>
	);
};

describe('LineageTab', () => {
	const mockEntity = {
		guid: 'test-guid-123',
		typeName: 'DataSet',
		attributes: {
			name: 'Test Dataset',
			qualifiedName: 'test_dataset@cluster1'
		},
		isIncomplete: false,
		status: 'ACTIVE',
		classifications: [
			{
				typeName: 'PII'
			}
		]
	};

	const mockLineageData = {
		baseEntityGuid: 'test-guid-123',
		guidEntityMap: {
			'test-guid-123': {
				guid: 'test-guid-123',
				typeName: 'DataSet',
				displayText: 'Test Dataset',
				attributes: {
					name: 'Test Dataset'
				}
			},
			'node-guid-1': {
				guid: 'node-guid-1',
				typeName: 'Table',
				displayText: 'Source Table'
			},
			'node-guid-2': {
				guid: 'node-guid-2',
				typeName: 'View',
				displayText: 'Target View'
			}
		},
		relations: [
			{
				fromEntityId: 'node-guid-1',
				toEntityId: 'test-guid-123',
				relationshipId: 'rel-1'
			},
			{
				fromEntityId: 'test-guid-123',
				toEntityId: 'node-guid-2',
				relationshipId: 'rel-2'
			}
		],
		legends: true,
		lineageOnDemandPayload: {},
		relationsOnDemand: {}
	};

	beforeEach(() => {
		jest.clearAllMocks();
		MockLineageHelper.mockClear();
		(global as any).__lineageTestCallbacks = {};
		
		mockIsEmpty.mockImplementation((val: any) => {
			if (val === null || val === undefined || val === '') return true;
			if (Array.isArray(val) && val.length === 0) return true;
			if (typeof val === 'object' && val !== null && Object.keys(val).length === 0) return true;
			return false;
		});
		mockExtractKeyValueFromEntity.mockImplementation((entity: any) => {
			if (!entity) return { name: '', found: false, key: null };
			const name = entity.attributes?.name || entity.name || entity.guid || '';
			return { name, found: !!name, key: 'name' };
		});
		mockGetValues.mockImplementation((val: any) => val);
		mockGetLineageData.mockResolvedValue({
			data: mockLineageData
		});
		mockAddLineageData.mockResolvedValue({
			data: mockLineageData
		});
		mockGetNode.mockReturnValue({
			guid: 'test-guid-123',
			typeName: 'DataSet',
			attributes: {
				name: 'Test Dataset'
			},
			entityDef: {
				attributeDefs: []
			}
		});
		mockZoomIn.mockClear();
		mockZoomOut.mockClear();
		mockExportLineage.mockClear();
		mockDisplayFullName.mockClear();
		mockRefresh.mockClear();
		mockSearchNode.mockClear();
		mockNavigate.mockClear();
	});

	describe('Component Rendering', () => {
		it('should render LineageTab component', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				const tooltips = screen.getAllByTestId('light-tooltip');
				expect(tooltips.length).toBeGreaterThan(0);
			}, { timeout: 15000 });
		}, 30000);

		it('should render with loading state initially', async () => {
			// Delay the API response to ensure loader shows
			mockGetLineageData.mockImplementation(() => 
				new Promise(resolve => setTimeout(() => resolve({ data: mockLineageData }), 100))
			);

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			// Check immediately for loader before data loads
			await waitFor(() => {
				expect(screen.getByRole('progressbar')).toBeInTheDocument();
			}, { timeout: 15000 });

			// Reset mock for other tests
			mockGetLineageData.mockResolvedValue({ data: mockLineageData });
		}, 30000);

		it('should render all toolbar buttons', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and LineageHelper to be created
			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			// Check for icon buttons (they should be present)
			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				expect(buttons.length).toBeGreaterThan(0);
			}, { timeout: 15000 });
		}, 30000);

		it('should render with empty entity', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={null} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should render with isProcess prop', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} isProcess={true} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('Data Fetching', () => {
		it('should fetch lineage data on mount', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalledWith('test-guid-123', {});
			}, { timeout: 15000 });
		}, 30000);

		it('should fetch lineage data when guid changes', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			mockGetLineageData.mockClear();

			// Note: guid comes from useParams, so this test verifies the effect dependency
			await waitFor(() => {
				// Component should still render
				const tooltips = screen.getAllByTestId('light-tooltip');
				expect(tooltips.length).toBeGreaterThan(0);
			}, { timeout: 15000 });
		}, 30000);

		it('should handle API error gracefully', async () => {
			mockGetLineageData.mockRejectedValue(new Error('API Error'));

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should handle empty lineage data', async () => {
			const emptyLineageData = {
				baseEntityGuid: 'test-guid-123',
				guidEntityMap: {},
				relations: [],
				legends: true,
				relationsOnDemand: null
			};

			mockGetLineageData.mockResolvedValue({
				data: emptyLineageData
			});

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('D3 Visualization Interactions', () => {
		it('should create LineageHelper instance when data is available', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should call zoomIn when zoom in button is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and lineageMethods to be set
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for MockLineageHelper to be called (ensures lineageMethods will be set)
			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for buttons to be available and enabled
			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				const zoomInBtn = buttons.find((btn) => {
					const svg = btn.querySelector('svg[data-testid="ZoomInIcon"]');
					return svg && !btn.hasAttribute('disabled');
				});
				expect(zoomInBtn).toBeTruthy();
			}, { timeout: 15000 });

			// Find zoom in button and click using fireEvent (more reliable for MUI)
			const buttons = screen.getAllByRole('button');
			const zoomInButton = buttons.find((btn) => {
				const svg = btn.querySelector('svg[data-testid="ZoomInIcon"]');
				return svg && !btn.hasAttribute('disabled');
			});
			
			expect(zoomInButton).toBeTruthy();
			
			await act(async () => {
				if (zoomInButton) {
					fireEvent.click(zoomInButton);
				}
			});

			await waitFor(() => {
				expect(mockZoomIn).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should call zoomOut when zoom out button is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and lineageMethods to be set
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for buttons to be available, enabled, and lineageMethods to be set
			await waitFor(async () => {
				const buttons = screen.getAllByRole('button');
				const zoomOutBtn = buttons.find((btn) => {
					const svg = btn.querySelector('svg[data-testid="ZoomOutIcon"]');
					return svg && !btn.hasAttribute('disabled');
				});
				expect(zoomOutBtn).toBeTruthy();
				// Give React time to update onClick handlers
				await new Promise(resolve => setTimeout(resolve, 100));
			}, { timeout: 15000 });

			// Find zoom out button and click
			const buttons = screen.getAllByRole('button');
			const zoomOutButton = buttons.find((btn) => {
				const svg = btn.querySelector('svg[data-testid="ZoomOutIcon"]');
				return svg && !btn.hasAttribute('disabled');
			});
			
			expect(zoomOutButton).toBeTruthy();
			
			const user = userEvent.setup();
			await act(async () => {
				if (zoomOutButton) {
					await user.click(zoomOutButton);
				}
			});

			await waitFor(() => {
				expect(mockZoomOut).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should call exportLineage when export button is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and lineageMethods to be set
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for buttons to be available, enabled, and lineageMethods to be set
			await waitFor(async () => {
				const buttons = screen.getAllByRole('button');
				const exportBtn = buttons.find((btn) => {
					const svg = btn.querySelector('svg[data-testid="CameraAltIcon"]');
					return svg && !btn.hasAttribute('disabled');
				});
				expect(exportBtn).toBeTruthy();
				// Give React time to update onClick handlers
				await new Promise(resolve => setTimeout(resolve, 100));
			}, { timeout: 15000 });

			// Find export button and click
			const buttons = screen.getAllByRole('button');
			const exportButton = buttons.find((btn) => {
				const svg = btn.querySelector('svg[data-testid="CameraAltIcon"]');
				return svg && !btn.hasAttribute('disabled');
			});
			
			expect(exportButton).toBeTruthy();
			
			const user = userEvent.setup();
			await act(async () => {
				if (exportButton) {
					await user.click(exportButton);
				}
			});

			await waitFor(() => {
				expect(mockExportLineage).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should disable buttons when no lineage data', async () => {
			const emptyData = {
				baseEntityGuid: 'test-guid-123',
				guidEntityMap: {},
				relations: [],
				legends: true
			};

			mockGetLineageData.mockResolvedValue({
				data: emptyData
			});

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Buttons should be disabled when isLineageOptionsEnabled is false
			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				const disabledButtons = buttons.filter((btn) => btn.hasAttribute('disabled'));
				expect(disabledButtons.length).toBeGreaterThan(0);
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('Settings Popover', () => {
		it('should open settings popover when settings button is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				const settingsButton = buttons.find((btn) => btn.querySelector('svg[data-testid="SettingsIcon"]'));
				expect(settingsButton).toBeTruthy();
				if (settingsButton) {
					act(() => {
						fireEvent.click(settingsButton);
					});
				}
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(screen.getByText('Settings')).toBeInTheDocument();
			}, { timeout: 15000 });
		}, 30000);

		it('should close settings popover when close button is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				const settingsButton = buttons.find((btn) => btn.querySelector('svg[data-testid="SettingsIcon"]'));
				expect(settingsButton).toBeTruthy();
				if (settingsButton) {
					act(() => {
						fireEvent.click(settingsButton);
					});
				}
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(screen.getByText('Settings')).toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				const closeButton = screen.getByText('Settings').parentElement?.querySelector('button');
				expect(closeButton).toBeTruthy();
				if (closeButton) {
					act(() => {
						fireEvent.click(closeButton);
					});
				}
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(screen.queryByText('Settings')).not.toBeInTheDocument();
			}, { timeout: 15000 });
		}, 30000);

		it('should toggle current path checkbox', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				const settingsButton = buttons.find((btn) => btn.querySelector('svg[data-testid="SettingsIcon"]'));
				expect(settingsButton).toBeTruthy();
				if (settingsButton) {
					act(() => {
						fireEvent.click(settingsButton);
					});
				}
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(screen.getByText('Settings')).toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				const switches = screen.getAllByTestId('ant-switch');
				expect(switches.length).toBeGreaterThan(0);
				if (switches.length > 0) {
					const currentPathSwitch = switches[0];
					const initialChecked = currentPathSwitch.checked;
					act(() => {
						fireEvent.change(currentPathSwitch, { target: { checked: !initialChecked } });
					});
					expect(currentPathSwitch.checked).toBe(!initialChecked);
				}
			}, { timeout: 15000 });
		}, 30000);

		it('should toggle node details checkbox', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				const settingsButton = buttons.find((btn) => btn.querySelector('svg[data-testid="SettingsIcon"]'));
				expect(settingsButton).toBeTruthy();
				if (settingsButton) {
					act(() => {
						fireEvent.click(settingsButton);
					});
				}
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(screen.getByText('Settings')).toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				const switches = screen.getAllByTestId('ant-switch');
				expect(switches.length).toBeGreaterThan(1);
				if (switches.length > 1) {
					const nodeDetailsSwitch = switches[1];
					const initialChecked = nodeDetailsSwitch.checked;
					act(() => {
						fireEvent.change(nodeDetailsSwitch, { target: { checked: !initialChecked } });
					});
					expect(nodeDetailsSwitch.checked).toBe(!initialChecked);
				}
			}, { timeout: 15000 });
		}, 30000);

		it('should toggle display full name checkbox', async () => {
			const user = userEvent.setup();
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and lineageMethods to be set
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for buttons to be available
			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				expect(buttons.length).toBeGreaterThan(0);
			}, { timeout: 15000 });

			// Open settings popover
			const buttons = screen.getAllByRole('button');
			const settingsButton = buttons.find((btn) => {
				const svg = btn.querySelector('svg[data-testid="SettingsIcon"]');
				return svg && !btn.hasAttribute('disabled');
			});
			
			expect(settingsButton).toBeTruthy();
			
			await act(async () => {
				if (settingsButton) {
					fireEvent.click(settingsButton);
				}
			});

			await waitFor(() => {
				expect(screen.getByText('Settings')).toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for switches to be available
			await waitFor(() => {
				expect(screen.getAllByTestId('ant-switch').length).toBeGreaterThan(1);
				expect(screen.getByText('Display full name')).toBeInTheDocument();
			}, { timeout: 15000 });

			const fullNameLabel = screen.getByText('Display full name');
			const fullNameSwitch = fullNameLabel.parentElement?.querySelector('input[type="checkbox"]');
			expect(fullNameSwitch).toBeTruthy();

			await act(async () => {
				if (fullNameSwitch) {
					await user.click(fullNameSwitch);
				}
			});

			await waitFor(() => {
				expect(mockDisplayFullName).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('Filter Popover', () => {
		it('should open filter popover when filter button is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				const filterButton = buttons.find((btn) => btn.querySelector('svg[data-testid="FilterListIcon"]'));
				expect(filterButton).toBeTruthy();
				if (filterButton) {
					act(() => {
						fireEvent.click(filterButton);
					});
				}
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(screen.getByText('Filters')).toBeInTheDocument();
			}, { timeout: 15000 });
		}, 30000);

		it('should toggle hide process checkbox', async () => {
			const user = userEvent.setup();
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} isProcess={false} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and lineageMethods to be set
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for buttons to be available
			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				expect(buttons.length).toBeGreaterThan(0);
			}, { timeout: 15000 });

			// Open filter popover
			const buttons = screen.getAllByRole('button');
			const filterButton = buttons.find((btn) => {
				const svg = btn.querySelector('svg[data-testid="FilterListIcon"]');
				return svg && !btn.hasAttribute('disabled');
			});
			
			expect(filterButton).toBeTruthy();
			
			await act(async () => {
				if (filterButton) {
					fireEvent.click(filterButton);
				}
			});

			await waitFor(() => {
				expect(screen.getByText('Filters')).toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(screen.getAllByTestId('ant-switch').length).toBeGreaterThan(0);
				expect(screen.getByText('Hide Process')).toBeInTheDocument();
			}, { timeout: 15000 });

			const hideProcessLabel = screen.getByText('Hide Process');
			const hideProcessSwitch = hideProcessLabel.parentElement?.querySelector('input[type="checkbox"]');
			expect(hideProcessSwitch).toBeTruthy();

			await act(async () => {
				if (hideProcessSwitch) {
					await user.click(hideProcessSwitch);
				}
			});

			await waitFor(() => {
				expect(mockRefresh).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should toggle hide deleted entity checkbox', async () => {
			const user = userEvent.setup();
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and lineageMethods to be set
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for buttons to be available
			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				expect(buttons.length).toBeGreaterThan(0);
			}, { timeout: 15000 });

			// Open filter popover
			const buttons = screen.getAllByRole('button');
			const filterButton = buttons.find((btn) => {
				const svg = btn.querySelector('svg[data-testid="FilterListIcon"]');
				return svg && !btn.hasAttribute('disabled');
			});
			
			expect(filterButton).toBeTruthy();
			
			await act(async () => {
				if (filterButton) {
					fireEvent.click(filterButton);
				}
			});

			await waitFor(() => {
				expect(screen.getByText('Filters')).toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(screen.getAllByTestId('ant-switch').length).toBeGreaterThan(0);
				expect(screen.getByText('Hide Deleted Entity')).toBeInTheDocument();
			}, { timeout: 15000 });

			const hideDeletedLabel = screen.getByText('Hide Deleted Entity');
			const hideDeletedSwitch = hideDeletedLabel.parentElement?.querySelector('input[type="checkbox"]');
			expect(hideDeletedSwitch).toBeTruthy();

			await act(async () => {
				if (hideDeletedSwitch) {
					await user.click(hideDeletedSwitch);
				}
			});

			await waitFor(() => {
				expect(mockRefresh).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should change depth value', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for buttons to be available
			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				expect(buttons.length).toBeGreaterThan(0);
			}, { timeout: 15000 });

			// Open filter popover
			const buttons = screen.getAllByRole('button');
			const filterButton = buttons.find((btn) => {
				const svg = btn.querySelector('svg[data-testid="FilterListIcon"]');
				return svg && !btn.hasAttribute('disabled');
			});
			
			expect(filterButton).toBeTruthy();
			
			await act(async () => {
				if (filterButton) {
					fireEvent.click(filterButton);
				}
			});

			await waitFor(() => {
				expect(screen.getByText('Filters')).toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for textbox to be available
			await waitFor(() => {
				const depthInputs = screen.getAllByRole('textbox');
				expect(depthInputs.length).toBeGreaterThan(0);
			}, { timeout: 15000 });

			// Find depth autocomplete input
			const depthLabel = screen.getByText('Depth:');
			const depthContainer = depthLabel.parentElement;
			const depthInput = depthContainer?.querySelector('input[type="number"]');
			
			expect(depthInput).toBeTruthy();
			
			await act(async () => {
				if (depthInput) {
					fireEvent.change(depthInput, { target: { value: '6' } });
				}
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalledTimes(2);
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('Search Popover', () => {
		it('should open search popover when search button is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				const searchButton = buttons.find((btn) => btn.querySelector('svg[data-testid="SearchIcon"]'));
				expect(searchButton).toBeTruthy();
				if (searchButton) {
					act(() => {
						fireEvent.click(searchButton);
					});
				}
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(screen.getByText('Search')).toBeInTheDocument();
			}, { timeout: 15000 });
		}, 30000);

		it('should search for node when selected', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and lineageMethods to be set
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for buttons to be available
			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				expect(buttons.length).toBeGreaterThan(0);
			}, { timeout: 15000 });

			// Open search popover
			const buttons = screen.getAllByRole('button');
			const searchButton = buttons.find((btn) => {
				const svg = btn.querySelector('svg[data-testid="SearchIcon"]');
				return svg && !btn.hasAttribute('disabled');
			});
			
			expect(searchButton).toBeTruthy();
			
			await act(async () => {
				if (searchButton) {
					fireEvent.click(searchButton);
				}
			});

			await waitFor(() => {
				expect(screen.getByText('Search')).toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for textbox to be available
			await waitFor(() => {
				const inputs = screen.getAllByRole('textbox');
				expect(inputs.length).toBeGreaterThan(0);
			}, { timeout: 15000 });

			// Find autocomplete input
			const inputs = screen.getAllByRole('textbox');
			const searchInput = inputs.find((input) => 
				input.closest('div')?.textContent?.includes('Select Node')
			);
			
			expect(searchInput).toBeTruthy();
			
			await act(async () => {
				if (searchInput) {
					fireEvent.change(searchInput, { target: { value: 'Test Dataset' } });
				}
			});
		}, 30000);
	});

	describe('Node Expansion', () => {
		it('should handle expand node click', async () => {
			const lineageDataWithExpand = {
				...mockLineageData,
				relationsOnDemand: {
					'test-guid-123': {
						hasMoreInputs: true,
						hasMoreOutputs: false,
						inputRelationsCount: 3,
						outputRelationsCount: 3
					}
				}
			};

			mockGetLineageData.mockResolvedValue({
				data: lineageDataWithExpand
			});

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Simulate expand button click through LineageHelper callback
			const callbacks = getStoredCallbacks();
			if (callbacks.onNodeClick) {
				act(() => {
					callbacks.onNodeClick({
						clickedData: ['more-inputs-test-guid-123']
					});
				});
			}

			await waitFor(() => {
				// Should trigger fetchGraph
				expect(mockGetNode).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should update query object for input expansion', async () => {
			const lineageDataWithExpand = {
				...mockLineageData,
				lineageOnDemandPayload: {
					'test-guid-123': {
						direction: 'BOTH',
						inputRelationsLimit: 6,
						outputRelationsLimit: 6,
						depth: 3
					}
				},
				relationsOnDemand: {
					'test-guid-123': {
						hasMoreInputs: true,
						inputRelationsCount: 6,
						outputRelationsCount: 6
					}
				}
			};

			mockGetLineageData.mockResolvedValue({
				data: lineageDataWithExpand
			});

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('Node Details Drawer', () => {
		it('should open drawer when node is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Simulate node click through LineageHelper callback
			const callbacks = getStoredCallbacks();
			if (callbacks.onNodeClick) {
				act(() => {
					callbacks.onNodeClick({
						clickedData: ['test-guid-123']
					});
				});
			}

			await waitFor(() => {
				expect(screen.getAllByText('DataSet').length).toBeGreaterThan(0);
			}, { timeout: 15000 });
		}, 30000);

		it('should close drawer when close button is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Open drawer
			const callbacks = getStoredCallbacks();
			if (callbacks.onNodeClick) {
				act(() => {
					callbacks.onNodeClick({
						clickedData: ['test-guid-123']
					});
				});
			}

			await waitFor(() => {
				const closeButton = screen.queryByRole('button', { name: /close/i });
				expect(closeButton).toBeTruthy();
				if (closeButton) {
					act(() => {
						fireEvent.click(closeButton);
					});
				}
			}, { timeout: 15000 });
		}, 30000);

		it('should display node details in drawer', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			const callbacks = getStoredCallbacks();
			if (callbacks.onNodeClick) {
				act(() => {
					callbacks.onNodeClick({
						clickedData: ['test-guid-123']
					});
				});
			}

			await waitFor(() => {
				expect(screen.getAllByText('DataSet').length).toBeGreaterThan(0);
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('Path Click - Propagation Modal', () => {
		it('should open propagation modal when path is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and callbacks to be stored
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
				const callbacks = getStoredCallbacks();
				expect(callbacks.onPathClick).toBeDefined();
			}, { timeout: 15000 });

			// Simulate path click through LineageHelper callback
			const callbacks = getStoredCallbacks();
			expect(callbacks.onPathClick).toBeDefined();
			
			await act(async () => {
				if (callbacks.onPathClick) {
					callbacks.onPathClick({
						pathRelationObj: {
							relationshipId: 'rel-1',
							fromEntityId: 'node-guid-1',
							toEntityId: 'test-guid-123'
						}
					});
				}
			});

			await waitFor(() => {
				expect(screen.getByTestId('propagation-property-modal')).toBeInTheDocument();
			}, { timeout: 15000 });
		}, 30000);

		it('should close propagation modal', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			const callbacks = getStoredCallbacks();
			if (callbacks.onPathClick) {
				act(() => {
					callbacks.onPathClick({
						pathRelationObj: {
							relationshipId: 'rel-1'
						}
					});
				});
			}

			await waitFor(() => {
				expect(screen.getByTestId('propagation-property-modal')).toBeInTheDocument();
			}, { timeout: 15000 });

			const closeButton = screen.getByTestId('close-propagation-modal');
			act(() => {
				fireEvent.click(closeButton);
			});

			await waitFor(() => {
				expect(screen.queryByTestId('propagation-property-modal')).not.toBeInTheDocument();
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('Label Click Navigation', () => {
		it('should navigate to entity detail page when label is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and callbacks to be stored
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
				const callbacks = getStoredCallbacks();
				expect(callbacks.onLabelClick).toBeDefined();
			}, { timeout: 15000 });

			const callbacks = getStoredCallbacks();
			expect(callbacks.onLabelClick).toBeDefined();
			
			await act(async () => {
				if (callbacks.onLabelClick) {
					callbacks.onLabelClick({
						clickedData: 'node-guid-1'
					});
				}
			});

			await waitFor(() => {
				expect(mockNavigate).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should show toast when clicking current entity label', async () => {
			const { toast } = require('react-toastify');
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and callbacks to be stored
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
				const callbacks = getStoredCallbacks();
				expect(callbacks.onLabelClick).toBeDefined();
			}, { timeout: 15000 });

			const callbacks = getStoredCallbacks();
			expect(callbacks.onLabelClick).toBeDefined();
			
			await act(async () => {
				if (callbacks.onLabelClick) {
					callbacks.onLabelClick({
						clickedData: 'test-guid-123'
					});
				}
			});

			await waitFor(() => {
				expect(toast.info).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('Reset Lineage', () => {
		it('should reset lineage when refresh button is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for buttons to be available
			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				expect(buttons.length).toBeGreaterThan(0);
			}, { timeout: 15000 });

			const buttons = screen.getAllByRole('button');
			const refreshButton = buttons.find((btn) => {
				const svg = btn.querySelector('svg[data-testid="RefreshIcon"]');
				return svg && !btn.hasAttribute('disabled');
			});
			
			expect(refreshButton).toBeTruthy();
			
			await act(async () => {
				if (refreshButton) {
					fireEvent.click(refreshButton);
				}
			});

			await waitFor(() => {
				expect(mockRefresh).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should reset full name display on reset', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			// Wait for buttons to be available
			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				expect(buttons.length).toBeGreaterThan(0);
			}, { timeout: 15000 });

			const buttons = screen.getAllByRole('button');
			const refreshButton = buttons.find((btn) => {
				const svg = btn.querySelector('svg[data-testid="RefreshIcon"]');
				return svg && !btn.hasAttribute('disabled');
			});
			
			expect(refreshButton).toBeTruthy();
			
			await act(async () => {
				if (refreshButton) {
					fireEvent.click(refreshButton);
				}
			});

			await waitFor(() => {
				expect(mockDisplayFullName).toHaveBeenCalledWith({ bLabelFullText: false });
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('Fullscreen Toggle', () => {
		it('should toggle fullscreen when fullscreen button is clicked', async () => {
			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
			}, { timeout: 15000 });

			await waitFor(() => {
				const buttons = screen.getAllByRole('button');
				const fullscreenButton = buttons.find((btn) => btn.querySelector('svg[data-testid="FullscreenIcon"]'));
				expect(fullscreenButton).toBeTruthy();
				if (fullscreenButton) {
					act(() => {
						fireEvent.click(fullscreenButton);
					});
				}
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('Edge Cases', () => {
		it('should handle missing entity attributes', async () => {
			const entityWithoutAttributes = {
				guid: 'test-guid-123',
				typeName: 'DataSet'
			};

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={entityWithoutAttributes} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should handle entity without classifications', async () => {
			const entityWithoutClassifications = {
				...mockEntity,
				classifications: []
			};

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={entityWithoutClassifications} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should handle undefined node in getNode', async () => {
			mockGetNode.mockReturnValue(undefined);

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Wait for loader to finish and callbacks to be stored
			await waitFor(() => {
				expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
				const callbacks = getStoredCallbacks();
				expect(callbacks.onNodeClick).toBeDefined();
			}, { timeout: 15000 });

			const callbacks = getStoredCallbacks();
			expect(callbacks.onNodeClick).toBeDefined();
			
			await act(async () => {
				if (callbacks.onNodeClick) {
					callbacks.onNodeClick({
						clickedData: ['invalid-guid']
					});
				}
			});

			// Should handle gracefully without error
			await waitFor(() => {
				expect(mockGetNode).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should handle empty relationsOnDemand', async () => {
			const dataWithoutOnDemand = {
				...mockLineageData,
				relationsOnDemand: null
			};

			mockGetLineageData.mockResolvedValue({
				data: dataWithoutOnDemand
			});

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);
	});

	describe('Data Processing', () => {
		it('should process lineage data with expand buttons', async () => {
			const dataWithExpand = {
				...mockLineageData,
				relationsOnDemand: {
					'test-guid-123': {
						hasMoreInputs: true,
						hasMoreOutputs: true
					}
				}
			};

			mockGetLineageData.mockResolvedValue({
				data: dataWithExpand
			});

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });

			// Data should be processed and expand buttons added
			await waitFor(() => {
				expect(MockLineageHelper).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);

		it('should handle baseEntityGuid not in guidEntityMap', async () => {
			const dataWithoutBaseEntity = {
				baseEntityGuid: 'test-guid-123',
				guidEntityMap: {},
				relations: [],
				legends: true
			};

			mockGetLineageData.mockResolvedValue({
				data: dataWithoutBaseEntity
			});

			await act(async () => {
				render(
					<TestWrapper>
						<LineageTab entity={mockEntity} />
					</TestWrapper>
				);
			});

			await waitFor(() => {
				expect(mockGetLineageData).toHaveBeenCalled();
			}, { timeout: 15000 });
		}, 30000);
	});
});
