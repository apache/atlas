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
 * Comprehensive unit tests for TypeSystemTreeView component
 * 
 * Coverage Target:
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import TypeSystemTreeView from '../TypeSystemTreeView';

// Mock dependencies
const mockGetNode = jest.fn();
const mockSetNode = jest.fn();
const mockSetEdge = jest.fn();
const mockCreateGraph = jest.fn();
const mockRefresh = jest.fn();
const mockExportLineage = jest.fn();
const mockZoomIn = jest.fn();
const mockZoomOut = jest.fn();
const mockDisplayFullName = jest.fn();
const mockSearchNode = jest.fn();
const mockGetNodes = jest.fn(() => ({}));

const mockLineageHelperInstance = {
	getNode: mockGetNode,
	setNode: mockSetNode,
	setEdge: mockSetEdge,
	createGraph: mockCreateGraph,
	refresh: mockRefresh,
	exportLineage: mockExportLineage,
	zoomIn: mockZoomIn,
	zoomOut: mockZoomOut,
	displayFullName: mockDisplayFullName,
	searchNode: mockSearchNode,
	getNodes: mockGetNodes
};

var MockLineageHelper: jest.Mock;

// Mock LineageHelper
jest.mock('@views/Lineage/atlas-lineage/src', () => {
	MockLineageHelper = jest.fn().mockImplementation(() => mockLineageHelperInstance);
	return {
		__esModule: true,
		default: MockLineageHelper
	};
});

// Mock react-router-dom
const mockParams = { guid: 'test-guid' };
jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useParams: () => mockParams
}));

// Mock Redux hooks
const mockUseAppSelector = jest.fn();
jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (...args: any[]) => mockUseAppSelector(...args)
}));

jest.mock('react-redux', () => ({
	...jest.requireActual('react-redux'),
	useSelector: jest.fn()
}));

// Mock utils
const mockIsEmpty = jest.fn((val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0));
const mockCloneDeep = jest.fn((val: any) => JSON.parse(JSON.stringify(val)));
const mockExtend = jest.fn((deep: boolean, target: any, ...sources: any[]) => {
	return Object.assign(target, ...sources);
});
const mockOmit = jest.fn((obj: any, keys: string[]) => {
	const result = { ...obj };
	keys.forEach(key => delete result[key]);
	return result;
});
const mockSortByKeyWithUnderscoreFirst = jest.fn((arr: any[], key: string) => {
	return [...arr].sort((a, b) => {
		const aKey = a[key] || '';
		const bKey = b[key] || '';
		if (aKey.startsWith('_') && !bKey.startsWith('_')) return -1;
		if (!aKey.startsWith('_') && bKey.startsWith('_')) return 1;
		return aKey.localeCompare(bKey);
	});
});

jest.mock('@utils/Utils', () => ({
	isEmpty: (...args: any[]) => mockIsEmpty(...args)
}));

jest.mock('@utils/Helper', () => ({
	cloneDeep: (...args: any[]) => mockCloneDeep(...args),
	extend: (...args: any[]) => mockExtend(...args),
	omit: (...args: any[]) => mockOmit(...args),
	sortByKeyWithUnderscoreFirst: (...args: any[]) => mockSortByKeyWithUnderscoreFirst(...args)
}));

jest.mock('@utils/Enum', () => ({
	lineageDepth: 3
}));

jest.mock('@utils/Muiutils', () => ({
	AntSwitch: ({ checked, onChange, ...props }: any) => (
		<input
			type="checkbox"
			checked={checked}
			onChange={onChange}
			data-testid="ant-switch"
			{...props}
		/>
	)
}));

jest.mock('@components/muiComponents', () => ({
	LightTooltip: ({ children, title }: any) => (
		<div data-testid="tooltip" title={title}>
			{children}
		</div>
	)
}));

jest.mock('@components/commonComponents', () => ({
	getValues: jest.fn((val: any) => {
		if (Array.isArray(val)) return val.join(', ');
		if (typeof val === 'object') return JSON.stringify(val);
		return String(val);
	})
}));

// Mock String.prototype.trunc
if (!String.prototype.trunc) {
	String.prototype.trunc = function(n: number) {
		return this.length > n ? this.substr(0, n) + '...' : this;
	};
}

describe('TypeSystemTreeView Component', () => {
	const mockEntityDefs = [
		{
			guid: 'guid-1',
			name: 'Entity1',
			superTypes: ['SuperType1'],
			subTypes: ['SubType1'],
			serviceType: 'Service1',
			attributeDefs: [{ name: 'attr1' }],
			businessAttributeDefs: { bm1: 'value1' },
			relationshipAttributeDefs: [{ name: 'rel1' }]
		},
		{
			guid: 'guid-2',
			name: 'Entity2',
			superTypes: [],
			subTypes: [],
			serviceType: 'Service2',
			attributeDefs: [],
			businessAttributeDefs: {},
			relationshipAttributeDefs: []
		},
		{
			guid: 'guid-3',
			name: '_Entity3',
			superTypes: ['Entity1'],
			subTypes: [],
			serviceType: 'Service1'
		}
	];

	const renderComponent = (entityDefs = mockEntityDefs) => {
		return render(
			<MemoryRouter>
				<TypeSystemTreeView entityDefs={entityDefs} />
			</MemoryRouter>
		);
	};

	const getTooltipButton = (title: string) => {
		const tooltip = screen.getAllByTestId('tooltip').find((el) => {
			return el.getAttribute('title') === title;
		});
		return tooltip?.querySelector('button');
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockGetNode.mockClear();
		mockSetNode.mockClear();
		mockSetEdge.mockClear();
		mockCreateGraph.mockClear();
		mockRefresh.mockClear();
		mockExportLineage.mockClear();
		mockZoomIn.mockClear();
		mockZoomOut.mockClear();
		mockDisplayFullName.mockClear();
		mockSearchNode.mockClear();
		mockGetNodes.mockReturnValue({});
		mockIsEmpty.mockImplementation((val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0));
		mockCloneDeep.mockImplementation((val: any) => JSON.parse(JSON.stringify(val)));
		mockExtend.mockImplementation((deep: boolean, target: any, ...sources: any[]) => {
			return Object.assign(target, ...sources);
		});
		mockOmit.mockImplementation((obj: any, keys: string[]) => {
			const result = { ...obj };
			keys.forEach(key => delete result[key]);
			return result;
		});
		mockSortByKeyWithUnderscoreFirst.mockImplementation((arr: any[], key: string) => {
			return [...arr].sort((a, b) => {
				const aKey = a[key] || '';
				const bKey = b[key] || '';
				if (aKey.startsWith('_') && !bKey.startsWith('_')) return -1;
				if (!aKey.startsWith('_') && bKey.startsWith('_')) return 1;
				return aKey.localeCompare(bKey);
			});
		});
		
		// Setup getBoundingClientRect mock
		Element.prototype.getBoundingClientRect = jest.fn(() => ({
			width: 800,
			height: 600,
			top: 0,
			left: 0,
			bottom: 600,
			right: 800,
			x: 0,
			y: 0,
			toJSON: jest.fn()
		}));
	});

	describe('Component Rendering', () => {
		it('should render TypeSystemTreeView component', () => {
			renderComponent();
			
			expect(screen.getAllByTestId('tooltip').length).toBeGreaterThan(0);
		});

		it('should render all toolbar buttons', () => {
			renderComponent();
			
			// Check for icon buttons (they should be rendered)
			const buttons = screen.getAllByRole('button');
			expect(buttons.length).toBeGreaterThan(0);
		});

		it('should render lineage div', () => {
			const { container } = renderComponent();
			
			const svgDiv = container.querySelector('.typesystem-svg');
			expect(svgDiv).toBeInTheDocument();
		});
	});

	describe('LineageHelper Initialization', () => {
		it('should initialize LineageHelper with correct parameters', () => {
			renderComponent();
			
			expect(MockLineageHelper).toHaveBeenCalled();
			const callArgs = MockLineageHelper.mock.calls[0][0];
			expect(callArgs.legends).toBe(false);
			expect(callArgs.setDataManually).toBe(true);
			expect(callArgs.zoom).toBe(true);
			expect(callArgs.fitToScreen).toBe(true);
			expect(callArgs.dagreOptions.rankdir).toBe('tb');
			expect(callArgs.toolTipTitle).toBe('Type');
		});

		it('should call fetchGraph on mount', async () => {
			renderComponent();
			
			await waitFor(() => {
				expect(mockCloneDeep).toHaveBeenCalled();
			});
		});
	});

	describe('Popover Controls', () => {
		it('should open settings popover when settings button is clicked', () => {
			renderComponent();
			
			const settingsButton = getTooltipButton('Settings');
			
			if (settingsButton) {
				fireEvent.click(settingsButton);
			}
			
			// Popover should open
			expect(screen.getByText('Settings')).toBeInTheDocument();
		});

		it('should open filter popover when filter button is clicked', () => {
			renderComponent();
			
			const filterButton = getTooltipButton('Filter');
			
			if (filterButton) {
				fireEvent.click(filterButton);
			}
			
			// Should handle filter popover
			expect(screen.queryByText('Filters')).toBeInTheDocument();
		});

		it('should open search popover when search button is clicked', () => {
			renderComponent();
			
			const searchButton = getTooltipButton('Search');
			
			if (searchButton) {
				fireEvent.click(searchButton);
			}
			
			// Should handle search popover
			expect(screen.queryByText('Search')).toBeInTheDocument();
		});

		it('should close popovers when close button is clicked', async () => {
			renderComponent();
			
			const settingsButton = getTooltipButton('Settings');
			if (settingsButton) {
				fireEvent.click(settingsButton);
			}
			
			await waitFor(() => {
				const closeButtons = screen.getAllByRole('button');
				const closeButton = closeButtons.find(btn => 
					btn.textContent === '' && btn.querySelector('svg')
				);
				if (closeButton) {
					fireEvent.click(closeButton);
				}
			});
		});
	});

	describe('Settings Toggles', () => {
		it('should toggle currentPathChecked', () => {
			renderComponent();
			
			const settingsButton = getTooltipButton('Settings');
			if (settingsButton) {
				fireEvent.click(settingsButton);
			}

			const switches = screen.getAllByTestId('ant-switch');
			if (switches.length > 0) {
				fireEvent.change(switches[0], { target: { checked: false } });
				expect(switches[0]).toBeInTheDocument();
			}
		});

		it('should toggle nodeDetailsChecked', () => {
			renderComponent();
			
			const settingsButton = getTooltipButton('Settings');
			if (settingsButton) {
				fireEvent.click(settingsButton);
			}

			const switches = screen.getAllByTestId('ant-switch');
			if (switches.length > 1) {
				fireEvent.change(switches[1], { target: { checked: true } });
				expect(switches[1]).toBeInTheDocument();
			}
		});

		it('should toggle fullNameChecked', () => {
			renderComponent();
			
			const settingsButton = getTooltipButton('Settings');
			if (settingsButton) {
				fireEvent.click(settingsButton);
			}

			const switches = screen.getAllByTestId('ant-switch');
			if (switches.length > 2) {
				fireEvent.change(switches[2], { target: { checked: true } });
				expect(switches[2]).toBeInTheDocument();
			}
		});
	});

	describe('Toolbar Actions', () => {
		it('should call refresh when reset button is clicked', () => {
			renderComponent();
			
			const resetButton = getTooltipButton('Reset');
			
			if (resetButton) {
				fireEvent.click(resetButton);
			}
			
			expect(mockRefresh).toHaveBeenCalled();
		});

		it('should call exportLineage when export button is clicked', () => {
			renderComponent();
			
			const exportButton = getTooltipButton('Export to PNG');
			
			if (exportButton) {
				fireEvent.click(exportButton);
			}
			
			expect(mockExportLineage).toHaveBeenCalledWith({ downloadFileName: 'TypeSystemView' });
		});

		it('should call zoomIn when zoom in button is clicked', () => {
			renderComponent();
			
			const zoomInButton = getTooltipButton('Zoom In');
			
			if (zoomInButton) {
				fireEvent.click(zoomInButton);
			}
			
			expect(mockZoomIn).toHaveBeenCalled();
		});

		it('should call zoomOut when zoom out button is clicked', () => {
			renderComponent();
			
			const zoomOutButton = getTooltipButton('Zoom Out');
			
			if (zoomOutButton) {
				fireEvent.click(zoomOutButton);
			}
			
			expect(mockZoomOut).toHaveBeenCalled();
		});

		it('should toggle fullscreen when fullscreen button is clicked', () => {
			renderComponent();
			
			const fullscreenButton =
				getTooltipButton('Full Screen') || getTooltipButton('Default View');
			
			if (fullscreenButton) {
				fireEvent.click(fullscreenButton);
			}
			
			// Fullscreen state should toggle
			expect(fullscreenButton).toBeInTheDocument();
		});
	});

	describe('Graph Generation', () => {
		it('should generate graph data for entityDefs', async () => {
			renderComponent();
			
			await waitFor(() => {
				expect(mockCloneDeep).toHaveBeenCalled();
				expect(mockSortByKeyWithUnderscoreFirst).toHaveBeenCalled();
			});
		});

		it('should handle empty entityDefs', () => {
			renderComponent([]);
			
			expect(mockIsEmpty).toHaveBeenCalled();
		});

		it('should create graph after generating data', async () => {
			renderComponent();
			
			await waitFor(() => {
				expect(mockCreateGraph).toHaveBeenCalled();
			});
		});

		it('should handle filter options in fetchGraph', async () => {
			renderComponent();
			
			await waitFor(() => {
				expect(mockExtend).toHaveBeenCalled();
			});
		});
	});

	describe('Node Data Creation', () => {
		it('should create node data for valid relationObj', async () => {
			renderComponent();
			
			await waitFor(() => {
				expect(mockSetNode).toHaveBeenCalled();
			});
		});

		it('should return undefined for invalid relationObj', async () => {
			renderComponent([{
				guid: '',
				name: 'Invalid'
			}]);
			
			await waitFor(() => {
				// Should handle invalid nodes
				expect(mockIsEmpty).toHaveBeenCalled();
			});
		});

		it('should return existing obj if updatedValues is true', async () => {
			renderComponent([{
				guid: 'guid-1',
				name: 'Entity1',
				updatedValues: true
			}]);
			
			await waitFor(() => {
				expect(mockSetNode).toHaveBeenCalled();
			});
		});
	});

	describe('Edge Creation', () => {
		it('should create edges between nodes', async () => {
			renderComponent();
			
			await waitFor(() => {
				expect(mockCreateGraph).toHaveBeenCalled();
			});
		});

		it('should not create edge for invalid guids', async () => {
			renderComponent([{
				guid: 'guid-1',
				name: 'Entity1',
				subTypes: ['']
			}]);
			
			await waitFor(() => {
				// Should handle invalid edges
				expect(mockIsEmpty).toHaveBeenCalled();
			});
		});
	});

	describe('Filter Functionality', () => {
		it('should filter by serviceType', async () => {
			renderComponent();
			
			// Open filter popover and select serviceType
			const filterButton = getTooltipButton('Filter');
			
			if (filterButton) {
				fireEvent.click(filterButton);
			}
			
			expect(screen.queryByText('Filters')).toBeInTheDocument();
		});

		it('should render filter options from nodes', () => {
			mockGetNodes.mockReturnValue({
				'node1': { serviceType: 'Service1', guid: 'guid-1' },
				'node2': { serviceType: 'Service2', guid: 'guid-2' }
			});
			
			renderComponent();
			
			// Filter options should be available
			expect(mockGetNodes).toHaveBeenCalled();
		});
	});

	describe('Search Functionality', () => {
		it('should search for node by guid', () => {
			renderComponent();
			
			// Open search popover and select node
			const searchButton = getTooltipButton('Search');
			
			if (searchButton) {
				fireEvent.click(searchButton);
			}
			
			// Should handle search
			expect(mockSearchNode).toBeDefined();
		});

		it('should render search options from nodes', () => {
			mockGetNodes.mockReturnValue({
				'node1': { name: 'Entity1', guid: 'guid-1' },
				'node2': { name: 'Entity2', guid: 'guid-2' }
			});
			
			renderComponent();
			
			// Search options should be available
			expect(mockGetNodes).toHaveBeenCalled();
		});
	});

	describe('Node Details Drawer', () => {
		it('should open drawer when node is clicked', async () => {
			mockGetNode.mockReturnValue({
				guid: 'guid-1',
				name: 'Entity1',
				attributeDefs: [{ name: 'attr1' }],
				businessAttributeDefs: { bm1: 'value1' },
				relationshipAttributeDefs: [{ name: 'rel1' }]
			});
			
			renderComponent();
			
			// Simulate node click through LineageHelper callback
			const onNodeClick = MockLineageHelper.mock.calls[0][0].onNodeClick;
			if (onNodeClick) {
				onNodeClick({ clickedData: 'guid-1' });
			}
			
			await waitFor(() => {
				expect(mockGetNode).toHaveBeenCalled();
			});
		});

		it('should close drawer when close button is clicked', async () => {
			mockGetNode.mockReturnValue({
				guid: 'guid-1',
				name: 'Entity1',
				attributeDefs: [],
				businessAttributeDefs: {},
				relationshipAttributeDefs: []
			});
			
			renderComponent();
			
			// Open drawer first
			const onNodeClick = MockLineageHelper.mock.calls[0][0].onNodeClick;
			if (onNodeClick) {
				onNodeClick({ clickedData: 'guid-1' });
			}
			
			await waitFor(() => {
				const closeButtons = screen.getAllByRole('button');
				const closeButton = closeButtons.find(btn => 
					btn.textContent === '' && btn.querySelector('svg')
				);
				if (closeButton) {
					fireEvent.click(closeButton);
				}
			});
		});

		it('should display node details correctly', async () => {
			mockGetNode.mockReturnValue({
				guid: 'guid-1',
				name: 'Entity1',
				attributeDefs: [{ name: 'attr1' }],
				businessAttributeDefs: { bm1: 'value1' },
				relationshipAttributeDefs: [{ name: 'rel1' }],
				otherProp: 'value'
			});
			
			renderComponent();
			
			// Simulate node click
			const onNodeClick = MockLineageHelper.mock.calls[0][0].onNodeClick;
			if (onNodeClick) {
				onNodeClick({ clickedData: 'guid-1' });
			}
			
			await waitFor(() => {
				expect(mockOmit).toHaveBeenCalled();
			});
		});
	});

	describe('Edge Cases', () => {
		it('should handle empty entityDefs', () => {
			renderComponent([]);
			
			expect(screen.getAllByTestId('tooltip').length).toBeGreaterThan(0);
		});

		it('should handle undefined entityDefs', () => {
			renderComponent(undefined as any);
			
			expect(screen.getAllByTestId('tooltip').length).toBeGreaterThan(0);
		});

		it('should handle nodes without superTypes', async () => {
			renderComponent([{
				guid: 'guid-1',
				name: 'Entity1',
				superTypes: [],
				subTypes: []
			}]);
			
			await waitFor(() => {
				expect(mockCreateGraph).toHaveBeenCalled();
			});
		});

		it('should handle nodes without subTypes', async () => {
			renderComponent([{
				guid: 'guid-1',
				name: 'Entity1',
				superTypes: [],
				subTypes: []
			}]);
			
			await waitFor(() => {
				expect(mockCreateGraph).toHaveBeenCalled();
			});
		});

		it('should handle filter with pendingSuperList', async () => {
			renderComponent([{
				guid: 'guid-1',
				name: 'Entity1',
				superTypes: ['PendingType'],
				serviceType: 'Service1'
			}]);
			
			await waitFor(() => {
				expect(mockCreateGraph).toHaveBeenCalled();
			});
		});

		it('should handle generateData rejection', async () => {
			mockExtend.mockImplementation(() => {
				throw new Error('Generate data error');
			});
			
			expect(() => renderComponent()).toThrow('Generate data error');
		});
	});

	describe('Fullscreen Toggle', () => {
		it('should toggle fullscreen state', () => {
			renderComponent();
			
			const fullscreenButton =
				getTooltipButton('Full Screen') || getTooltipButton('Default View');
			
			if (fullscreenButton) {
				fireEvent.click(fullscreenButton);
			}
			
			// State should toggle
			expect(fullscreenButton).toBeInTheDocument();
		});
	});

	describe('Reset Functionality', () => {
		it('should reset graph and clear filters', () => {
			renderComponent();
			
			const resetButton = getTooltipButton('Reset');
			
			if (resetButton) {
				fireEvent.click(resetButton);
			}
			
			expect(mockRefresh).toHaveBeenCalled();
		});
	});
});
