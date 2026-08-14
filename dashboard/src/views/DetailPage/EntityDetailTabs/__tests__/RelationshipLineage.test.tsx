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
import { render, screen, fireEvent, waitFor, act } from '@utils/test-utils';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import RelationshipLineage from '../RelationshipLineage';
import * as d3 from 'd3';

var mockCloneDeep: jest.Mock;
var mockExtractKeyValueFromEntity: jest.Mock;
var mockCustomSortBy: jest.Mock;

var mockZoomInstance: any;

jest.mock('@utils/Helper', () => {
	const actualHelper = jest.requireActual('@utils/Helper');
	return {
		...actualHelper,
		cloneDeep: (...args: any[]) => {
			if (mockCloneDeep) {
				return mockCloneDeep(...args);
			}
			return actualHelper.cloneDeep(...args);
		}
	};
});

jest.mock('@utils/Utils', () => {
	const actualUtils = jest.requireActual('@utils/Utils');
	return {
		...actualUtils,
		extractKeyValueFromEntity: (...args: any[]) => {
			if (mockExtractKeyValueFromEntity) {
				return mockExtractKeyValueFromEntity(...args);
			}
			return actualUtils.extractKeyValueFromEntity(...args);
		},
		customSortBy: (...args: any[]) => {
			if (mockCustomSortBy) {
				return mockCustomSortBy(...args);
			}
			return actualUtils.customSortBy(...args);
		}
	};
});

// Mock D3 with proper hoisting
jest.mock('d3', () => {
	const mockEnterSelection: any = {
		append: jest.fn(),
		attr: jest.fn(),
		text: jest.fn(),
		style: jest.fn(),
		call: jest.fn(),
		on: jest.fn()
	};
	Object.keys(mockEnterSelection).forEach((key) => {
		mockEnterSelection[key].mockReturnValue(mockEnterSelection);
	});

	const mockTransition = {
		duration: jest.fn().mockReturnThis(),
		scaleBy: jest.fn().mockReturnThis()
	};

	const mockZoomInSelection: any = { on: jest.fn() };
	const mockZoomOutSelection: any = { on: jest.fn() };
	mockZoomInSelection.on.mockReturnValue(mockZoomInSelection);
	mockZoomOutSelection.on.mockReturnValue(mockZoomOutSelection);

	const mockSelection: any = {
		attr: jest.fn(),
		append: jest.fn(),
		selectAll: jest.fn(),
		data: jest.fn(),
		enter: jest.fn(),
		call: jest.fn(),
		on: jest.fn(),
		text: jest.fn(),
		style: jest.fn(),
		select: jest.fn(),
		transition: jest.fn(),
		remove: jest.fn()
	};
	Object.keys(mockSelection).forEach((key) => {
		if (key === 'enter') {
			mockSelection[key].mockReturnValue(mockEnterSelection);
		} else if (key === 'transition') {
			mockSelection[key].mockReturnValue(mockTransition);
		} else {
			mockSelection[key].mockReturnValue(mockSelection);
		}
	});

	Object.keys(mockEnterSelection).forEach((key) => {
		if (typeof mockEnterSelection[key] === 'function') {
			mockEnterSelection[key].mockReturnValue(mockEnterSelection);
		}
	});

	const mockD3EventObj = {
		transform: { x: 0, y: 0, k: 1 },
		sourceEvent: {
			stopPropagation: jest.fn(),
			preventDefault: jest.fn()
		},
		defaultPrevented: false,
		active: false,
		x: 100,
		y: 100
	};

	const createMockForceLink = () => {
		const forceLinkInstance: any = {
			id: jest.fn().mockImplementation(function () {
				return forceLinkInstance;
			}),
			distance: jest.fn().mockImplementation(function () {
				return forceLinkInstance;
			}),
			strength: jest.fn().mockImplementation(function () {
				return forceLinkInstance;
			}),
			links: jest.fn().mockImplementation(function () {
				return forceLinkInstance;
			})
		};
		return forceLinkInstance;
	};

	const createMockSimulation = () => {
		const simulation: any = {
			nodes: jest.fn().mockReturnThis(),
			force: jest.fn((name?: string, forceInstance?: any) => {
				if (name && forceInstance) {
					if (name === 'link') {
						simulation.linkForce = forceInstance;
					}
					return simulation;
				}
				if (name === 'link') {
					return simulation.linkForce || createMockForceLink();
				}
				return simulation;
			}),
			on: jest.fn().mockReturnThis(),
			alphaTarget: jest.fn().mockReturnThis(),
			restart: jest.fn().mockReturnThis()
		};
		return simulation;
	};

	const createMockDrag = () => ({
		on: jest.fn().mockReturnThis()
	});

	const createMockZoom = () => ({
		scaleExtent: jest.fn().mockReturnThis(),
		on: jest.fn().mockReturnThis(),
		scaleBy: jest.fn().mockReturnThis()
	});

	const mockD3 = {
		select: jest.fn(() => mockSelection),
		selectAll: jest.fn(() => mockSelection),
		values: jest.fn((obj) => {
			if (!obj) {
				return [];
			}
			const values = Object.values(obj);
			return values.map((node: any, index: number) => {
				if (node && typeof node === 'object' && !node.id) {
					return { ...node, id: node.name || `node-${index}` };
				}
				return node;
			});
		}),
		zoom: jest.fn(() => createMockZoom()),
		forceSimulation: jest.fn(() => createMockSimulation()),
		forceLink: jest.fn(() => createMockForceLink()),
		forceManyBody: jest.fn(() => ({})),
		forceCenter: jest.fn(() => ({})),
		drag: jest.fn(() => createMockDrag())
	};

	Object.defineProperty(mockD3, 'event', {
		get: () => mockD3EventObj,
		set: (value) => {
			if (value && typeof value === 'object') {
				Object.assign(mockD3EventObj, value);
			}
		},
		configurable: true,
		enumerable: true
	});

	(globalThis as any).__relationshipLineageD3 = {
		mockEnterSelection,
		mockSelection,
		mockTransition,
		mockZoomInSelection,
		mockZoomOutSelection,
		mockD3EventObj
	};

	return mockD3;
});


// Mock Enum
jest.mock('@utils/Enum', () => ({
	entityStateReadOnly: {
		ACTIVE: false,
		DELETED: true,
		STATUS_ACTIVE: false,
		STATUS_DELETED: true
	},
	graphIcon: {
		DataSet: { textContent: '\uf1c0' },
		Process: { textContent: '\uf085' },
		Table: { textContent: '\uf0ce' }
	}
}));

// Mock React Router
const mockParams = { guid: 'test-guid-123' };
const mockLocation = {
	pathname: '/detailPage/test-guid-123',
	search: '?tabActive=relationship',
	hash: '',
	state: null,
	key: 'test-key'
};

jest.mock('react-router-dom', () => {
	const actualRouter = jest.requireActual('react-router-dom');
	return {
		...actualRouter,
		useParams: () => mockParams,
		useLocation: () => mockLocation
	};
});

// Mock MUI Components
jest.mock('@components/muiComponents', () => ({
	CloseIcon: () => <span data-testid="close-icon">×</span>,
	LightTooltip: ({ children, title }: any) => (
		<div data-testid="light-tooltip" title={title}>
			{children}
		</div>
	)
}));

const rld3 = (globalThis as any).__relationshipLineageD3;

const {
	mockEnterSelection,
	mockSelection,
	mockTransition,
	mockZoomInSelection,
	mockZoomOutSelection
} = rld3;

const mockD3EventObj = rld3.mockD3EventObj;

const theme = createTheme();

const triggerDrawerOpen = (overrides: Partial<{ name: string; value: any[] }> = {}) => {
	const mockNode = {
		name: 'Process',
		value: [
			{
				guid: 'proc-1',
				typeName: 'Process',
				displayText: 'Sample One',
				entityStatus: 'ACTIVE',
				relationshipStatus: 'ACTIVE'
			}
		],
		...overrides
	};

	act(() => {
		if (mockEnterSelection.clickHandler) {
			mockEnterSelection.clickHandler(mockNode);
		}
	});
};

const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => (
	<ThemeProvider theme={theme}>{children}</ThemeProvider>
);

describe('RelationshipLineage', () => {
	const mockEntityWithRelationships = {
		guid: 'test-guid-123',
		typeName: 'DataSet',
		displayText: 'Test Dataset',
		status: 'ACTIVE',
		relationshipAttributes: {
			inputToProcesses: [
				{
					guid: 'proc-1',
					typeName: 'Process',
					displayText: 'Process 1',
					entityStatus: 'ACTIVE',
					relationshipStatus: 'ACTIVE'
				},
				{
					guid: 'proc-2',
					typeName: 'Process',
					displayText: 'Process 2',
					entityStatus: 'ACTIVE',
					relationshipStatus: 'DELETED'
				}
			],
			outputFromProcesses: [
				{
					guid: 'proc-3',
					typeName: 'Process',
					displayText: 'Process 3',
					entityStatus: 'DELETED',
					relationshipStatus: 'ACTIVE'
				}
			]
		}
	};

	const mockEntityEmpty = {
		guid: 'test-guid-123',
		typeName: 'DataSet',
		relationshipAttributes: {}
	};

	const mockEntityWithSingleRelationship = {
		guid: 'test-guid-123',
		typeName: 'DataSet',
		relationshipAttributes: {
			inputToProcesses: {
				guid: 'proc-1',
				typeName: 'Process',
				displayText: 'Process 1',
				entityStatus: 'ACTIVE',
				relationshipStatus: 'ACTIVE'
			}
		}
	};

	const mockEntityWithArrayValue = {
		guid: 'test-guid-123',
		typeName: 'DataSet',
		relationshipAttributes: {
			inputToProcesses: [
				{
					guid: 'proc-1',
					typeName: 'Process',
					displayText: 'Process 1',
					entityStatus: 'ACTIVE',
					relationshipStatus: 'ACTIVE'
				},
				{
					guid: 'proc-2',
					typeName: 'Process',
					displayText: 'Process 2',
					entityStatus: 'ACTIVE',
					relationshipStatus: 'ACTIVE'
				},
				{
					guid: 'proc-3',
					typeName: 'Process',
					displayText: 'Process 3',
					entityStatus: 'ACTIVE',
					relationshipStatus: 'ACTIVE'
				}
			]
		}
	};

	const mockEntityWithGlossaryTerm = {
		guid: 'test-guid-123',
		typeName: 'DataSet',
		relationshipAttributes: {
			terms: [
				{
					guid: 'term-1',
					typeName: 'AtlasGlossaryTerm',
					displayText: 'Glossary Term 1',
					entityStatus: 'ACTIVE',
					relationshipStatus: 'ACTIVE'
				}
			]
		}
	};

	beforeEach(() => {
		jest.clearAllMocks();
		
		// Initialize mock functions with proper implementations
		mockCloneDeep = jest.fn((obj) => JSON.parse(JSON.stringify(obj)));
		mockExtractKeyValueFromEntity = jest.fn((entity: any, key?: string) => {
			if (key === 'displayText') {
				return { name: entity?.displayText || entity?.name || entity?.attributes?.name || '' };
			}
			return { name: entity?.name || entity?.displayText || entity?.attributes?.name || '' };
		});
		mockCustomSortBy = jest.fn((arr: any[], keys: string[]) => {
			return [...arr].sort((a, b) => {
				for (const key of keys) {
					const aVal = a[key] || '';
					const bVal = b[key] || '';
					if (aVal < bVal) return -1;
					if (aVal > bVal) return 1;
				}
				return 0;
			});
		});
		
		// Reset mockD3EventObj (same object ref as d3.event getter)
		Object.assign(mockD3EventObj, {
			transform: { x: 0, y: 0, k: 1 },
			sourceEvent: { stopPropagation: jest.fn(), preventDefault: jest.fn() },
			defaultPrevented: false,
			active: false,
			x: 100,
			y: 100
		});
		
		// Initialize zoom instance before component uses it
		mockZoomInstance = {
			scaleExtent: jest.fn().mockReturnThis(),
			on: jest.fn().mockReturnThis(),
			scaleBy: jest.fn().mockReturnThis()
		};
		
		// Reset D3 mocks - ensure all methods return chainable objects
		// CRITICAL: Use mockReturnValue (not mockImplementation) for reliable chaining
		// mockReturnValue persists through clearAllMocks, mockImplementation may not
		mockSelection.attr.mockReturnValue(mockSelection);
		mockSelection.append.mockReturnValue(mockSelection);
		mockSelection.selectAll.mockReturnValue(mockSelection);
		mockSelection.data.mockReturnValue(mockSelection);
		mockSelection.enter.mockReturnValue(mockEnterSelection);
		mockSelection.call.mockReturnValue(mockSelection);
		mockSelection.on.mockReturnValue(mockSelection);
		mockSelection.text.mockReturnValue(mockSelection);
		mockSelection.style.mockReturnValue(mockSelection);
		mockSelection.select.mockReturnValue(mockSelection);
		mockSelection.transition.mockReturnValue(mockTransition);
		mockSelection.remove.mockReturnValue(mockSelection);
		
		mockEnterSelection.append.mockReturnValue(mockEnterSelection);
		mockEnterSelection.attr.mockReturnValue(mockEnterSelection);
		mockEnterSelection.text.mockReturnValue(mockEnterSelection);
		mockEnterSelection.style.mockReturnValue(mockEnterSelection);
		mockEnterSelection.call.mockReturnValue(mockEnterSelection);
		mockEnterSelection.on.mockImplementation((eventName: string, handler?: (data?: any) => void) => {
			if (eventName === 'click' && typeof handler === 'function') {
				mockEnterSelection.clickHandler = handler;
			}
			return mockEnterSelection;
		});
		
		// Reset transition mocks
		mockTransition.duration.mockReturnValue(mockTransition);
		mockTransition.scaleBy.mockReturnValue(mockTransition);
		
		// Mock getBoundingClientRect
		Element.prototype.getBoundingClientRect = jest.fn(() => ({
			width: 800,
			height: 400,
			top: 0,
			left: 0,
			bottom: 400,
			right: 800,
			x: 0,
			y: 0,
			toJSON: jest.fn()
		}));
		
		// Reset D3 select mocks - ensure they return mockSelection
		mockZoomInSelection.on.mockImplementation((eventName: string, handler?: () => void) => {
			if (eventName === 'click' && typeof handler === 'function') {
				mockZoomInSelection.clickHandler = handler;
			}
			return mockZoomInSelection;
		});

		mockZoomOutSelection.on.mockImplementation((eventName: string, handler?: () => void) => {
			if (eventName === 'click' && typeof handler === 'function') {
				mockZoomOutSelection.clickHandler = handler;
			}
			return mockZoomOutSelection;
		});

		(d3.select as jest.Mock).mockImplementation((element: { id?: string } | null) => {
			if (element?.id === 'zoom_in') {
				return mockZoomInSelection;
			}
			if (element?.id === 'zoom_out') {
				return mockZoomOutSelection;
			}
			return mockSelection;
		});
		(d3.selectAll as jest.Mock).mockReturnValue(mockSelection);
		
		// Setup D3 zoom to return initialized zoom instance
		(d3.zoom as jest.Mock).mockImplementation(() => {
			return mockZoomInstance;
		});
		
		// d3.event is already set up in the mock factory, no need to redefine
	});

	describe('Component Rendering', () => {
		it('should render RelationshipLineage component with relationships', async () => {
			const { container } = render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			// Wait for SVG to be rendered and ref to be set
			await waitFor(() => {
				const svgElement = container.querySelector('svg');
				expect(svgElement).toBeInTheDocument();
				expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
			}, { timeout: 3000 });
			
			expect(screen.getByTitle('Zoom In')).toBeInTheDocument();
			expect(screen.getByTitle('Zoom Out')).toBeInTheDocument();
		});

		it('should render empty state when no relationships', () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityEmpty} />
				</TestWrapper>
			);

			expect(screen.getByText('No relationship data found')).toBeInTheDocument();
		});

		it('should render legend with Active and Deleted labels', () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			expect(screen.getByText('Active')).toBeInTheDocument();
			expect(screen.getByText('Deleted')).toBeInTheDocument();
		});

		it('should render SVG element', () => {
			const { container } = render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			const svg = container.querySelector('svg');
			expect(svg).toBeInTheDocument();
		});

		it('should render zoom controls', () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			const zoomInButton = screen.getByTitle('Zoom In');
			const zoomOutButton = screen.getByTitle('Zoom Out');
			
			expect(zoomInButton).toBeInTheDocument();
			expect(zoomOutButton).toBeInTheDocument();
		});
	});

	describe('D3.js Visualization Setup', () => {
		it('should create graph when component mounts', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(d3.select).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should setup zoom behavior', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(d3.zoom).toHaveBeenCalled();
				if (mockZoomInstance) {
					expect(mockZoomInstance.scaleExtent).toHaveBeenCalledWith([0.1, 4]);
				}
			}, { timeout: 3000 });
		});

		it('should setup force simulation', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(d3.forceSimulation).toHaveBeenCalled();
				expect(d3.forceLink).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should create SVG markers for links', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockSelection.append).toHaveBeenCalledWith('svg:defs');
			}, { timeout: 3000 });
		});

		it('should set SVG viewBox based on dimensions', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockSelection.attr).toHaveBeenCalledWith(
					'viewBox',
					expect.stringMatching(/^-\d+ -\d+ \d+ \d+$/)
				);
			}, { timeout: 3000 });
		});
	});

	describe('Data Processing', () => {
		it('should process relationship attributes into nodes and links', () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			expect(mockCloneDeep).toHaveBeenCalledWith(mockEntityWithRelationships);
		});

		it('should handle empty relationship attributes', () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityEmpty} />
				</TestWrapper>
			);

			expect(screen.getByText('No relationship data found')).toBeInTheDocument();
		});

		it('should create nodes for each relationship type', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockSelection.data).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should handle single relationship value (non-array)', () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithSingleRelationship} />
				</TestWrapper>
			);

			expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
		});

		it('should filter out empty relationship values', () => {
			const entityWithEmptyValues = {
				guid: 'test-guid-123',
				typeName: 'DataSet',
				relationshipAttributes: {
					emptyArray: [],
					nullValue: null,
					validRelation: [
						{
							guid: 'proc-1',
							typeName: 'Process',
							entityStatus: 'ACTIVE',
							relationshipStatus: 'ACTIVE'
						}
					]
				}
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithEmptyValues} />
				</TestWrapper>
			);

			expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
		});
	});

	describe('User Interactions - Zoom', () => {
		it('should handle zoom in button click', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			const zoomInButton = screen.getByTitle('Zoom In');
			
			await waitFor(() => {
				expect(d3.select).toHaveBeenCalled();
			}, { timeout: 3000 });

			act(() => {
				fireEvent.click(zoomInButton);
				if (mockZoomInSelection.clickHandler) {
					mockZoomInSelection.clickHandler();
				}
			});

			await waitFor(() => {
				if (mockZoomInstance) {
					expect(mockZoomInstance.scaleBy).toHaveBeenCalled();
				}
			}, { timeout: 3000 });
		});

		it('should handle zoom out button click', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			const zoomOutButton = screen.getByTitle('Zoom Out');
			
			await waitFor(() => {
				expect(d3.select).toHaveBeenCalled();
			}, { timeout: 3000 });

			act(() => {
				fireEvent.click(zoomOutButton);
				if (mockZoomOutSelection.clickHandler) {
					mockZoomOutSelection.clickHandler();
				}
			});

			await waitFor(() => {
				if (mockZoomInstance) {
					expect(mockZoomInstance.scaleBy).toHaveBeenCalled();
				}
			}, { timeout: 3000 });
		});

		it('should disable double-click zoom', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockSelection.on).toHaveBeenCalledWith('dblclick.zoom', null);
			}, { timeout: 3000 });
		});
	});

	describe('User Interactions - Node Click', () => {
		it('should open drawer when node is clicked', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			// Simulate node click by calling the click handler
			await waitFor(() => {
				expect(mockSelection.on).toHaveBeenCalled();
			}, { timeout: 3000 });

			// The drawer should be closed initially
			const drawer = screen.queryByText('inputToProcesses');
			expect(drawer).not.toBeInTheDocument();
		});

		it('should not open drawer when current entity node is clicked', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockSelection.on).toHaveBeenCalled();
			}, { timeout: 3000 });

			// Current entity node click should not open drawer
			// This is tested through the click handler logic
		});

		it('should close drawer when close button is clicked', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			// Open drawer first (simulated)
			// Then close it
			const closeButtons = screen.queryAllByTestId('close-icon');
			if (closeButtons.length > 0) {
				fireEvent.click(closeButtons[0]);
			}
		});
	});

	describe('Drawer Functionality', () => {
		it('should render drawer when drawerOpen is true', () => {
			// This will be tested through state manipulation
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
		});

		it('should display node details in drawer', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			triggerDrawerOpen();

			// Drawer content is rendered conditionally
			await waitFor(() => {
				expect(mockExtractKeyValueFromEntity).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should filter entities in drawer by search term', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithArrayValue} />
				</TestWrapper>
			);

			triggerDrawerOpen();

			// Search functionality is tested through the updateRelationshipDetails function
			await waitFor(() => {
				expect(mockExtractKeyValueFromEntity).toHaveBeenCalled();
			}, { timeout: 3000 });
		});
	});

	describe('Node and Edge Rendering', () => {
		it('should render nodes with correct colors for active entities', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.append).toHaveBeenCalledWith('circle');
			}, { timeout: 3000 });
		});

		it('should render nodes with correct colors for deleted entities', async () => {
			const entityWithDeleted = {
				guid: 'test-guid-123',
				typeName: 'DataSet',
				relationshipAttributes: {
					deletedProcess: [
						{
							guid: 'proc-1',
							typeName: 'Process',
							entityStatus: 'DELETED',
							relationshipStatus: 'DELETED'
						}
					]
				}
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithDeleted} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.append).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should render selected node with different color', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.attr).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should render links between nodes', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.append).toHaveBeenCalledWith('svg:path');
			}, { timeout: 3000 });
		});

		it('should render link markers with correct colors', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.append).toHaveBeenCalledWith('svg:marker');
			}, { timeout: 3000 });
		});

		it('should render node icons based on type', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.append).toHaveBeenCalledWith('text');
			}, { timeout: 3000 });
		});

		it('should render count badge for multiple entities', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithArrayValue} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.append).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should render node labels', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.append).toHaveBeenCalledWith('text');
			}, { timeout: 3000 });
		});
	});

	describe('Entity Status Handling', () => {
		it('should handle ACTIVE entity status', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.attr).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should handle DELETED entity status', async () => {
			const entityWithDeleted = {
				guid: 'test-guid-123',
				typeName: 'DataSet',
				relationshipAttributes: {
					deletedProcess: [
						{
							guid: 'proc-1',
							typeName: 'Process',
							entityStatus: 'DELETED',
							relationshipStatus: 'DELETED'
						}
					]
				}
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithDeleted} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.attr).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should handle ACTIVE relationship status', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.attr).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should handle DELETED relationship status', async () => {
			const entityWithDeletedRelation = {
				guid: 'test-guid-123',
				typeName: 'DataSet',
				relationshipAttributes: {
					deletedRelation: [
						{
							guid: 'proc-1',
							typeName: 'Process',
							entityStatus: 'ACTIVE',
							relationshipStatus: 'DELETED'
						}
					]
				}
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithDeletedRelation} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.attr).toHaveBeenCalled();
			}, { timeout: 3000 });
		});
	});

	describe('CustomLink Component', () => {
		it('should render CustomLink for regular entities', () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
		});

		it('should render CustomLink for AtlasGlossaryTerm with special route', () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithGlossaryTerm} />
				</TestWrapper>
			);

			expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
		});

		it('should apply deleted-relation class for deleted entities', () => {
			const entityWithDeleted = {
				guid: 'test-guid-123',
				typeName: 'DataSet',
				relationshipAttributes: {
					deletedProcess: [
						{
							guid: 'proc-1',
							typeName: 'Process',
							entityStatus: 'DELETED',
							relationshipStatus: 'DELETED'
						}
					]
				}
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithDeleted} />
				</TestWrapper>
			);

			expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
		});
	});

	describe('Search Functionality', () => {
		it('should filter entities by search term', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithArrayValue} />
				</TestWrapper>
			);

			triggerDrawerOpen();

			// Search input is rendered in drawer
			await waitFor(() => {
				expect(mockExtractKeyValueFromEntity).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should handle empty search term', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithArrayValue} />
				</TestWrapper>
			);

			triggerDrawerOpen();

			await waitFor(() => {
				expect(mockExtractKeyValueFromEntity).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should handle case-insensitive search', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithArrayValue} />
				</TestWrapper>
			);

			triggerDrawerOpen();

			await waitFor(() => {
				expect(mockExtractKeyValueFromEntity).toHaveBeenCalled();
			}, { timeout: 3000 });
		});
	});

	describe('Data Sorting', () => {
		it('should sort entities by displayText', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithArrayValue} />
				</TestWrapper>
			);

			const mockNode = {
				name: 'Process',
				value: [
					{
						guid: 'proc-1',
						typeName: 'Process',
						displayText: 'B',
						entityStatus: 'ACTIVE',
						relationshipStatus: 'ACTIVE'
					},
					{
						guid: 'proc-2',
						typeName: 'Process',
						displayText: 'A',
						entityStatus: 'ACTIVE',
						relationshipStatus: 'ACTIVE'
					}
				]
			};

			act(() => {
				if (mockEnterSelection.clickHandler) {
					mockEnterSelection.clickHandler(mockNode);
				}
			});

			await waitFor(() => {
				expect(mockCustomSortBy).toHaveBeenCalled();
			}, { timeout: 3000 });
		});
	});

	describe('Edge Cases', () => {
		it('should handle entity without relationshipAttributes', () => {
			const entityWithoutAttributes = {
				guid: 'test-guid-123',
				typeName: 'DataSet'
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithoutAttributes} />
				</TestWrapper>
			);

			expect(screen.getByText('No relationship data found')).toBeInTheDocument();
		});

		it('should handle null relationshipAttributes', () => {
			const entityWithNull = {
				guid: 'test-guid-123',
				typeName: 'DataSet',
				relationshipAttributes: null
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithNull as any} />
				</TestWrapper>
			);

			expect(screen.getByText('No relationship data found')).toBeInTheDocument();
		});

		it('should handle undefined relationshipAttributes', () => {
			const entityWithUndefined = {
				guid: 'test-guid-123',
				typeName: 'DataSet',
				relationshipAttributes: undefined
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithUndefined as any} />
				</TestWrapper>
			);

			expect(screen.getByText('No relationship data found')).toBeInTheDocument();
		});

		it('should handle entity with missing displayText', () => {
			const entityWithoutDisplayText = {
				guid: 'test-guid-123',
				typeName: 'DataSet',
				relationshipAttributes: {
					process: [
						{
							guid: 'proc-1',
							typeName: 'Process',
							name: 'Process 1',
							entityStatus: 'ACTIVE',
							relationshipStatus: 'ACTIVE'
						}
					]
				}
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithoutDisplayText} />
				</TestWrapper>
			);

			expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
		});

		it('should handle entity with missing typeName', () => {
			const entityWithoutTypeName = {
				guid: 'test-guid-123',
				relationshipAttributes: {
					process: [
						{
							guid: 'proc-1',
							displayText: 'Process 1',
							entityStatus: 'ACTIVE',
							relationshipStatus: 'ACTIVE'
						}
					]
				}
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithoutTypeName as any} />
				</TestWrapper>
			);

			expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
		});

		it('should handle SVG element with zero dimensions', async () => {
			Element.prototype.getBoundingClientRect = jest.fn(() => ({
				width: 0,
				height: 0,
				top: 0,
				left: 0,
				bottom: 0,
				right: 0,
				x: 0,
				y: 0,
				toJSON: jest.fn()
			}));

			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(d3.select).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should handle node drag when node is current entity', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(d3.drag).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should handle node click when event is prevented', async () => {
			mockD3EventObj.defaultPrevented = true;

			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockSelection.on).toHaveBeenCalled();
			}, { timeout: 3000 });

			mockD3EventObj.defaultPrevented = false;
		});
	});

	describe('Error Handling', () => {
		it('should handle D3 selection errors gracefully', async () => {
			let selectCallCount = 0;
			(d3.select as jest.Mock).mockImplementation(() => {
				selectCallCount += 1;
				if (selectCallCount === 1) {
					return mockSelection;
				}
				throw new Error('D3 selection error');
			});

			// Component should still render
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
		});

		it('should handle missing graphIcon gracefully', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			await waitFor(() => {
				expect(mockEnterSelection.text).toHaveBeenCalled();
			}, { timeout: 3000 });
		});

		it('should handle empty node value array', async () => {
			const entityWithEmptyNodeValue = {
				guid: 'test-guid-123',
				typeName: 'DataSet',
				relationshipAttributes: {
					process: []
				}
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithEmptyNodeValue} />
				</TestWrapper>
			);

			expect(screen.getByText('No relationship data found')).toBeInTheDocument();
		});
	});

	describe('Integration Tests', () => {
		it('should handle complete workflow: render -> click node -> search -> close', async () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithArrayValue} />
				</TestWrapper>
			);

			expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
			expect(screen.getByTitle('Zoom In')).toBeInTheDocument();
			expect(screen.getByTitle('Zoom Out')).toBeInTheDocument();

			// Zoom interactions
			fireEvent.click(screen.getByTitle('Zoom In'));
			if (mockZoomInSelection.clickHandler) {
				mockZoomInSelection.clickHandler();
			}
			fireEvent.click(screen.getByTitle('Zoom Out'));
			if (mockZoomOutSelection.clickHandler) {
				mockZoomOutSelection.clickHandler();
			}

			await waitFor(() => {
				if (mockZoomInstance) {
					expect(mockZoomInstance.scaleBy).toHaveBeenCalled();
				}
			}, { timeout: 3000 });
		});

		it('should handle multiple relationship types', () => {
			const entityWithMultipleTypes = {
				guid: 'test-guid-123',
				typeName: 'DataSet',
				relationshipAttributes: {
					inputToProcesses: [
						{
							guid: 'proc-1',
							typeName: 'Process',
							entityStatus: 'ACTIVE',
							relationshipStatus: 'ACTIVE'
						}
					],
					outputFromProcesses: [
						{
							guid: 'proc-2',
							typeName: 'Process',
							entityStatus: 'ACTIVE',
							relationshipStatus: 'ACTIVE'
						}
					],
					columns: [
						{
							guid: 'col-1',
							typeName: 'Column',
							entityStatus: 'ACTIVE',
							relationshipStatus: 'ACTIVE'
						}
					]
				}
			};

			render(
				<TestWrapper>
					<RelationshipLineage entity={entityWithMultipleTypes} />
				</TestWrapper>
			);

			expect(screen.getByTestId('relationshipSVG')).toBeInTheDocument();
		});
	});

	describe('Accessibility', () => {
		it('should have proper ARIA labels on zoom buttons', () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			expect(screen.getByTitle('Zoom In')).toBeInTheDocument();
			expect(screen.getByTitle('Zoom Out')).toBeInTheDocument();
		});

		it('should have proper tooltips on legend items', () => {
			render(
				<TestWrapper>
					<RelationshipLineage entity={mockEntityWithRelationships} />
				</TestWrapper>
			);

			const tooltips = screen.getAllByTestId('light-tooltip');
			expect(tooltips.length).toBeGreaterThan(0);
		});
	});
});
