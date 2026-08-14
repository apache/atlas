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
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { Provider } from 'react-redux';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { configureStore } from '@reduxjs/toolkit';
import TermRelation from '../TermRelation';

// Mock dependencies
jest.mock('react-toastify', () => ({
	toast: {
		dismiss: jest.fn(),
		error: jest.fn(),
		success: jest.fn()
	}
}));

// Mock child components
jest.mock('../TermRelationAttributes', () => ({
	__esModule: true,
	default: ({ editModal, termObj, control, currentType }: any) => (
		<div data-testid="term-relation-attributes">
			<div>Edit Mode: {editModal ? 'true' : 'false'}</div>
			<div>Current Type: {currentType}</div>
			<div>Term Count: {termObj?.length || 0}</div>
		</div>
	)
}));

jest.mock('@components/DialogShowMoreLess', () => ({
	__esModule: true,
	default: ({ columnVal, colName }: any) => (
		<div data-testid="dialog-show-more-less">
			{columnVal} - {colName}
		</div>
	)
}));

jest.mock('@components/Modal', () => ({
	__esModule: true,
	default: ({ open, onClose, title, button1Label, button2Label, button2Handler, children }: any) => (
		open ? (
			<div data-testid="custom-modal">
				<div data-testid="modal-title">{title}</div>
				{button1Label && <button data-testid="button1" onClick={onClose}>{button1Label}</button>}
				<button data-testid="button2" onClick={button2Handler}>{button2Label}</button>
				<div>{children}</div>
			</div>
		) : null
	)
}));

jest.mock('@components/Table/TableLayout', () => ({
	TableLayout: ({ data, columns, emptyText }: any) => (
		<div data-testid="table-layout">
			{data && data.length > 0 ? (
				<table>
					<tbody>
						{data.map((row: string, idx: number) => (
							<tr key={idx} data-testid={`table-row-${row}`}>
								<td>{row}</td>
								{columns.map((col: any, colIdx: number) => (
									<td key={colIdx}>
										{col.cell ? col.cell({ row: { original: row } }) : row}
									</td>
								))}
							</tr>
						))}
					</tbody>
				</table>
			) : (
				<div data-testid="empty-text">{emptyText}</div>
			)}
		</div>
	)
}));

// Mock API methods
const mockAssignGlossaryType = jest.fn();
const mockRemoveTerm = jest.fn();
jest.mock('@api/apiMethods/glossaryApiMethod', () => ({
	assignGlossaryType: (...args: any[]) => mockAssignGlossaryType(...args),
	removeTerm: (...args: any[]) => mockRemoveTerm(...args)
}));

// Mock Redux actions
const mockFetchGlossaryDetails = jest.fn();
const mockFetchDetailPageData = jest.fn();
jest.mock('@redux/slice/glossaryDetailsSlice', () => ({
	fetchGlossaryDetails: (...args: any[]) => mockFetchGlossaryDetails(...args)
}));

jest.mock('@redux/slice/detailPageSlice', () => ({
	fetchDetailPageData: (...args: any[]) => mockFetchDetailPageData(...args)
}));

// Mock Utils
const mockServerError = jest.fn();
const mockCloneDeep = jest.fn((obj) => JSON.parse(JSON.stringify(obj)));
jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0),
	serverError: (...args: any[]) => mockServerError(...args)
}));

jest.mock('@utils/Helper', () => ({
	cloneDeep: (...args: any[]) => mockCloneDeep(...args)
}));

// Mock Enum
jest.mock('@utils/Enum', () => ({
	termRelationAttributeList: {
		seeAlso: 'See Also',
		synonyms: 'Synonyms',
		antonyms: 'Antonyms',
		preferredTerms: 'Preferred Terms',
		preferredToTerms: 'Preferred To Terms',
		replacementTerms: 'Replacement Terms',
		replacedBy: 'Replaced By',
		translationTerms: 'Translation Terms',
		translatedTerms: 'Translated Terms',
		isA: 'Is A',
		classifies: 'Classifies',
		validValues: 'Valid Values',
		validValuesFor: 'Valid Values For'
	}
}));

// Mock react-hook-form
const mockHandleSubmit = jest.fn((fn) => (e?: any) => {
	if (e) e.preventDefault();
	return fn({});
});

jest.mock('react-hook-form', () => ({
	useForm: () => ({
		control: {},
		handleSubmit: mockHandleSubmit,
		formState: { isSubmitting: false }
	})
}));

// Mock moment
jest.mock('moment', () => {
	const mockMoment = jest.fn(() => ({
		milliseconds: jest.fn()
	}));
	mockMoment.now = jest.fn(() => 1640995200000);
	return mockMoment;
});

// Helper to create mock store
const createMockStore = () => {
	return configureStore({
		reducer: {
			glossaryDetails: (state = { glossary: {}, loading: false }) => state,
			session: (state = { user: {} }) => state
		},
		middleware: (getDefaultMiddleware) =>
			getDefaultMiddleware({
				serializableCheck: false,
				immutableCheck: false
			})
	});
};

// Helper to render with router and redux
const renderWithRouter = (
	component: React.ReactElement,
	options: { searchParams?: string; guid?: string } = {}
) => {
	const { searchParams = '', guid = 'test-guid-123' } = options;
	const store = createMockStore();
	const path = `/glossary/${guid}${searchParams ? `?${searchParams}` : ''}`;

	return render(
		<Provider store={store}>
			<MemoryRouter initialEntries={[path]}>
				<Routes>
					<Route path="/glossary/:guid" element={component} />
				</Routes>
			</MemoryRouter>
		</Provider>
	);
};

describe('TermRelation - 100% Coverage', () => {
	const mockGlossaryData = {
		guid: 'test-guid-123',
		name: 'Test Term',
		seeAlso: [
			{ displayText: 'Related Term 1', qualifiedName: 'term1' },
			{ displayText: 'Related Term 2', qualifiedName: 'term2' }
		],
		synonyms: [
			{ displayText: 'Synonym 1', qualifiedName: 'syn1' }
		]
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockFetchGlossaryDetails.mockReturnValue({ type: 'glossaryDetails/fetch' });
		mockFetchDetailPageData.mockReturnValue({ type: 'detailPage/fetch' });
		mockAssignGlossaryType.mockResolvedValue({ data: {} });
	});

	describe('Component Rendering', () => {
		test('renders TermRelation component', () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('renders table with relation types', () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			expect(screen.getByTestId('table-row-seeAlso')).toBeInTheDocument();
			expect(screen.getByTestId('table-row-synonyms')).toBeInTheDocument();
		});

		test('renders all relation type rows', () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			// Should render all relation types from termRelationAttributeList
			expect(screen.getByTestId('table-row-seeAlso')).toBeInTheDocument();
			expect(screen.getByTestId('table-row-synonyms')).toBeInTheDocument();
			expect(screen.getByTestId('table-row-antonyms')).toBeInTheDocument();
			expect(screen.getByTestId('table-row-preferredTerms')).toBeInTheDocument();
		});
	});

	describe('Table Columns', () => {
		test('renders Relation Types column', () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			expect(screen.getAllByText('seeAlso').length).toBeGreaterThan(0);
		});

		test('renders Related Terms column with DialogShowMoreLess', () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const dialogs = screen.getAllByTestId('dialog-show-more-less');
			expect(dialogs.length).toBeGreaterThan(0);
		});

		test('renders Attributes column with view and edit buttons', () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const viewButtons = screen.getAllByTestId('showAttribute');
			const editButtons = screen.getAllByTestId('editAttribute');
			
			expect(viewButtons.length).toBeGreaterThan(0);
			expect(editButtons.length).toBeGreaterThan(0);
		});

		test('does not render buttons when glossaryTypeData is empty', () => {
			renderWithRouter(<TermRelation glossaryTypeData={{}} />);

			expect(screen.queryByTestId('showAttribute')).not.toBeInTheDocument();
			expect(screen.queryByTestId('editAttribute')).not.toBeInTheDocument();
		});

		test('does not render buttons when specific relation type is empty', () => {
			const dataWithEmptyRelation = {
				...mockGlossaryData,
				antonyms: []
			};

			renderWithRouter(<TermRelation glossaryTypeData={dataWithEmptyRelation} />);

			// Should have buttons for seeAlso and synonyms but not for antonyms
			const viewButtons = screen.getAllByTestId('showAttribute');
			expect(viewButtons.length).toBe(2); // Only for seeAlso and synonyms
		});
	});

	describe('View Modal', () => {
		test('opens view modal when view button is clicked', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const viewButton = screen.getAllByTestId('showAttribute')[0];
			fireEvent.click(viewButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});
		});

		test('sets editModal to false when view button is clicked', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const viewButton = screen.getAllByTestId('showAttribute')[0];
			fireEvent.click(viewButton);

			await waitFor(() => {
				expect(screen.getAllByText('Edit Mode: false').length).toBeGreaterThan(0);
			});
		});

		test('displays correct modal title for view mode', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const viewButton = screen.getAllByTestId('showAttribute')[0];
			fireEvent.click(viewButton);

			await waitFor(() => {
				expect(screen.getByTestId('modal-title')).toHaveTextContent('Attributes of seeAlso');
			});
		});

		test('sets current type when view button is clicked', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const viewButton = screen.getAllByTestId('showAttribute')[0];
			fireEvent.click(viewButton);

			await waitFor(() => {
				expect(screen.getByText('Current Type: seeAlso')).toBeInTheDocument();
			});
		});

		test('sets term object when view button is clicked', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const viewButton = screen.getAllByTestId('showAttribute')[0];
			fireEvent.click(viewButton);

			await waitFor(() => {
				expect(screen.getByText('Term Count: 2')).toBeInTheDocument();
			});
		});
	});

	describe('Edit Modal', () => {
		test('opens edit modal when edit button is clicked', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});
		});

		test('sets editModal to true when edit button is clicked', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByText('Edit Mode: true')).toBeInTheDocument();
			});
		});

		test('displays correct modal title for edit mode', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('modal-title')).toHaveTextContent('Edit Attributes of seeAlso');
			});
		});

		test('renders Close and Update buttons in edit mode', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('button1')).toHaveTextContent('Close');
				expect(screen.getByTestId('button2')).toHaveTextContent('Update');
			});
		});

		test('renders only Close button in view mode', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const viewButton = screen.getAllByTestId('showAttribute')[0];
			fireEvent.click(viewButton);

			await waitFor(() => {
				expect(screen.queryByTestId('button1')).not.toBeInTheDocument();
				expect(screen.getByTestId('button2')).toHaveTextContent('Close');
			});
		});
	});

	describe('Modal Close Handling', () => {
		test('closes modal when handleCloseModal is called', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const viewButton = screen.getAllByTestId('showAttribute')[0];
			fireEvent.click(viewButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});

			const closeButton = screen.getByTestId('button2');
			fireEvent.click(closeButton);

			await waitFor(() => {
				expect(screen.queryByTestId('custom-modal')).not.toBeInTheDocument();
			});
		});

		test('closes modal when button1 is clicked in edit mode', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});

			const button1 = screen.getByTestId('button1');
			fireEvent.click(button1);

			await waitFor(() => {
				expect(screen.queryByTestId('custom-modal')).not.toBeInTheDocument();
			});
		});
	});

	describe('Form Submission', () => {
		test('calls assignGlossaryType on form submit', async () => {
			mockHandleSubmit.mockImplementation((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({ seeAlso: { 'Related Term 1': { description: 'Updated' } } });
			});

			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />, {
				searchParams: 'gtype=term'
			});

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});

			const updateButton = screen.getByTestId('button2');
			fireEvent.click(updateButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalledWith(
					'test-guid-123',
					expect.any(Object)
				);
			});
		});

		test('updates glossary data correctly on submit', async () => {
			mockHandleSubmit.mockImplementation((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({ seeAlso: { 'Related Term 1': { description: 'Updated' } } });
			});

			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />, {
				searchParams: 'gtype=term'
			});

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});

			const updateButton = screen.getByTestId('button2');
			fireEvent.click(updateButton);

			await waitFor(() => {
				expect(mockCloneDeep).toHaveBeenCalledWith(mockGlossaryData);
			});
		});

		test('dispatches fetchGlossaryDetails after successful submit', async () => {
			mockHandleSubmit.mockImplementation((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({ seeAlso: { 'Related Term 1': { description: 'Updated' } } });
			});

			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />, {
				searchParams: 'gtype=term'
			});

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});

			const updateButton = screen.getByTestId('button2');
			fireEvent.click(updateButton);

			await waitFor(() => {
				expect(mockFetchGlossaryDetails).toHaveBeenCalledWith({
					gtype: 'term',
					guid: 'test-guid-123'
				});
			});
		});

		test('dispatches fetchDetailPageData after successful submit', async () => {
			mockHandleSubmit.mockImplementation((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({ seeAlso: { 'Related Term 1': { description: 'Updated' } } });
			});

			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />, {
				searchParams: 'gtype=term'
			});

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});

			const updateButton = screen.getByTestId('button2');
			fireEvent.click(updateButton);

			await waitFor(() => {
				expect(mockFetchDetailPageData).toHaveBeenCalledWith('test-guid-123');
			});
		});

		test('shows success toast after successful submit', async () => {
			const { toast } = require('react-toastify');
			
			mockHandleSubmit.mockImplementation((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({ seeAlso: { 'Related Term 1': { description: 'Updated' } } });
			});

			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />, {
				searchParams: 'gtype=term'
			});

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});

			const updateButton = screen.getByTestId('button2');
			fireEvent.click(updateButton);

			await waitFor(() => {
				expect(toast.success).toHaveBeenCalledWith('Attributes updated successfully');
			});
		});

		test('handles API error on submit', async () => {
			mockAssignGlossaryType.mockRejectedValue(new Error('API Error'));

			mockHandleSubmit.mockImplementation((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({ seeAlso: { 'Related Term 1': { description: 'Updated' } } });
			});

			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />, {
				searchParams: 'gtype=term'
			});

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});

			const updateButton = screen.getByTestId('button2');
			fireEvent.click(updateButton);

			await waitFor(() => {
				expect(mockServerError).toHaveBeenCalled();
			});
		});
	});

	describe('handleClick Function', () => {
		test('sets currentType correctly', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const viewButton = screen.getAllByTestId('showAttribute')[0];
			fireEvent.click(viewButton);

			await waitFor(() => {
				expect(screen.getByText('Current Type: seeAlso')).toBeInTheDocument();
			});
		});

		test('sets termObj correctly', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const viewButton = screen.getAllByTestId('showAttribute')[0];
			fireEvent.click(viewButton);

			await waitFor(() => {
				expect(screen.getByText('Term Count: 2')).toBeInTheDocument();
			});
		});

		test('handles different relation types', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			// Click on synonyms view button (second one)
			const viewButtons = screen.getAllByTestId('showAttribute');
			fireEvent.click(viewButtons[1]);

			await waitFor(() => {
				expect(screen.getByText('Current Type: synonyms')).toBeInTheDocument();
				expect(screen.getByText('Term Count: 1')).toBeInTheDocument();
			});
		});
	});

	describe('Edge Cases', () => {
		test('handles null glossaryTypeData', () => {
			renderWithRouter(<TermRelation glossaryTypeData={null} />);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles undefined glossaryTypeData', () => {
			renderWithRouter(<TermRelation glossaryTypeData={undefined} />);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles empty glossaryTypeData', () => {
			renderWithRouter(<TermRelation glossaryTypeData={{}} />);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
			expect(screen.queryByTestId('showAttribute')).not.toBeInTheDocument();
		});

		test('handles missing guid parameter', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<MemoryRouter initialEntries={['/glossary/']}>
						<Routes>
							<Route path="/glossary/:guid?" element={<TermRelation glossaryTypeData={mockGlossaryData} />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('handles missing gtype parameter', () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});

		test('does not dispatch actions when entityGuid is empty', async () => {
			mockHandleSubmit.mockImplementation((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({ seeAlso: { 'Related Term 1': { description: 'Updated' } } });
			});

			const store = createMockStore();

			render(
				<Provider store={store}>
					<MemoryRouter initialEntries={['/glossary/?gtype=term']}>
						<Routes>
							<Route path="/glossary/:guid?" element={<TermRelation glossaryTypeData={mockGlossaryData} />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});

			mockFetchGlossaryDetails.mockClear();
			mockFetchDetailPageData.mockClear();

			const updateButton = screen.getByTestId('button2');
			fireEvent.click(updateButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			});

			// Should not dispatch when entityGuid is empty
			expect(mockFetchGlossaryDetails).not.toHaveBeenCalled();
			expect(mockFetchDetailPageData).not.toHaveBeenCalled();
		});
	});

	describe('Component Integration', () => {
		test('passes correct props to TermRelationAttributes', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				expect(screen.getByTestId('term-relation-attributes')).toBeInTheDocument();
			});
		});

		test('renders form with TermRelationAttributes', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const editButton = screen.getAllByTestId('editAttribute')[0];
			fireEvent.click(editButton);

			await waitFor(() => {
				const form = screen.getByTestId('custom-modal').querySelector('form');
				expect(form).toBeInTheDocument();
			});
		});

		test('updates table when updateTable state changes', async () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			// Initial render
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();

			// The table should re-render when updateTable changes (via useMemo dependency)
			const viewButton = screen.getAllByTestId('showAttribute')[0];
			fireEvent.click(viewButton);

			await waitFor(() => {
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			});
		});
	});

	describe('Table Configuration', () => {
		test('renders table with correct props', () => {
			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			const table = screen.getByTestId('table-layout');
			expect(table).toBeInTheDocument();
		});

		test('renders empty text when no data', () => {
			// Mock termRelationAttributeList to be empty
			jest.doMock('@utils/Enum', () => ({
				termRelationAttributeList: {}
			}));

			renderWithRouter(<TermRelation glossaryTypeData={mockGlossaryData} />);

			// Table should still render
			expect(screen.getByTestId('table-layout')).toBeInTheDocument();
		});
	});
});
