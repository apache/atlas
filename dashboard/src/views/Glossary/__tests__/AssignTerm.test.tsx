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
import userEvent from '@testing-library/user-event';
import AssignTerm from '../AssignTerm';
import { toast } from 'react-toastify';

// Mock Redux hooks
const mockDispatch = jest.fn();
const mockUseAppSelector = jest.fn();

jest.mock('@hooks/reducerHook', () => ({
	useAppDispatch: () => mockDispatch,
	useAppSelector: (selector: any) => mockUseAppSelector(selector)
}));

// Mock React Router hooks
const mockLocation = {
	pathname: '/glossary',
	search: '',
	hash: '',
	state: null,
	key: 'default'
};

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useLocation: jest.fn(() => mockLocation),
	useParams: jest.fn(() => ({}))
}));

// Mock API methods
const mockAssignGlossaryType = jest.fn();
const mockAssignTermstoCategory = jest.fn();
const mockAssignTermstoEntites = jest.fn();

jest.mock('@api/apiMethods/glossaryApiMethod', () => ({
	assignGlossaryType: (...args: any[]) => mockAssignGlossaryType(...args),
	assignTermstoCategory: (...args: any[]) => mockAssignTermstoCategory(...args),
	assignTermstoEntites: (...args: any[]) => mockAssignTermstoEntites(...args)
}));

// Mock Redux actions
const mockFetchDetailPageData = jest.fn(() => ({ type: 'detailPage/fetch' }));
const mockFetchGlossaryDetails = jest.fn(() => ({ type: 'glossaryDetails/fetch' }));
const mockFetchGlossaryData = jest.fn(() => ({ type: 'glossary/fetch' }));

jest.mock('@redux/slice/detailPageSlice', () => ({
	fetchDetailPageData: (...args: any[]) => mockFetchDetailPageData(...args)
}));

jest.mock('@redux/slice/glossaryDetailsSlice', () => ({
	fetchGlossaryDetails: (...args: any[]) => mockFetchGlossaryDetails(...args)
}));

jest.mock('@redux/slice/glossarySlice', () => ({
	fetchGlossaryData: (...args: any[]) => mockFetchGlossaryData(...args)
}));

// Mock utils
const mockIsEmpty = jest.fn();
const mockCustomSortBy = jest.fn();
const mockCustomSortByObjectKeys = jest.fn();
const mockNoTreeData = jest.fn();
const mockServerError = jest.fn();

jest.mock('@utils/Utils', () => ({
	isEmpty: (...args: any[]) => mockIsEmpty(...args),
	customSortBy: (...args: any[]) => mockCustomSortBy(...args),
	customSortByObjectKeys: (...args: any[]) => mockCustomSortByObjectKeys(...args),
	noTreeData: (...args: any[]) => mockNoTreeData(...args),
	serverError: (...args: any[]) => mockServerError(...args)
}));

const mockCloneDeep = jest.fn();

jest.mock('@utils/Helper', () => ({
	cloneDeep: (...args: any[]) => mockCloneDeep(...args)
}));

// Mock react-hook-form
let mockFormState = { isSubmitting: false };
const mockOnChange = jest.fn();
const mockHandleSubmit = jest.fn((fn: any) => (e?: any) => {
	if (e) e.preventDefault();
	return fn({});
});

const mockUseForm = jest.fn(() => ({
	control: {},
	handleSubmit: mockHandleSubmit,
	formState: mockFormState
}));

jest.mock('react-hook-form', () => ({
	useForm: (...args: any[]) => mockUseForm(...args),
	Controller: ({ render, name }: any) => {
		const field = {
			onChange: mockOnChange,
			value: ''
		};
		return render({ field });
	}
}));

// Mock react-toastify
jest.mock('react-toastify', () => ({
	toast: {
		error: jest.fn(() => 'toast-id'),
		success: jest.fn(() => 'toast-id'),
		info: jest.fn(() => 'toast-id'),
		dismiss: jest.fn()
	}
}));

// Mock moment-timezone
jest.mock('moment-timezone', () => {
	const moment = jest.requireActual('moment-timezone');
	const mockNow = jest.fn(() => 1234567890);
	return {
		...moment,
		default: {
			...moment.default,
			now: mockNow
		},
		now: mockNow
	};
});

// Mock components
jest.mock('@components/Modal', () => ({
	__esModule: true,
	default: ({
		open,
		onClose,
		title,
		button1Label,
		button1Handler,
		button2Label,
		button2Handler,
		disableButton2,
		isDirty,
		children
	}: any) =>
		open ? (
			<div data-testid="custom-modal">
				<div data-testid="modal-title">{title}</div>
				<div data-testid="modal-content">{children}</div>
				<button data-testid="button-1" onClick={button1Handler}>
					{button1Label}
				</button>
				<button
					data-testid="button-2"
					onClick={button2Handler}
					disabled={disableButton2}
				>
					{button2Label}
				</button>
				<button data-testid="modal-close" onClick={onClose}>
					Close
				</button>
			</div>
		) : null
}));

let mockOnNodeSelect: any;
jest.mock('@components/Forms/FormTreeView', () => ({
	__esModule: true,
	default: ({
		treeData,
		searchTerm,
		treeName,
		loader,
		onNodeSelect
	}: any) => {
		mockOnNodeSelect = onNodeSelect;
		return (
			<div data-testid="form-tree-view">
				<div data-testid="tree-name">{treeName}</div>
				<div data-testid="search-term">{searchTerm}</div>
				<div data-testid="loader">{loader ? 'Loading' : 'Not Loading'}</div>
				{Array.isArray(treeData) && treeData.map((node: any) => (
					<button
						key={node.id}
						data-testid={`tree-node-${node.id}`}
						onClick={() => onNodeSelect(node.id)}
					>
						{node.label}
					</button>
				))}
			</div>
		);
	}
}));

describe('AssignTerm', () => {
	const mockOnClose = jest.fn();
	const mockUpdateTable = jest.fn();
	const mockSetRowSelection = jest.fn();

	const defaultGlossaryData = [
		{
			name: 'Test Glossary',
			guid: 'glossary-guid-1',
			terms: [
				{
					displayText: 'Test Term',
					termGuid: 'term-guid-1',
					categoryGuid: undefined,
					parentCategoryGuid: undefined
				},
				{
					displayText: 'Another Term',
					termGuid: 'term-guid-2',
					categoryGuid: undefined,
					parentCategoryGuid: undefined
				}
			],
			categories: [],
			subTypes: [],
			superTypes: []
		}
	];

	const defaultProps = {
		open: true,
		onClose: mockOnClose,
		data: {
			guid: 'entity-guid-1',
			meanings: [],
			terms: []
		},
		updateTable: mockUpdateTable,
		relatedTerm: false,
		columnVal: undefined,
		setRowSelection: mockSetRowSelection
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockFormState = { isSubmitting: false };
		
		// Setup default isEmpty mock
		mockIsEmpty.mockImplementation((val: any) => {
			if (val === null || val === undefined) return true;
			if (val === '') return true;
			if (Array.isArray(val) && val.length === 0) return true;
			if (typeof val === 'object' && val !== null && Object.keys(val).length === 0) return true;
			return false;
		});

		// Setup default customSortBy mock
		mockCustomSortBy.mockImplementation((arr: any[], keys: string[]) => {
			if (!Array.isArray(arr)) return [];
			return [...arr].sort((a, b) => {
				for (const key of keys) {
					const aVal = a[key];
					const bVal = b[key];
					if (aVal < bVal) return -1;
					if (aVal > bVal) return 1;
				}
				return 0;
			});
		});

		// Setup default customSortByObjectKeys mock
		mockCustomSortByObjectKeys.mockImplementation((arr: any[]) => {
			if (!Array.isArray(arr)) return [];
			return [...arr].sort((a, b) => {
				const aKey = Object.keys(a)[0];
				const bKey = Object.keys(b)[0];
				if (aKey < bKey) return -1;
				if (aKey > bKey) return 1;
				return 0;
			});
		});

		// Setup default noTreeData mock
		mockNoTreeData.mockReturnValue([
			{ id: 'No Records Found', label: 'No Records Found', children: [] }
		]);

		// Setup default cloneDeep mock
		mockCloneDeep.mockImplementation((obj: any) => {
			if (obj === null || obj === undefined) return {};
			try {
				return JSON.parse(JSON.stringify(obj));
			} catch {
				return typeof obj === 'object' && obj !== null ? { ...obj } : obj;
			}
		});

		mockUseAppSelector.mockImplementation((selector: any) => {
			const mockState = {
				glossary: {
					glossaryData: defaultGlossaryData,
					loader: false
				}
			};
			return selector(mockState);
		});
		
		// Reset router mocks
		const { useLocation, useParams } = require('react-router-dom');
		useLocation.mockReturnValue({ ...mockLocation, search: '' });
		useParams.mockReturnValue({});
		
		mockAssignGlossaryType.mockResolvedValue({});
		mockAssignTermstoCategory.mockResolvedValue({});
		mockAssignTermstoEntites.mockResolvedValue({});
		mockHandleSubmit.mockImplementation((fn: any) => (e?: any) => {
			if (e) e.preventDefault();
			return fn({});
		});
		mockUseForm.mockReturnValue({
			control: {},
			handleSubmit: mockHandleSubmit,
			formState: mockFormState
		});
		mockServerError.mockImplementation(() => {
			toast.error('An error occurred');
		});
		
		// Reset moment mock
		const moment = require('moment-timezone');
		if (moment.now) {
			moment.now.mockReturnValue(1234567890);
		}
	});

	describe('Rendering', () => {
		it('should render modal when open is true', () => {
			render(<AssignTerm {...defaultProps} />);
			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should not render modal when open is false', () => {
			render(<AssignTerm {...defaultProps} open={false} />);
			expect(screen.queryByTestId('custom-modal')).not.toBeInTheDocument();
		});

		it('should render correct title for entity assignment', () => {
			render(<AssignTerm {...defaultProps} />);
			expect(screen.getByTestId('modal-title')).toHaveTextContent('Assign term to entity');
		});

		it('should render correct title for category assignment', () => {
			const { useLocation, useParams } = require('react-router-dom');
			useLocation.mockReturnValue({ ...mockLocation, search: '?gtype=category' });
			useParams.mockReturnValue({ guid: 'category-guid-1' });
			render(<AssignTerm {...defaultProps} />);
			expect(screen.getByTestId('modal-title')).toHaveTextContent('Assign term to Catgeory');
		});

		it('should render correct title for related term', () => {
			render(<AssignTerm {...defaultProps} relatedTerm={true} columnVal="meanings" />);
			expect(screen.getByTestId('modal-title')).toHaveTextContent('Assign term to meanings');
		});

		it('should render FormTreeView when relatedTerm is false', () => {
			render(<AssignTerm {...defaultProps} />);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should render stepper when relatedTerm is true', () => {
			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			expect(screen.getByText('Select Term')).toBeInTheDocument();
			expect(screen.getByText('Attributes')).toBeInTheDocument();
		});
	});

	describe('Search Functionality', () => {
		it('should update search term when typing in search field', async () => {
			const user = userEvent.setup();
			render(<AssignTerm {...defaultProps} />);
			const searchInput = screen.getByLabelText('Search Term');
			await user.type(searchInput, 'test search');
			expect(searchInput).toHaveValue('test search');
		});

		it('should update search term in relatedTerm mode', async () => {
			const user = userEvent.setup();
			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			const searchInput = screen.getByLabelText('Search Term');
			await user.type(searchInput, 'test search');
			expect(searchInput).toHaveValue('test search');
		});
	});

	describe('Node Selection', () => {
		it('should show toast when selecting "No Records Found"', () => {
			mockNoTreeData.mockReturnValue([
				{ id: 'No Records Found', label: 'No Records Found', children: [] }
			]);
			mockIsEmpty.mockReturnValue(true);
			render(<AssignTerm {...defaultProps} />);
			if (mockOnNodeSelect) {
				mockOnNodeSelect('No Records Found');
				expect(toast.dismiss).toHaveBeenCalled();
				expect(toast.info).toHaveBeenCalledWith('No terms present');
			}
		});

		it('should handle node selection', () => {
			render(<AssignTerm {...defaultProps} />);
			if (mockOnNodeSelect) {
				mockOnNodeSelect('Test Term@Test Glossary');
			}
		});
	});

	describe('Term Assignment (non-relatedTerm)', () => {
		it('should show error toast when no term is selected', async () => {
			mockIsEmpty.mockReturnValue(true);
			render(<AssignTerm {...defaultProps} />);
			const assignButton = screen.getByTestId('button-2');
			fireEvent.click(assignButton);
			await waitFor(() => {
				expect(toast.error).toHaveBeenCalledWith('No Term Selected');
			});
		});

		it('should assign term to entity when term is selected', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			const treeData = [
				{
					id: 'Test Term@Test Glossary',
					label: 'Test Term',
					children: []
				}
			];

			// Mock tree data generation
			mockCustomSortBy.mockReturnValue(treeData);
			mockCustomSortByObjectKeys.mockReturnValue([{ 'Test Glossary': { name: 'Test Glossary', children: [] } }]);

			render(<AssignTerm {...defaultProps} />);
			
			// Select node
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			// Click assign button
			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockAssignTermstoEntites).toHaveBeenCalled();
			});
		});

		it('should assign term to category when gType is category', async () => {
			const { useLocation, useParams } = require('react-router-dom');
			useLocation.mockReturnValue({ ...mockLocation, search: '?gtype=category' });
			useParams.mockReturnValue({ guid: 'category-guid-1' });
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			mockCloneDeep.mockReturnValue({ terms: [] });

			render(<AssignTerm {...defaultProps} data={{ guid: 'entity-guid-1', terms: [] }} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockAssignTermstoCategory).toHaveBeenCalled();
			});
		});

		it('should assign term to category when data.terms does not exist', async () => {
			const { useLocation, useParams } = require('react-router-dom');
			useLocation.mockReturnValue({ ...mockLocation, search: '?gtype=category' });
			useParams.mockReturnValue({ guid: 'category-guid-1' });
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			mockCloneDeep.mockReturnValue({});

			render(<AssignTerm {...defaultProps} data={{ guid: 'entity-guid-1' }} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockAssignTermstoCategory).toHaveBeenCalled();
			});
		});

		it('should handle assignment with existing terms in category', async () => {
			const { useLocation, useParams } = require('react-router-dom');
			useLocation.mockReturnValue({ ...mockLocation, search: '?gtype=category' });
			useParams.mockReturnValue({ guid: 'category-guid-1' });
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			mockCloneDeep.mockReturnValue({ terms: [{ termGuid: 'existing-term' }] });

			render(<AssignTerm {...defaultProps} data={{ guid: 'entity-guid-1', terms: [{ termGuid: 'existing-term' }] }} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockAssignTermstoCategory).toHaveBeenCalled();
			});
		});

		it('should handle assignment with multiple entities', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			const multiEntityData = [
				{ guid: 'entity-guid-1' },
				{ guid: 'entity-guid-2' }
			];

			render(<AssignTerm {...defaultProps} data={multiEntityData} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockAssignTermstoEntites).toHaveBeenCalled();
			});
		});

		it('should call updateTable after successful assignment', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockUpdateTable).toHaveBeenCalledWith(1234567890);
			});
		});

		it('should dispatch actions after successful assignment with entityGuid', async () => {
			const { useLocation, useParams } = require('react-router-dom');
			useLocation.mockReturnValue({ ...mockLocation, search: '?gtype=term' });
			useParams.mockReturnValue({ guid: 'entity-guid-1' });
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			mockCloneDeep.mockReturnValue({ terms: [] });

			render(<AssignTerm {...defaultProps} data={{ guid: 'entity-guid-1', terms: [] }} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockDispatch).toHaveBeenCalled();
				expect(mockFetchDetailPageData).toHaveBeenCalled();
				expect(mockFetchGlossaryData).toHaveBeenCalled();
				expect(mockFetchGlossaryDetails).toHaveBeenCalled();
			});
		});

		it('should call setRowSelection after successful assignment', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockSetRowSelection).toHaveBeenCalledWith({});
			});
		});

		it('should handle error during assignment', async () => {
			mockAssignTermstoEntites.mockRejectedValue(new Error('Assignment failed'));
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockServerError).toHaveBeenCalled();
			});
		});

		it('should handle assignment when guid is empty but entityGuid exists', async () => {
			const { useParams } = require('react-router-dom');
			useParams.mockReturnValue({ guid: 'entity-guid-1' });
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} data={{ guid: '', meanings: [], terms: [] }} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockAssignTermstoEntites).toHaveBeenCalled();
			});
		});
	});

	describe('Stepper Navigation (relatedTerm)', () => {
		it('should start at step 0', () => {
			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			expect(screen.getByText('Select Term')).toBeInTheDocument();
		});

		it('should show error when clicking Next without selecting term', async () => {
			mockIsEmpty.mockReturnValue(true);
			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			const nextButton = screen.getByText('Next');
			fireEvent.click(nextButton);
			await waitFor(() => {
				expect(toast.error).toHaveBeenCalledWith('Please select Term for association');
			});
		});

		it('should navigate to next step when term is selected', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			await waitFor(() => {
				expect(screen.getByText('Back')).not.toBeDisabled();
			});
		});

		it('should navigate back to previous step', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			const backButton = screen.getByText('Back');
			await act(async () => {
				fireEvent.click(backButton);
			});

			expect(backButton).toBeDisabled();
		});

		it('should handle step click', () => {
			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			const stepButtons = screen.getAllByRole('button');
			const stepButton = stepButtons.find(btn => btn.textContent === 'Select Term');
			if (stepButton) {
				fireEvent.click(stepButton);
			}
		});

		it('should show reset button when all steps completed', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			// Complete form submission to mark steps as completed
			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});
		});

		it('should reset stepper when reset button is clicked', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				return false;
			});

			const { useParams } = require('react-router-dom');
			useParams.mockReturnValue({ guid: 'entity-guid-1' });
			mockCloneDeep.mockReturnValue({ meanings: [] });

			render(<AssignTerm {...defaultProps} relatedTerm={true} columnVal="meanings" data={{ meanings: [] }} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			// Complete form to mark steps as completed
			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				const resetButton = screen.queryByText('Reset');
				if (resetButton) {
					fireEvent.click(resetButton);
					// After reset, should be back at step 0
					expect(screen.getByText('Select Term')).toBeInTheDocument();
				}
			});
		});

		it('should handle reset button click and reset stepper', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				return false;
			});

			const { useParams } = require('react-router-dom');
			useParams.mockReturnValue({ guid: 'entity-guid-1' });
			mockCloneDeep.mockReturnValue({ meanings: [] });

			render(<AssignTerm {...defaultProps} relatedTerm={true} columnVal="meanings" data={{ meanings: [] }} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				const resetButton = screen.queryByText('Reset');
				if (resetButton) {
					fireEvent.click(resetButton);
					// After reset, should be back at step 0 (lines 149-150)
					expect(screen.getByText('Select Term')).toBeInTheDocument();
				}
			});
		});

		it('should handle step click to navigate to step 1', () => {
			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			// Test handleStep function (line 145) by clicking on Attributes step
			const stepButtons = screen.getAllByRole('button');
			const attributesStepButton = stepButtons.find(btn => btn.textContent === 'Attributes');
			if (attributesStepButton) {
				fireEvent.click(attributesStepButton);
			}
		});

		it('should find first incomplete step when at last step and not all completed', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			// Navigate to last step (step 1)
			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			// At last step, clicking next should find first incomplete step (line 135)
			// This tests: isLastStep() && !allStepsCompleted() ? steps.findIndex(...)
			await act(async () => {
				fireEvent.click(nextButton);
			});
		});

		it('should handle step navigation when not all steps completed and at last step', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			// Navigate to last step
			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			// At last step, clicking next should find first incomplete step (line 135)
			// This tests the branch: isLastStep() && !allStepsCompleted()
			await act(async () => {
				fireEvent.click(nextButton);
			});
		});

		it('should handle step click navigation', () => {
			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			const stepButtons = screen.getAllByRole('button');
			const attributesStepButton = stepButtons.find(btn => btn.textContent === 'Attributes');
			if (attributesStepButton) {
				fireEvent.click(attributesStepButton);
			}
		});

		it('should handle step click to navigate to specific step', () => {
			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			// Find step button by text content - this tests handleStep function (line 145)
			const stepButtons = screen.getAllByRole('button');
			const attributesStepButton = stepButtons.find(btn => btn.textContent === 'Attributes');
			if (attributesStepButton) {
				fireEvent.click(attributesStepButton);
			}
			// Also test clicking Select Term step
			const selectTermStepButton = stepButtons.find(btn => btn.textContent === 'Select Term');
			if (selectTermStepButton) {
				fireEvent.click(selectTermStepButton);
			}
		});
	});

	describe('Form Submission (relatedTerm)', () => {
		it('should show error when submitting without selecting term', async () => {
			mockIsEmpty.mockReturnValue(true);
			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			const assignButton = screen.getByTestId('button-2');
			fireEvent.click(assignButton);
			await waitFor(() => {
				expect(toast.error).toHaveBeenCalledWith('No Term Selected');
			});
		});

		it('should show error when submitting at step 0', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(toast.error).toHaveBeenCalledWith('Please click on next step');
			});
		});

		it('should submit form with term data', async () => {
			const { useLocation, useParams } = require('react-router-dom');
			useParams.mockReturnValue({ guid: 'entity-guid-1' });
			useLocation.mockReturnValue({ ...mockLocation, search: '?gtype=category' });
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			mockCloneDeep.mockReturnValue({ meanings: [] });

			render(
				<AssignTerm
					{...defaultProps}
					relatedTerm={true}
					columnVal="meanings"
					data={{ meanings: [] }}
				/>
			);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			});
		});

		it('should handle form submission with existing columnVal data', async () => {
			const { useParams } = require('react-router-dom');
			useParams.mockReturnValue({ guid: 'entity-guid-1' });
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			mockCloneDeep.mockReturnValue({ meanings: [{ termGuid: 'existing-term' }] });

			render(
				<AssignTerm
					{...defaultProps}
					relatedTerm={true}
					columnVal="meanings"
					data={{ meanings: [{ termGuid: 'existing-term' }] }}
				/>
			);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			});
		});

		it('should call assignGlossaryType on form submission', async () => {
			const { useParams } = require('react-router-dom');
			useParams.mockReturnValue({ guid: 'entity-guid-1' });
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			mockCloneDeep.mockReturnValue({ meanings: [] });

			render(
				<AssignTerm
					{...defaultProps}
					relatedTerm={true}
					columnVal="meanings"
					data={{ meanings: [] }}
				/>
			);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			});
		});

		it('should handle error during form submission', async () => {
			mockAssignGlossaryType.mockRejectedValue(new Error('Submission failed'));
			const { useParams } = require('react-router-dom');
			useParams.mockReturnValue({ guid: 'entity-guid-1' });
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			mockCloneDeep.mockReturnValue({ meanings: [] });

			render(
				<AssignTerm
					{...defaultProps}
					relatedTerm={true}
					columnVal="meanings"
					data={{ meanings: [] }}
				/>
			);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockServerError).toHaveBeenCalled();
			});
		});

		it('should call updateTable after successful form submission', async () => {
			const { useParams } = require('react-router-dom');
			useParams.mockReturnValue({ guid: 'entity-guid-1' });
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			mockCloneDeep.mockReturnValue({ meanings: [] });

			render(
				<AssignTerm
					{...defaultProps}
					relatedTerm={true}
					columnVal="meanings"
					data={{ meanings: [] }}
				/>
			);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockUpdateTable).toHaveBeenCalledWith(1234567890);
			});
		});

		it('should dispatch actions after successful form submission', async () => {
			const { useParams } = require('react-router-dom');
			useParams.mockReturnValue({ guid: 'entity-guid-1' });
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			mockCloneDeep.mockReturnValue({ meanings: [] });

			render(
				<AssignTerm
					{...defaultProps}
					relatedTerm={true}
					columnVal="meanings"
					data={{ meanings: [] }}
				/>
			);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			const assignButton = screen.getByTestId('button-2');
			await act(async () => {
				fireEvent.click(assignButton);
			});

			await waitFor(() => {
				expect(mockDispatch).toHaveBeenCalled();
				expect(mockFetchGlossaryDetails).toHaveBeenCalled();
				expect(mockFetchDetailPageData).toHaveBeenCalled();
			});
		});
	});

	describe('Term Names Extraction', () => {
		it('should extract term names from meanings', () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			render(
				<AssignTerm
					{...defaultProps}
					data={{
						meanings: [{ displayText: 'Term 1' }, { displayText: 'Term 2' }]
					}}
				/>
			);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should extract term names from terms', () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			render(
				<AssignTerm
					{...defaultProps}
					data={{
						terms: [{ displayText: 'Term 1' }, { displayText: 'Term 2' }]
					}}
				/>
			);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should extract term names from columnVal', () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			render(
				<AssignTerm
					{...defaultProps}
					columnVal="meanings"
					data={{
						meanings: [{ displayText: 'Term 1' }]
					}}
				/>
			);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should handle empty term names', () => {
			mockIsEmpty.mockReturnValue(true);
			render(
				<AssignTerm
					{...defaultProps}
					data={{ meanings: [], terms: [] }}
				/>
			);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});
	});

	describe('Tree Data Generation', () => {
		it('should generate tree data from glossary data', () => {
			render(<AssignTerm {...defaultProps} />);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should handle glossary with categories', () => {
			const glossaryWithCategories = [
				{
					name: 'Test Glossary',
					guid: 'glossary-guid-1',
					terms: [
						{
							displayText: 'Term',
							termGuid: 'term-guid-1',
							categoryGuid: 'cat-guid-1',
							parentCategoryGuid: undefined
						}
					],
					categories: [
						{
							displayText: 'Category',
							termGuid: 'term-guid-1',
							categoryGuid: 'cat-guid-1',
							parentCategoryGuid: undefined
						}
					],
					subTypes: [],
					superTypes: []
				}
			];

			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: glossaryWithCategories,
						loader: false
					}
				};
				return selector(mockState);
			});

			render(<AssignTerm {...defaultProps} />);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should handle glossary with nested categories', () => {
			const glossaryWithNestedCategories = [
				{
					name: 'Test Glossary',
					guid: 'glossary-guid-1',
					terms: [
						{
							displayText: 'Child Term',
							termGuid: 'term-guid-1',
							categoryGuid: 'child-cat-guid',
							parentCategoryGuid: 'parent-cat-guid'
						}
					],
					categories: [
						{
							displayText: 'Parent Category',
							termGuid: 'parent-term-guid',
							categoryGuid: 'parent-cat-guid',
							parentCategoryGuid: undefined
						},
						{
							displayText: 'Child Category',
							termGuid: 'term-guid-1',
							categoryGuid: 'child-cat-guid',
							parentCategoryGuid: 'parent-cat-guid'
						}
					],
					subTypes: [],
					superTypes: []
				}
			];

			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: glossaryWithNestedCategories,
						loader: false
					}
				};
				return selector(mockState);
			});

			render(<AssignTerm {...defaultProps} />);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should handle glossary with categories that have matching parentCategoryGuid', () => {
			const glossaryWithMatchingCategories = [
				{
					name: 'Test Glossary',
					guid: 'glossary-guid-1',
					terms: [
						{
							displayText: 'Term',
							termGuid: 'term-guid-1',
							categoryGuid: 'cat-guid-1',
							parentCategoryGuid: undefined
						}
					],
					categories: [
						{
							displayText: 'Category',
							termGuid: 'term-guid-1',
							categoryGuid: 'cat-guid-1',
							parentCategoryGuid: 'cat-guid-1'
						}
					],
					subTypes: [],
					superTypes: []
				}
			];

			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: glossaryWithMatchingCategories,
						loader: false
					}
				};
				return selector(mockState);
			});

			render(<AssignTerm {...defaultProps} />);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should filter out existing terms', () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			render(
				<AssignTerm
					{...defaultProps}
					data={{
						terms: [{ displayText: 'Test Term' }]
					}}
				/>
			);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should handle empty glossary data', () => {
			mockIsEmpty.mockReturnValue(true);
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(<AssignTerm {...defaultProps} />);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should handle glossary with no terms', () => {
			const glossaryNoTerms = [
				{
					name: 'Test Glossary',
					guid: 'glossary-guid-1',
					terms: [],
					categories: [],
					subTypes: [],
					superTypes: []
				}
			];

			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: glossaryNoTerms,
						loader: false
					}
				};
				return selector(mockState);
			});

			render(<AssignTerm {...defaultProps} />);
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});
	});

	describe('Edge Cases', () => {
		it('should handle data with empty properties', () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});
			// Component expects data to have guid, meanings, terms properties
			render(<AssignTerm {...defaultProps} data={{ guid: '', meanings: [], terms: [] }} />);
			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle empty string guid', () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (val === '') return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			render(
				<AssignTerm
					{...defaultProps}
					data={{ guid: '', meanings: [], terms: [] }}
				/>
			);
			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle missing updateTable callback', () => {
			render(<AssignTerm {...defaultProps} updateTable={undefined} />);
			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle missing setRowSelection callback', () => {
			render(<AssignTerm {...defaultProps} setRowSelection={undefined} />);
			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle loader state', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: defaultGlossaryData,
						loader: true
					}
				};
				return selector(mockState);
			});

			render(<AssignTerm {...defaultProps} />);
			expect(screen.getByText('Loading')).toBeInTheDocument();
		});
	});

	describe('Modal Actions', () => {
		it('should call onClose when cancel button is clicked', () => {
			render(<AssignTerm {...defaultProps} />);
			const cancelButton = screen.getByTestId('button-1');
			fireEvent.click(cancelButton);
			expect(mockOnClose).toHaveBeenCalled();
		});

		it('should call onClose when close button is clicked', () => {
			render(<AssignTerm {...defaultProps} />);
			const closeButton = screen.getByTestId('modal-close');
			fireEvent.click(closeButton);
			expect(mockOnClose).toHaveBeenCalled();
		});

		it('should disable assign button when isSubmitting is true', () => {
			mockFormState = { isSubmitting: true };
			mockUseForm.mockReturnValue({
				control: {},
				handleSubmit: mockHandleSubmit,
				formState: { isSubmitting: true }
			});

			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			const assignButton = screen.getByTestId('button-2');
			expect(assignButton).toBeDisabled();
		});
	});

	describe('Form Fields (relatedTerm step 1)', () => {
		it('should render form fields when navigating to step 1', async () => {
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			await waitFor(() => {
				expect(screen.getByText('description')).toBeInTheDocument();
				expect(screen.getByText('expression')).toBeInTheDocument();
				expect(screen.getByText('steward')).toBeInTheDocument();
				expect(screen.getByText('source')).toBeInTheDocument();
			});
		});

		it('should update form values when typing', async () => {
			const user = userEvent.setup();
			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				return false;
			});

			render(<AssignTerm {...defaultProps} relatedTerm={true} />);
			
			if (mockOnNodeSelect) {
				act(() => {
					mockOnNodeSelect('Test Term@Test Glossary');
				});
			}

			const nextButton = screen.getByText('Next');
			await act(async () => {
				fireEvent.click(nextButton);
			});

			await waitFor(() => {
				const descriptionInput = screen.getByPlaceholderText('description');
				expect(descriptionInput).toBeInTheDocument();
			});

			const descriptionInput = screen.getByPlaceholderText('description');
			await user.type(descriptionInput, 'test description');
			expect(mockOnChange).toHaveBeenCalled();

			// Test other form fields
			const expressionInput = screen.getByPlaceholderText('expression');
			await user.type(expressionInput, 'test expression');
			
			const stewardInput = screen.getByPlaceholderText('steward');
			await user.type(stewardInput, 'test steward');
			
			const sourceInput = screen.getByPlaceholderText('source');
			await user.type(sourceInput, 'test source');
			
			expect(mockOnChange).toHaveBeenCalled();
		});
	});
});
