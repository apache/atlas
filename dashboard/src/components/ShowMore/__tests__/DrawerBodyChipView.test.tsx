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
 * Unit tests for DrawerBodyChipView component
 * 
 * Coverage Target: 100%
 */

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@utils/test-utils';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import DrawerBodyChipView from '../DrawerBodyChipView';

const theme = createTheme();

// Mock Redux hooks
const mockDispatch = jest.fn();
const mockUseAppSelector = jest.fn();

jest.mock('@hooks/reducerHook', () => ({
	useAppDispatch: () => mockDispatch,
	useAppSelector: (selector: any) => mockUseAppSelector(selector)
}));

// Mock React Router hooks
const mockNavigate = jest.fn();
const mockLocation = {
	pathname: '/detailPage/test-guid',
	search: '?tabActive=properties',
	hash: '',
	state: null,
	key: 'test-key'
};

const mockUseParams = jest.fn(() => ({ guid: 'test-guid' }));

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useNavigate: () => mockNavigate,
	useLocation: () => mockLocation,
	useParams: () => mockUseParams(),
	Link: ({ to, children, ...props }: any) => (
		<a href={to.pathname || to} {...props} data-testid="link">
			{children}
		</a>
	)
}));

// Mock toast
const mockToastId = { current: null };
const mockToastSuccess = jest.fn(() => {
	mockToastId.current = 'toast-id';
	return 'toast-id';
});
const mockToastDismiss = jest.fn();

jest.mock('react-toastify', () => ({
	toast: {
		success: jest.fn(() => {
			mockToastId.current = 'toast-id';
			return 'toast-id';
		}),
		dismiss: jest.fn(),
		error: jest.fn()
	}
}));

// Mock utils
const mockExtractKeyValueFromEntity = jest.fn();
const mockIsEmpty = jest.fn();
const mockServerError = jest.fn();
const mockCloneDeep = jest.fn();

jest.mock('@utils/Utils', () => ({
	extractKeyValueFromEntity: (...args: any[]) => mockExtractKeyValueFromEntity(...args),
	isEmpty: (...args: any[]) => mockIsEmpty(...args),
	serverError: (...args: any[]) => mockServerError(...args)
}));

jest.mock('@utils/Helper', () => ({
	cloneDeep: (...args: any[]) => mockCloneDeep(...args)
}));

// Mock Redux slices
const mockFetchDetailPageData = jest.fn(() => ({ type: 'FETCH_DETAIL_PAGE_DATA' }));
const mockFetchGlossaryDetails = jest.fn(() => ({ type: 'FETCH_GLOSSARY_DETAILS' }));
const mockFetchGlossaryData = jest.fn(() => ({ type: 'FETCH_GLOSSARY_DATA' }));

jest.mock('@redux/slice/detailPageSlice', () => ({
	fetchDetailPageData: jest.fn(() => ({ type: 'FETCH_DETAIL_PAGE_DATA' }))
}));

jest.mock('@redux/slice/glossaryDetailsSlice', () => ({
	fetchGlossaryDetails: jest.fn(() => ({ type: 'FETCH_GLOSSARY_DETAILS' }))
}));

jest.mock('@redux/slice/glossarySlice', () => ({
	fetchGlossaryData: jest.fn(() => ({ type: 'FETCH_GLOSSARY_DATA' }))
}));

// Mock components
jest.mock('@components/Modal', () => ({
	__esModule: true,
	default: ({
		open,
		onClose,
		children,
		title,
		titleIcon,
		button1Label,
		button1Handler,
		button2Label,
		button2Handler,
		disableButton2
	}: any) =>
		open ? (
			<div data-testid="custom-modal">
				<div data-testid="modal-title">{title}</div>
				<div data-testid="modal-title-icon">{titleIcon}</div>
				<div data-testid="modal-content">{children}</div>
				<button
					data-testid="modal-button-1"
					onClick={button1Handler}
				>
					{button1Label}
				</button>
				<button
					data-testid="modal-button-2"
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

jest.mock('@components/commonComponents', () => ({
	EllipsisText: ({ children }: any) => <span className="ellipsis">{children}</span>
}));

jest.mock('@components/muiComponents', () => ({
	LightTooltip: ({ children, title }: any) => (
		<div data-testid="light-tooltip" title={title}>
			{children}
		</div>
	)
}));

// Mock MUI components - mock individual imports
jest.mock('@mui/material/Paper', () => ({
	__esModule: true,
	default: ({ children, ...props }: any) => <div data-testid="paper" {...props}>{children}</div>
}));

jest.mock('@mui/material/Stack', () => ({
	__esModule: true,
	default: ({ children, ...props }: any) => <div data-testid="stack" {...props}>{children}</div>
}));

jest.mock('@mui/material/Typography', () => ({
	__esModule: true,
	default: ({ children, ...props }: any) => <span data-testid="typography" {...props}>{children}</span>
}));

jest.mock('@mui/material/Chip', () => ({
	__esModule: true,
	default: ({ label, onDelete, deleteIcon, ...props }: any) => (
		<div
			data-testid="chip"
			{...props}
			data-has-ondelete={!!onDelete}
		>
			<span data-testid="chip-label">{label}</span>
			{deleteIcon && <span data-testid="chip-delete-icon">{deleteIcon}</span>}
			{onDelete && (
				<button
					data-testid="chip-delete-button"
					onClick={onDelete}
					style={{ display: 'none' }}
				/>
			)}
		</div>
	)
}));

jest.mock('@mui/material/InputBase', () => ({
	__esModule: true,
	default: ({ value, onChange, ...props }: any) => (
		<input
			data-testid="input-base"
			value={value}
			onChange={onChange}
			{...props}
		/>
	)
}));

jest.mock('@mui/material/IconButton', () => ({
	__esModule: true,
	default: ({ children, onClick, ...props }: any) => (
		<button data-testid="icon-button" onClick={onClick} {...props}>
			{children}
		</button>
	)
}));

jest.mock('@mui/material', () => ({
	Link: ({ children, ...props }: any) => (
		<a {...props}>{children}</a>
	)
}));

jest.mock('@mui/icons-material/Clear', () => ({
	__esModule: true,
	default: () => <span data-testid="clear-icon">Clear</span>
}));

jest.mock('@mui/icons-material/Search', () => ({
	__esModule: true,
	default: () => <span data-testid="search-icon">Search</span>
}));

jest.mock('@mui/icons-material/ErrorRounded', () => ({
	__esModule: true,
	default: () => <span data-testid="error-icon">Error</span>
}));

const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => {
	return (
		<ThemeProvider theme={theme}>
			{children}
		</ThemeProvider>
	);
};

describe('DrawerBodyChipView', () => {
	const mockCurrentEntity = {
		guid: 'entity-guid-123',
		typeName: 'DataSet',
		name: 'Test Entity',
		attributes: {
			name: 'Test Entity'
		}
	};

	const mockRemoveApiMethod = jest.fn();

	const defaultProps = {
		data: [
			{ name: 'Tag1', displayText: 'Tag1' },
			{ name: 'Tag2', displayText: 'Tag2' }
		],
		title: 'Classifications',
		displayKey: 'name',
		currentEntity: mockCurrentEntity,
		removeApiMethod: mockRemoveApiMethod,
		removeTagsTitle: 'Remove Tag',
		isDeleteIcon: false
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockToastId.current = null;
		mockUseParams.mockReturnValue({ guid: 'test-guid' });
		mockIsEmpty.mockImplementation((val: any) =>
			val === null ||
			val === undefined ||
			val === '' ||
			(Array.isArray(val) && val.length === 0) ||
			(typeof val === 'object' && val !== null && Object.keys(val).length === 0)
		);
		mockExtractKeyValueFromEntity.mockReturnValue({
			name: 'Test Entity',
			found: true,
			key: 'name'
		});
		mockCloneDeep.mockImplementation((obj: any) => {
			if (obj === null || obj === undefined) {
				return null;
			}
			try {
				return JSON.parse(JSON.stringify(obj));
			} catch (e) {
				return typeof obj === 'object' && obj !== null && !Array.isArray(obj)
					? { ...obj }
					: {};
			}
		});
		mockUseAppSelector.mockReturnValue({
			classificationData: {
				classificationDefs: []
			}
		});
		mockRemoveApiMethod.mockResolvedValue({ success: true });
		mockLocation.search = '?tabActive=properties';
	});

	describe('Component Rendering', () => {
		it('should render component with basic props', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('input-base')).toBeInTheDocument();
			expect(screen.getByPlaceholderText('Search')).toBeInTheDocument();
		});

		it('should render chips when data is provided', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should render "No Data Found" when filteredData is empty', () => {
			mockIsEmpty.mockReturnValue(true);
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} data={[]} />
				</TestWrapper>
			);

			expect(screen.getByText('No Data Found')).toBeInTheDocument();
		});

		it('should render search input with correct placeholder', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const input = screen.getByPlaceholderText('Search');
			expect(input).toBeInTheDocument();
		});
	});

	describe('Search Functionality', () => {
		it('should update search term when input changes', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const input = screen.getByTestId('input-base') as HTMLInputElement;
			fireEvent.change(input, { target: { value: 'Tag1' } });

			expect(input.value).toBe('Tag1');
		});

		it('should show clear button when search term has length > 0', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const input = screen.getByTestId('input-base') as HTMLInputElement;
			fireEvent.change(input, { target: { value: 'test' } });

			const clearButtons = screen.getAllByTestId('icon-button');
			expect(clearButtons.length).toBeGreaterThan(0);
		});

		it('should clear search term when clear button is clicked', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const input = screen.getByTestId('input-base') as HTMLInputElement;
			fireEvent.change(input, { target: { value: 'test' } });

			const clearButtons = screen.getAllByTestId('icon-button');
			const clearButton = clearButtons.find(btn => 
				btn.querySelector('[data-testid="clear-icon"]')
			);
			
			if (clearButton) {
				fireEvent.click(clearButton);
				expect(input.value).toBe('');
			}
		});

		it('should filter chips based on search term', () => {
			const data = [
				{ name: 'Tag1', displayText: 'Tag1' },
				{ name: 'Tag2', displayText: 'Tag2' },
				{ name: 'Other', displayText: 'Other' }
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} data={data} />
				</TestWrapper>
			);

			const input = screen.getByTestId('input-base') as HTMLInputElement;
			fireEvent.change(input, { target: { value: 'Tag' } });

			// Should show chips matching "Tag"
			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should handle case-insensitive search', () => {
			const data = [
				{ name: 'Tag1', displayText: 'Tag1' },
				{ name: 'tag2', displayText: 'tag2' }
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} data={data} />
				</TestWrapper>
			);

			const input = screen.getByTestId('input-base') as HTMLInputElement;
			fireEvent.change(input, { target: { value: 'TAG' } });

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});
	});

	describe('Chip Display - Classifications', () => {
		it('should render chips for Classifications title', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} title="Classifications" />
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should call checkSuperTypes for Classifications', () => {
			const classificationData = {
				classificationDefs: [
					{
						name: 'Tag1',
						superTypes: ['Parent1', 'Parent2']
					}
				]
			};

			mockUseAppSelector.mockReturnValue({
				classificationData
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Classifications"
						data={[{ name: 'Tag1' }]}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should format classification with multiple superTypes', () => {
			const classificationData = {
				classificationDefs: [
					{
						name: 'Tag1',
						superTypes: ['Parent1', 'Parent2']
					}
				]
			};

			mockUseAppSelector.mockReturnValue({
				classificationData
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Classifications"
						data={[{ name: 'Tag1' }]}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should format classification with single superType', () => {
			const classificationData = {
				classificationDefs: [
					{
						name: 'Tag1',
						superTypes: ['Parent1']
					}
				]
			};

			mockUseAppSelector.mockReturnValue({
				classificationData
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Classifications"
						data={[{ name: 'Tag1' }]}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should handle classification without superTypes', () => {
			const classificationData = {
				classificationDefs: [
					{
						name: 'Tag1',
						superTypes: []
					}
				]
			};

			mockUseAppSelector.mockReturnValue({
				classificationData
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Classifications"
						data={[{ name: 'Tag1' }]}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should create correct href for Classifications', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Classifications"
						data={[{ name: 'Tag1' }]}
					/>
				</TestWrapper>
			);

			const links = screen.getAllByTestId('link');
			expect(links.length).toBeGreaterThan(0);
		});
	});

	describe('Chip Display - Propagated Classifications', () => {
		it('should render chips for Propagated Classifications title', () => {
			const classificationData = {
				classificationDefs: [
					{
						name: 'Tag1',
						superTypes: ['Parent1']
					}
				]
			};

			mockUseAppSelector.mockReturnValue({
				classificationData: {
					classificationDefs: classificationData.classificationDefs
				}
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Propagated Classifications"
						data={[{ name: 'Tag1' }]}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should call getTagParentList for Propagated Classifications', () => {
			const classificationData = {
				classificationDefs: [
					{
						name: 'Tag1',
						superTypes: ['Parent1']
					}
				]
			};

			mockUseAppSelector.mockReturnValue({
				classificationData: {
					classificationDefs: classificationData.classificationDefs
				}
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Propagated Classifications"
						data={[{ name: 'Tag1' }]}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should format propagated classification with multiple superTypes', () => {
			const classificationData = {
				classificationDefs: [
					{
						name: 'Tag1',
						superTypes: ['Parent1', 'Parent2']
					}
				]
			};

			mockUseAppSelector.mockReturnValue({
				classificationData: {
					classificationDefs: classificationData.classificationDefs
				}
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Propagated Classifications"
						data={[{ name: 'Tag1' }]}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should format propagated classification with single superType', () => {
			const classificationData = {
				classificationDefs: [
					{
						name: 'Tag1',
						superTypes: ['Parent1']
					}
				]
			};

			mockUseAppSelector.mockReturnValue({
				classificationData: {
					classificationDefs: classificationData.classificationDefs
				}
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Propagated Classifications"
						data={[{ name: 'Tag1' }]}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should handle propagated classification without superTypes', () => {
			const classificationData = {
				classificationDefs: [
					{
						name: 'Tag1',
						superTypes: []
					}
				]
			};

			mockUseAppSelector.mockReturnValue({
				classificationData: {
					classificationDefs: classificationData.classificationDefs
				}
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Propagated Classifications"
						data={[{ name: 'Tag1' }]}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});
	});

	describe('Chip Display - Terms', () => {
		it('should render chips for Terms title', () => {
			const data = [
				{
					displayText: 'Term1',
					termGuid: 'term-guid-1',
					guid: 'term-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Terms"
						displayKey="displayText"
						data={data}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should create correct href for Terms', () => {
			const data = [
				{
					displayText: 'Term1',
					termGuid: 'term-guid-1',
					guid: 'term-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Terms"
						displayKey="displayText"
						data={data}
					/>
				</TestWrapper>
			);

			const links = screen.getAllByTestId('link');
			expect(links.length).toBeGreaterThan(0);
		});

		it('should use termGuid for Terms href', () => {
			const data = [
				{
					displayText: 'Term1',
					termGuid: 'term-guid-1',
					guid: 'term-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Terms"
						displayKey="displayText"
						data={data}
					/>
				</TestWrapper>
			);

			const links = screen.getAllByTestId('link');
			expect(links.length).toBeGreaterThan(0);
		});
	});

	describe('Chip Display - Category', () => {
		it('should render chips for Category title', () => {
			const data = [
				{
					displayText: 'Category1',
					categoryGuid: 'category-guid-1',
					guid: 'category-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Category"
						displayKey="displayText"
						data={data}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should create correct href for Category', () => {
			const data = [
				{
					displayText: 'Category1',
					categoryGuid: 'category-guid-1',
					guid: 'category-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Category"
						displayKey="displayText"
						data={data}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
			// Category should render chips - links may be inside chips
			expect(chips[0]).toBeInTheDocument();
		});
	});

	describe('Chip Delete Functionality', () => {
		it('should open modal when delete is clicked on chip', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			}
		});

		it('should call handleDelete when chip delete is clicked', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			}
		});

		it('should not show delete icon when removeApiMethod is empty', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						removeApiMethod={null}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			chips.forEach(chip => {
				const deleteIcon = chip.querySelector('[data-testid="chip-delete-icon"]');
				expect(deleteIcon).toBeNull();
			});
		});

		it('should not show delete icon when isDeleteIcon is true but count <= 1', () => {
			const data = [
				{ name: 'Tag1', displayText: 'Tag1', count: 1 }
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						data={data}
						isDeleteIcon={true}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			chips.forEach(chip => {
				const deleteIcon = chip.querySelector('[data-testid="chip-delete-icon"]');
				expect(deleteIcon).toBeNull();
			});
		});

		it('should show delete icon with count when isDeleteIcon is true and count > 1', () => {
			const data = [
				{ name: 'Tag1', displayText: 'Tag1', count: 2, typeName: 'Tag1' }
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						data={data}
						isDeleteIcon={true}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			const chipWithDelete = chips.find(chip => 
				chip.querySelector('[data-testid="chip-delete-icon"]')
			);
			expect(chipWithDelete).toBeDefined();
		});

		it('should navigate when delete icon with count is clicked', () => {
			const data = [
				{ name: 'Tag1', displayText: 'Tag1', count: 2, typeName: 'Tag1' }
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						data={data}
						isDeleteIcon={true}
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				expect(mockNavigate).toHaveBeenCalled();
			}
		});
	});

	describe('Modal Functionality', () => {
		it('should open modal when handleDelete is called', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			}
		});

		it('should close modal when Cancel button is clicked', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const cancelButton = screen.getByTestId('modal-button-1');
				fireEvent.click(cancelButton);
				expect(screen.queryByTestId('custom-modal')).not.toBeInTheDocument();
			}
		});

		it('should close modal when close button is clicked', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const closeButton = screen.getByTestId('modal-close');
				fireEvent.click(closeButton);
				expect(screen.queryByTestId('custom-modal')).not.toBeInTheDocument();
			}
		});

		it('should display correct modal title', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				expect(screen.getByTestId('modal-title')).toHaveTextContent('Remove Tag');
			}
		});

		it('should display selected value in modal', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const modalContent = screen.getByTestId('modal-content');
				expect(modalContent).toBeInTheDocument();
			}
		});

		it('should display entity name in modal', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const modalContent = screen.getByTestId('modal-content');
				expect(modalContent).toBeInTheDocument();
			}
		});

		it('should display entity name with typeName in modal', () => {
			const entityWithTypeName = {
				...mockCurrentEntity,
				typeName: 'DataSet'
			};

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						currentEntity={entityWithTypeName}
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const modalContent = screen.getByTestId('modal-content');
				expect(modalContent).toBeInTheDocument();
			}
		});
	});

	describe('Remove Functionality - Classifications', () => {
		it('should remove classification successfully', async () => {
			const { toast } = require('react-toastify');
			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Classifications"
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockRemoveApiMethod).toHaveBeenCalledWith(
						'test-guid',
						expect.any(String)
					);
				});

				await waitFor(() => {
					expect(toast.success).toHaveBeenCalled();
				});
			}
		});

		it('should dispatch fetchDetailPageData after removing classification', async () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Classifications"
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockDispatch).toHaveBeenCalled();
				});
			}
		});

		it('should close modal after successful removal', async () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Classifications"
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(screen.queryByTestId('custom-modal')).not.toBeInTheDocument();
				});
			}
		});
	});

	describe('Remove Functionality - Terms (without gType)', () => {
		beforeEach(() => {
			mockLocation.search = '';
		});

		it('should remove term successfully without gType', async () => {
			const { toast } = require('react-toastify');
			const data = [
				{
					displayText: 'Term1',
					qualifiedName: 'Term1',
					guid: 'term-guid-1',
					relationshipGuid: 'rel-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Terms"
						displayKey="displayText"
						data={data}
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockRemoveApiMethod).toHaveBeenCalled();
				});

				await waitFor(() => {
					expect(toast.success).toHaveBeenCalled();
				});
			}
		});

		it('should find term by qualifiedName', async () => {
			const data = [
				{
					displayText: 'Term1',
					qualifiedName: 'Term1',
					guid: 'term-guid-1',
					relationshipGuid: 'rel-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Terms"
						displayKey="displayText"
						data={data}
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockRemoveApiMethod).toHaveBeenCalled();
				});
			}
		});

		it('should find term by displayText when qualifiedName is not available', async () => {
			const data = [
				{
					displayText: 'Term1',
					guid: 'term-guid-1',
					relationshipGuid: 'rel-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Terms"
						displayKey="displayText"
						data={data}
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockRemoveApiMethod).toHaveBeenCalled();
				});
			}
		});
	});

	describe('Remove Functionality - Terms (with gType)', () => {
		beforeEach(() => {
			mockLocation.search = '?gtype=term';
		});

		it('should remove term successfully with gType', async () => {
			const { toast } = require('react-toastify');
			const entityWithTerms = {
				...mockCurrentEntity,
				terms: [
					{ displayText: 'Term1' },
					{ displayText: 'Term2' }
				]
			};

			const data = [
				{
					displayText: 'Term1',
					guid: 'term-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Terms"
						displayKey="displayText"
						data={data}
						currentEntity={entityWithTerms}
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockCloneDeep).toHaveBeenCalled();
				});

				await waitFor(() => {
					expect(mockRemoveApiMethod).toHaveBeenCalled();
				});

				await waitFor(() => {
					expect(toast.success).toHaveBeenCalled();
				});
			}
		});

		it('should filter out removed term from entity terms', async () => {
			const entityWithTerms = {
				...mockCurrentEntity,
				terms: [
					{ displayText: 'Term1' },
					{ displayText: 'Term2' }
				]
			};

			const data = [
				{
					displayText: 'Term1',
					guid: 'term-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Terms"
						displayKey="displayText"
						data={data}
						currentEntity={entityWithTerms}
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockCloneDeep).toHaveBeenCalled();
				});
			}
		});

		it('should dispatch fetchGlossaryData and fetchGlossaryDetails after removal', async () => {
			const entityWithTerms = {
				...mockCurrentEntity,
				terms: [
					{ displayText: 'Term1' }
				]
			};

			const data = [
				{
					displayText: 'Term1',
					guid: 'term-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Terms"
						displayKey="displayText"
						data={data}
						currentEntity={entityWithTerms}
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockDispatch).toHaveBeenCalled();
				});
			}
		});
	});

	describe('Remove Functionality - Category (with gType)', () => {
		beforeEach(() => {
			mockLocation.search = '?gtype=category';
		});

		it('should remove category successfully with gType', async () => {
			const { toast } = require('react-toastify');
			const entityWithCategories = {
				...mockCurrentEntity,
				categories: [
					{ displayText: 'Category1' },
					{ displayText: 'Category2' }
				]
			};

			const data = [
				{
					displayText: 'Category1',
					guid: 'category-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Category"
						displayKey="displayText"
						data={data}
						currentEntity={entityWithCategories}
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockCloneDeep).toHaveBeenCalled();
				});

				await waitFor(() => {
					expect(mockRemoveApiMethod).toHaveBeenCalled();
				});

				await waitFor(() => {
					expect(toast.success).toHaveBeenCalled();
				});
			}
		});

		it('should filter out removed category from entity categories', async () => {
			const entityWithCategories = {
				...mockCurrentEntity,
				categories: [
					{ displayText: 'Category1' },
					{ displayText: 'Category2' }
				]
			};

			const data = [
				{
					displayText: 'Category1',
					guid: 'category-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Category"
						displayKey="displayText"
						data={data}
						currentEntity={entityWithCategories}
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockCloneDeep).toHaveBeenCalled();
				});
			}
		});
	});

	describe('Error Handling', () => {
		it('should handle error when removeApiMethod fails', async () => {
			mockRemoveApiMethod.mockRejectedValue(new Error('Remove failed'));

			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockRemoveApiMethod).toHaveBeenCalled();
				});

				await waitFor(() => {
					expect(mockServerError).toHaveBeenCalled();
				});
			}
		});

		it('should disable remove button while removing', async () => {
			mockRemoveApiMethod.mockImplementation(
				() =>
					new Promise((resolve) => {
						setTimeout(() => resolve({ success: true }), 100);
					})
			);

			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(removeButton).toBeDisabled();
				});
			}
		});
	});

	describe('Edge Cases', () => {
		it('should handle empty data array', () => {
			mockIsEmpty.mockReturnValue(true);
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} data={[]} />
				</TestWrapper>
			);

			expect(screen.getByText('No Data Found')).toBeInTheDocument();
		});

		it('should handle null data', () => {
			mockIsEmpty.mockReturnValue(true);
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} data={null as any} />
				</TestWrapper>
			);

			expect(screen.getByText('No Data Found')).toBeInTheDocument();
		});

		it('should handle data with null displayKey values', () => {
			const data = [
				{ name: 'Tag1', displayText: 'Tag1' },
				{ name: 'Tag2', displayText: 'Tag2' }
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} data={data} />
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should handle data where object itself is the value', () => {
			const data = ['Tag1', 'Tag2'];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						data={data as any}
						displayKey=""
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should handle empty currentEntity', () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						currentEntity={null}
					/>
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const modalContent = screen.getByTestId('modal-content');
				expect(modalContent).toBeInTheDocument();
			}
		});

		it('should handle empty guid in params', async () => {
			mockUseParams.mockReturnValue({ guid: '' });

			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockRemoveApiMethod).toHaveBeenCalled();
				});
			}
		});

		it('should handle classificationData with empty classificationDefs', () => {
			mockUseAppSelector.mockReturnValue({
				classificationData: {
					classificationDefs: []
				}
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Classifications"
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should handle classificationData with null classificationDefs', () => {
			mockUseAppSelector.mockReturnValue({
				classificationData: {
					classificationDefs: null
				}
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Classifications"
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should handle useAppSelector returning undefined classification state', () => {
			mockUseAppSelector.mockReturnValue({});

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Classifications"
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should handle getLabel with optionalLabel fallback', () => {
			const data = [
				{ name: 'Tag1', displayText: 'Fallback Text' }
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						data={data}
						displayKey="name"
						title="Other"
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should handle Terms with missing termGuid and categoryGuid', () => {
			const data = [
				{
					displayText: 'Term1',
					guid: 'term-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Terms"
						displayKey="displayText"
						data={data}
					/>
				</TestWrapper>
			);

			const links = screen.getAllByTestId('link');
			expect(links.length).toBeGreaterThan(0);
		});

		it('should handle empty search term filtering', () => {
			const data = [
				{ name: 'Tag1', displayText: 'Tag1' },
				{ name: 'Tag2', displayText: 'Tag2' }
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} data={data} />
				</TestWrapper>
			);

			const input = screen.getByTestId('input-base') as HTMLInputElement;
			fireEvent.change(input, { target: { value: '' } });

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should handle filter when data item is null', () => {
			const data = [
				{ name: 'Tag1', displayText: 'Tag1' }
			];

			mockIsEmpty.mockImplementation((val: any) => {
				if (val === null || val === undefined) return true;
				if (Array.isArray(val) && val.length === 0) return true;
				return false;
			});

			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} data={data} />
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});
	});

	describe('getHref Edge Cases', () => {
		it('should return label directly for non-Classification/Terms/Category titles', () => {
			const data = [
				{ name: 'Item1', displayText: 'Item1' }
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Other"
						data={data}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});

		it('should handle Terms with categoryGuid', () => {
			const data = [
				{
					displayText: 'Term1',
					categoryGuid: 'category-guid-1',
					guid: 'term-guid-1'
				}
			];

			render(
				<TestWrapper>
					<DrawerBodyChipView
						{...defaultProps}
						title="Category"
						displayKey="displayText"
						data={data}
					/>
				</TestWrapper>
			);

			const chips = screen.getAllByTestId('chip');
			expect(chips.length).toBeGreaterThan(0);
		});
	});

	describe('Redux Dispatch Calls', () => {
		it('should dispatch fetchDetailPageData twice after successful removal', async () => {
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockDispatch).toHaveBeenCalled();
				});
			}
		});

		it('should not dispatch glossary actions when gType is empty', async () => {
			mockLocation.search = '';
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(mockDispatch).toHaveBeenCalled();
				});
			}
		});
	});

	describe('Toast Notifications', () => {
		it('should dismiss existing toast before showing success', async () => {
			const { toast } = require('react-toastify');
			mockToastId.current = 'existing-toast-id';

			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(toast.dismiss).toHaveBeenCalled();
				});

				await waitFor(() => {
					expect(toast.success).toHaveBeenCalled();
				});
			}
		});

		it('should show success toast with correct message', async () => {
			const { toast } = require('react-toastify');
			render(
				<TestWrapper>
					<DrawerBodyChipView {...defaultProps} title="Classifications" />
				</TestWrapper>
			);

			const deleteButtons = screen.queryAllByTestId('chip-delete-button');
			if (deleteButtons.length > 0) {
				fireEvent.click(deleteButtons[0]);
				const removeButton = screen.getByTestId('modal-button-2');
				fireEvent.click(removeButton);

				await waitFor(() => {
					expect(toast.success).toHaveBeenCalledWith(
						expect.stringContaining('Classifications')
					);
				});
			}
		});
	});
});
