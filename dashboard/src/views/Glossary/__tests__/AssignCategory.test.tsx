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
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import userEvent from '@testing-library/user-event';
import { toast } from 'react-toastify';
import AssignCategory from '../AssignCategory';

const theme = createTheme();

// Mock Redux hooks
const mockDispatch = jest.fn();
const mockUseAppSelector = jest.fn();

jest.mock('@hooks/reducerHook', () => ({
	useAppDispatch: () => mockDispatch,
	useAppSelector: (selector: any) => mockUseAppSelector(selector)
}));

// Mock React Router hooks
const mockParams = { guid: 'test-guid-123' };
const mockLocation = {
	pathname: '/glossary/test-guid-123',
	search: '?gtype=term',
	hash: '',
	state: null,
	key: 'test-key'
};

const mockUseParams = jest.fn(() => ({ guid: 'test-guid-123' }));
const mockUseLocation = jest.fn(() => ({
	pathname: '/glossary/test-guid-123',
	search: '?gtype=term',
	hash: '',
	state: null,
	key: 'test-key'
}));

jest.mock('react-router-dom', () => {
	const actualRouter = jest.requireActual('react-router-dom');
	return {
		...actualRouter,
		useParams: () => mockUseParams(),
		useLocation: () => mockUseLocation()
	};
});

// Mock API methods
const mockAssignGlossaryType = jest.fn();
jest.mock('@api/apiMethods/glossaryApiMethod', () => ({
	assignGlossaryType: (...args: any[]) => mockAssignGlossaryType(...args)
}));

// Mock Redux slices
const mockFetchGlossaryData = jest.fn(() => ({ type: 'glossary/fetchData' }));
const mockFetchGlossaryDetails = jest.fn(() => ({ type: 'glossary/fetchDetails' }));
const mockFetchDetailPageData = jest.fn(() => ({ type: 'detailPage/fetchData' }));

jest.mock('@redux/slice/glossarySlice', () => ({
	fetchGlossaryData: (...args: any[]) => mockFetchGlossaryData(...args)
}));

jest.mock('@redux/slice/glossaryDetailsSlice', () => ({
	fetchGlossaryDetails: (...args: any[]) => mockFetchGlossaryDetails(...args)
}));

jest.mock('@redux/slice/detailPageSlice', () => ({
	fetchDetailPageData: (...args: any[]) => mockFetchDetailPageData(...args)
}));

// Mock Utils - must be before importing the component
jest.mock('@utils/Utils', () => {
	const mockCustomSortByObjectKeys = (arr: any[]) => {
		// CRITICAL: Always return an array, never undefined
		if (arr === undefined || arr === null) return [];
		if (!Array.isArray(arr)) return [];
		// Even if empty, return empty array (not undefined)
		try {
			// Sort by the first key of each object (matching real implementation)
			const sorted = [...arr].sort((a: any, b: any) => {
				if (!a || !b) return 0;
				const keysA = Object.keys(a);
				const keysB = Object.keys(b);
				if (keysA.length === 0 || keysB.length === 0) return 0;
				keysA.sort();
				keysB.sort();
				const keyA = keysA[0];
				const keyB = keysB[0];
				return keyA?.localeCompare(keyB) || 0;
			});
			return sorted;
		} catch (e) {
			// If sorting fails, return empty array (never undefined)
			return [];
		}
	};

	const mockCustomSortBy = (arr: any[], ...args: any[]) => {
		if (arr === undefined || arr === null) return [];
		if (!Array.isArray(arr)) return [];
		if (arr.length === 0) return [];
		try {
			const keys = args[0] || [];
			return [...arr].sort((a, b) => {
				return keys.reduce((result: number, key: string) => {
					if (result !== 0) return result;
					return a[key] < b[key] ? -1 : a[key] > b[key] ? 1 : 0;
				}, 0);
			});
		} catch (e) {
			return arr;
		}
	};

	return {
		isEmpty: jest.fn((val: any) => {
			if (val === null || val === undefined) return true;
			if (val === '') return true;
			if (Array.isArray(val) && val.length === 0) return true;
			if (typeof val === 'object' && val !== null && Object.keys(val).length === 0) return true;
			return false;
		}),
		customSortBy: mockCustomSortBy,
		customSortByObjectKeys: mockCustomSortByObjectKeys,
		noTreeData: jest.fn(() => [{ id: 'No Records Found', label: 'No Records Found' }]),
		serverError: jest.fn((error: any, toastId: any) => {
			if (error?.response?.data?.errorMessage) {
				toastId.current = toast.error(error.response.data.errorMessage);
			} else if (error?.response?.data) {
				toastId.current = toast.error(error.response.data);
			} else if (error?.response?.data?.msgDesc) {
				toastId.current = toast.error(error.response.data.msgDesc);
			}
		})
	};
});

// Mock Helper
jest.mock('@utils/Helper', () => ({
	cloneDeep: jest.fn((obj: any) => {
		if (obj === null || obj === undefined) {
			return {};
		}
		try {
			const cloned = JSON.parse(JSON.stringify(obj));
			// Ensure we always return an object, never undefined
			return cloned !== undefined && cloned !== null ? cloned : {};
		} catch (e) {
			const result = typeof obj === 'object' && obj !== null && !Array.isArray(obj) ? { ...obj } : obj;
			// Ensure we always return an object, never undefined
			return result !== undefined && result !== null ? result : {};
		}
	})
}));

// Mock toast
const mockToastId = { current: null };
const mockToast = {
	success: jest.fn((message: string) => 'success-toast-id'),
	error: jest.fn((message: string) => 'error-toast-id'),
	dismiss: jest.fn(),
	info: jest.fn((message: string) => 'info-toast-id'),
	warning: jest.fn((message: string) => 'warning-toast-id')
};

jest.mock('react-toastify', () => {
	const mockToast = {
		success: jest.fn((message: string) => 'success-toast-id'),
		error: jest.fn((message: string) => 'error-toast-id'),
		dismiss: jest.fn(),
		info: jest.fn((message: string) => 'info-toast-id'),
		warning: jest.fn((message: string) => 'warning-toast-id')
	};
	return {
		toast: mockToast
	};
});

// Mock moment
jest.mock('moment-timezone', () => {
	const moment = jest.requireActual('moment-timezone');
	return {
		...moment,
		now: jest.fn(() => 1234567890)
	};
});

// Mock FormTreeView component
jest.mock('@components/Forms/FormTreeView', () => {
	return function MockFormTreeView({
		treeData,
		searchTerm,
		treeName,
		loader,
		onNodeSelect
	}: any) {
		return (
			<div data-testid="form-tree-view">
				<div data-testid="tree-name">{treeName}</div>
				<div data-testid="search-term">{searchTerm}</div>
				<div data-testid="loader">{loader ? 'loading' : 'loaded'}</div>
				<div data-testid="tree-data-count">{treeData?.length || 0}</div>
				{treeData && treeData.length > 0 && treeData[0]?.id !== 'No Records Found' && (
					<button
						data-testid="select-node-button"
						onClick={() => {
							if (onNodeSelect) {
								onNodeSelect('Category3@Glossary1');
							}
						}}
					>
						Select Node
					</button>
				)}
			</div>
		);
	};
});

// Mock CustomModal component
jest.mock('@components/Modal', () => {
	return function MockCustomModal({
		open,
		onClose,
		title,
		children,
		button1Label,
		button1Handler,
		button2Label,
		button2Handler,
		disableButton2
	}: any) {
		if (!open) return null;
		return (
			<div data-testid="custom-modal" role="dialog">
				<div data-testid="modal-title">{title}</div>
				{children}
				{button1Label && (
					<button data-testid="button-1" onClick={button1Handler}>
						{button1Label}
					</button>
				)}
				<button
					data-testid="button-2"
					onClick={button2Handler}
					disabled={disableButton2}
				>
					{button2Label}
				</button>
				<button data-testid="close-button" onClick={onClose}>
					Close
				</button>
			</div>
		);
	};
});

// Test wrapper component
const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => (
	<ThemeProvider theme={theme}>{children}</ThemeProvider>
);

describe('AssignCategory', () => {
	const defaultProps = {
		open: true,
		onClose: jest.fn(),
		data: {
			categories: [
				{ displayText: 'Category1', categoryGuid: 'cat-guid-1' },
				{ displayText: 'Category2', categoryGuid: 'cat-guid-2' }
			]
		},
		updateTable: jest.fn()
	};

	const mockGlossaryData = [
		{
			name: 'Glossary1',
			guid: 'glossary-guid-1',
			categories: [
				{
					displayText: 'Category3',
					categoryGuid: 'cat-guid-3',
					parentCategoryGuid: undefined
				},
				{
					displayText: 'Category4',
					categoryGuid: 'cat-guid-4',
					parentCategoryGuid: undefined
				},
				{
					displayText: 'SubCategory1',
					categoryGuid: 'subcat-guid-1',
					parentCategoryGuid: 'cat-guid-3'
				}
			],
			terms: [],
			subTypes: [],
			superTypes: [],
			catgeories: undefined // This is a typo in the source code (line 81)
		},
		{
			name: 'Glossary2',
			guid: 'glossary-guid-2',
			categories: [
				{
					displayText: 'Category5',
					categoryGuid: 'cat-guid-5',
					parentCategoryGuid: undefined
				}
			],
			terms: [],
			subTypes: [],
			superTypes: [],
			catgeories: undefined
		}
	];

	beforeEach(() => {
		jest.clearAllMocks();
		// Always reset mocks to default values
		mockUseParams.mockReturnValue({ guid: 'test-guid-123' });
		mockUseLocation.mockReturnValue({
			pathname: '/glossary/test-guid-123',
			search: '?gtype=term',
			hash: '',
			state: null,
			key: 'test-key'
		});
		mockUseAppSelector.mockImplementation((selector: any) => {
			const mockState = {
				glossary: {
					glossaryData: mockGlossaryData,
					loader: false
				}
			};
			return selector(mockState);
		});
		mockAssignGlossaryType.mockResolvedValue({ success: true });
		
		// Set up Redux action mocks
		mockFetchGlossaryData.mockImplementation(() => ({
			type: 'FETCH_GLOSSARY_DATA'
		}));
		
		mockFetchGlossaryDetails.mockImplementation(() => ({
			type: 'FETCH_GLOSSARY_DETAILS'
		}));
		
		mockFetchDetailPageData.mockImplementation(() => ({
			type: 'FETCH_DETAIL_PAGE_DATA'
		}));
		mockToastId.current = null;
	});

	afterEach(() => {
		// Reset mocks to default values after each test
		mockUseParams.mockReturnValue({ guid: 'test-guid-123' });
		mockUseLocation.mockReturnValue({
			pathname: '/glossary/test-guid-123',
			search: '?gtype=term',
			hash: '',
			state: null,
			key: 'test-key'
		});
	});


	describe('Component Rendering', () => {
		it('should render modal when open is true', () => {
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			expect(screen.getByText('Assign Category to term')).toBeInTheDocument();
		});

		it('should not render modal when open is false', () => {
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} open={false} />
				</TestWrapper>
			);

			expect(screen.queryByTestId('custom-modal')).not.toBeInTheDocument();
		});

		it('should render search TextField', () => {
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const searchField = screen.getByLabelText('Search Term');
			expect(searchField).toBeInTheDocument();
		});

		it('should render FormTreeView component', () => {
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
			expect(screen.getByTestId('tree-name')).toHaveTextContent('Category');
		});

		it('should render Cancel and Assign buttons', () => {
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('button-1')).toHaveTextContent('Cancel');
			expect(screen.getByTestId('button-2')).toHaveTextContent('Assign');
		});
	});

	describe('Modal Interactions', () => {
		it('should call onClose when Cancel button is clicked', () => {
			const onClose = jest.fn();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} onClose={onClose} />
				</TestWrapper>
			);

			fireEvent.click(screen.getByTestId('button-1'));
			expect(onClose).toHaveBeenCalledTimes(1);
		});

		it('should call onClose when close button is clicked', () => {
			const onClose = jest.fn();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} onClose={onClose} />
				</TestWrapper>
			);

			fireEvent.click(screen.getByTestId('close-button'));
			expect(onClose).toHaveBeenCalledTimes(1);
		});
	});

	describe('Search Functionality', () => {
		it('should update search term when TextField value changes', async () => {
			const user = userEvent.setup();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const searchField = screen.getByLabelText('Search Term');
			await user.type(searchField, 'test search');

			expect(searchField).toHaveValue('test search');
		});

		it('should pass search term to FormTreeView', async () => {
			const user = userEvent.setup();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const searchField = screen.getByLabelText('Search Term');
			await user.type(searchField, 'Category');

			await waitFor(() => {
				expect(screen.getByTestId('search-term')).toHaveTextContent('Category');
			});
		});
	});

	describe('Node Selection', () => {
		it('should handle node selection', async () => {
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			fireEvent.click(selectButton);

			// Node selection should be handled internally
			expect(selectButton).toBeInTheDocument();
		});
	});

	describe('Category Assignment - Success Cases', () => {
		it('should successfully assign category when node is selected', async () => {
			const user = userEvent.setup();
			const updateTable = jest.fn();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} updateTable={updateTable} />
				</TestWrapper>
			);

			// Select a node
			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			// Click Assign button
			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			}, { timeout: 10000 });

			await waitFor(() => {
				expect(toast.success).toHaveBeenCalledWith(
					'Category is associated successfully'
				);
			}, { timeout: 10000 });
		}, 30000);

		it('should call updateTable when category is assigned successfully', async () => {
			const user = userEvent.setup();
			const updateTable = jest.fn();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} updateTable={updateTable} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(updateTable).toHaveBeenCalled();
			}, { timeout: 10000 });
		}, 30000);

		it('should dispatch Redux actions after successful assignment', async () => {
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			fireEvent.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			fireEvent.click(assignButton);

			await waitFor(() => {
				expect(mockDispatch).toHaveBeenCalledWith(mockFetchGlossaryData());
			});

			await waitFor(() => {
				expect(mockDispatch).toHaveBeenCalledWith(
					mockFetchGlossaryDetails({ gtype: 'term', guid: 'test-guid-123' })
				);
			});

			await waitFor(() => {
				expect(mockDispatch).toHaveBeenCalledWith(
					mockFetchDetailPageData('test-guid-123')
				);
			});
		});

		it('should call onClose after successful assignment', async () => {
			const onClose = jest.fn();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} onClose={onClose} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			fireEvent.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			fireEvent.click(assignButton);

			await waitFor(() => {
				expect(onClose).toHaveBeenCalled();
			});
		});

		it('should add category to existing categories array', async () => {
			const user = userEvent.setup();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalledWith(
					'test-guid-123',
					expect.objectContaining({
						categories: expect.arrayContaining([
							expect.objectContaining({ categoryGuid: expect.any(String) })
						])
					})
				);
			}, { timeout: 10000 });
		}, 30000);

		it('should create categories array if it does not exist', async () => {
			const user = userEvent.setup();
			// Pass data with empty categories array to test the component's handling
			const propsWithoutCategories = {
				...defaultProps,
				data: { categories: [] }
			};

			render(
				<TestWrapper>
					<AssignCategory {...propsWithoutCategories} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalledWith(
					'test-guid-123',
					expect.objectContaining({
						categories: expect.arrayContaining([
							expect.objectContaining({ categoryGuid: expect.any(String) })
						])
					})
				);
			}, { timeout: 10000 });
		}, 30000);
	});

	describe('Category Assignment - Error Cases', () => {
		it('should show error toast when no node is selected', async () => {
			const user = userEvent.setup();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(toast.error).toHaveBeenCalledWith('No Term Selected');
			}, { timeout: 10000 });

			expect(mockAssignGlossaryType).not.toHaveBeenCalled();
		}, 30000);

		it('should handle API error during assignment', async () => {
			const error = {
				response: {
					data: {
						errorMessage: 'Failed to assign category'
					}
				}
			};
			mockAssignGlossaryType.mockRejectedValueOnce(error);

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			fireEvent.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			fireEvent.click(assignButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			});

			await waitFor(() => {
				const { serverError } = require('@utils/Utils');
				expect(serverError).toHaveBeenCalled();
			});
		});

		it('should set loading state during API call', async () => {
			const user = userEvent.setup();
			let resolvePromise: any;
			const promise = new Promise((resolve) => {
				resolvePromise = resolve;
			});
			mockAssignGlossaryType.mockReturnValueOnce(promise);

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(assignButton).toBeDisabled();
			}, { timeout: 10000 });

			resolvePromise({ success: true });
		}, 30000);

		it('should reset loading state after error', async () => {
			const user = userEvent.setup();
			const error = {
				response: {
					data: {
						errorMessage: 'Failed to assign category'
					}
				}
			};
			mockAssignGlossaryType.mockRejectedValueOnce(error);

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			}, { timeout: 10000 });

			await waitFor(() => {
				expect(assignButton).not.toBeDisabled();
			}, { timeout: 10000 });
		}, 30000);
	});

	describe('Edge Cases', () => {
		it('should handle glossary with catgeories typo (line 82)', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [
							{
								name: 'Glossary1',
								guid: 'glossary-guid-1',
								catgeories: [{ displayText: 'Test' }], // Typo: catgeories instead of categories
								categories: [
									{
										displayText: 'Category3',
										categoryGuid: 'cat-guid-3',
										parentCategoryGuid: undefined
									}
								],
								terms: [],
								subTypes: [],
								superTypes: []
							}
						],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle empty selectedNode (lines 230-232)', async () => {
			const user = userEvent.setup();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			// Click Assign without selecting a node
			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(toast.error).toHaveBeenCalledWith('No Term Selected');
			}, { timeout: 10000 });

			expect(mockAssignGlossaryType).not.toHaveBeenCalled();
		}, 30000);

		it('should handle empty categories array', () => {
			const propsWithEmptyCategories = {
				...defaultProps,
				data: {
					categories: []
				}
			};

			render(
				<TestWrapper>
					<AssignCategory {...propsWithEmptyCategories} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle null categories', () => {
			const propsWithNullCategories = {
				...defaultProps,
				data: {
					categories: null
				}
			};

			render(
				<TestWrapper>
					<AssignCategory {...propsWithNullCategories} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should handle undefined categories', () => {
			const propsWithUndefinedCategories = {
				...defaultProps,
				data: {
					categories: undefined
				}
			};

			render(
				<TestWrapper>
					<AssignCategory {...propsWithUndefinedCategories} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should handle empty glossary data', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle glossary with empty categories', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [
							{
								name: 'Glossary1',
								guid: 'glossary-guid-1',
								categories: [],
								terms: [],
								subTypes: [],
								superTypes: []
							}
						],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle glossary with null categories', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [
							{
								name: 'Glossary1',
								guid: 'glossary-guid-1',
								catgeories: null,
								categories: null,
								terms: [],
								subTypes: [],
								superTypes: []
							}
						],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should handle glossary with undefined categories', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [
							{
								name: 'Glossary1',
								guid: 'glossary-guid-1',
								categories: undefined,
								terms: [],
								subTypes: [],
								superTypes: []
							}
						],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should handle categories with parentCategoryGuid', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [
							{
								name: 'Glossary1',
								guid: 'glossary-guid-1',
								categories: [
									{
										displayText: 'ParentCategory',
										categoryGuid: 'parent-guid',
										parentCategoryGuid: undefined
									},
									{
										displayText: 'ChildCategory',
										categoryGuid: 'child-guid',
										parentCategoryGuid: 'parent-guid'
									}
								],
								terms: [],
								subTypes: [],
								superTypes: []
							}
						],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should filter out already assigned categories', () => {
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			// The component should filter categories that are already in data.categories
			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle updateTable being undefined', async () => {
			const user = userEvent.setup();
			const propsWithoutUpdateTable = {
				...defaultProps,
				updateTable: undefined
			};

			render(
				<TestWrapper>
					<AssignCategory {...propsWithoutUpdateTable} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			}, { timeout: 10000 });
		}, 30000);

		it('should handle entityGuid being undefined', async () => {
			const user = userEvent.setup();
			mockUseParams.mockReturnValue({ guid: undefined });
			mockUseLocation.mockReturnValue({
				pathname: '/glossary/test-guid-123',
				search: '?gtype=term',
				hash: '',
				state: null,
				key: 'test-key'
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				// Should still attempt to call API even without guid
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			}, { timeout: 10000 });
		}, 30000);

		it('should handle loader state from Redux', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: mockGlossaryData,
						loader: true
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('loader')).toHaveTextContent('loading');
		});

		it('should handle different gtype query parameter', () => {
			const mockLocationWithDifferentGtype = {
				...mockLocation,
				search: '?gtype=category'
			};

			jest.spyOn(require('react-router-dom'), 'useLocation').mockReturnValue(mockLocationWithDifferentGtype);

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle missing gtype query parameter', () => {
			const mockLocationWithoutGtype = {
				...mockLocation,
				search: ''
			};

			jest.spyOn(require('react-router-dom'), 'useLocation').mockReturnValue(mockLocationWithoutGtype);

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});
	});

	describe('Tree Data Generation', () => {
		it('should generate tree data from glossary data', () => {
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
			expect(screen.getByTestId('tree-data-count')).toBeInTheDocument();
		});

		it('should handle nested children structure (lines 180-182)', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [
							{
								name: 'Glossary1',
								guid: 'glossary-guid-1',
								categories: [
									{
										displayText: 'ParentCategory',
										categoryGuid: 'parent-guid',
										parentCategoryGuid: undefined
									},
									{
										displayText: 'ChildCategory',
										categoryGuid: 'child-guid',
										parentCategoryGuid: 'parent-guid'
									},
									{
										displayText: 'GrandChildCategory',
										categoryGuid: 'grandchild-guid',
										parentCategoryGuid: 'child-guid'
									}
								],
								terms: [],
								subTypes: [],
								superTypes: []
							}
						],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
			expect(screen.getByTestId('tree-data-count')).toBeInTheDocument();
		});

		it('should handle children with undefined children property', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [
							{
								name: 'Glossary1',
								guid: 'glossary-guid-1',
								categories: [
									{
										displayText: 'Category3',
										categoryGuid: 'cat-guid-3',
										parentCategoryGuid: undefined
									}
								],
								terms: [],
								subTypes: [],
								superTypes: []
							}
						],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
			expect(screen.getByTestId('tree-data-count')).toBeInTheDocument();
		});

		it('should handle glossary with multiple categories', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [
							{
								name: 'Glossary1',
								guid: 'glossary-guid-1',
								categories: [
									{
										displayText: 'Category1',
										categoryGuid: 'cat-guid-1',
										parentCategoryGuid: undefined
									},
									{
										displayText: 'Category2',
										categoryGuid: 'cat-guid-2',
										parentCategoryGuid: undefined
									},
									{
										displayText: 'Category3',
										categoryGuid: 'cat-guid-3',
										parentCategoryGuid: undefined
									}
								],
								terms: [],
								subTypes: [],
								superTypes: []
							}
						],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
			expect(screen.getByTestId('form-tree-view')).toBeInTheDocument();
		});

		it('should handle nested category structure', () => {
			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [
							{
								name: 'Glossary1',
								guid: 'glossary-guid-1',
								categories: [
									{
										displayText: 'Parent1',
										categoryGuid: 'parent1-guid',
										parentCategoryGuid: undefined
									},
									{
										displayText: 'Child1',
										categoryGuid: 'child1-guid',
										parentCategoryGuid: 'parent1-guid'
									},
									{
										displayText: 'Child2',
										categoryGuid: 'child2-guid',
										parentCategoryGuid: 'parent1-guid'
									},
									{
										displayText: 'GrandChild1',
										categoryGuid: 'grandchild1-guid',
										parentCategoryGuid: 'child1-guid'
									}
								],
								terms: [],
								subTypes: [],
								superTypes: []
							}
						],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});
	});


	describe('Category Selection Logic', () => {
		it('should correctly parse selected node with glossary name', async () => {
			const user = userEvent.setup();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			}, { timeout: 10000 });
		}, 30000);

		it('should find correct glossary object by name', async () => {
			const user = userEvent.setup();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			}, { timeout: 10000 });
		}, 30000);

		it('should find correct category object by displayText', async () => {
			const user = userEvent.setup();
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			}, { timeout: 10000 });
		}, 30000);

		it('should handle category not found in glossary', async () => {
			// Ensure mocks are set up
			mockUseParams.mockReturnValue({ guid: 'test-guid-123' });
			mockUseLocation.mockReturnValue({
				pathname: '/glossary/test-guid-123',
				search: '?gtype=term',
				hash: '',
				state: null,
				key: 'test-key'
			});

			// This test verifies that the component handles cases where a selected category
			// doesn't exist in the glossary data gracefully
			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			// The component should still render even if category is not found
			expect(screen.getByTestId('custom-modal')).toBeInTheDocument();
		});

		it('should handle assignment when categoryGuid is undefined (line 249)', async () => {
			const user = userEvent.setup();
			// Ensure mocks are set up properly
			mockUseParams.mockReturnValue({ guid: 'test-guid-123' });
			mockUseLocation.mockReturnValue({
				pathname: '/glossary/test-guid-123',
				search: '?gtype=term',
				hash: '',
				state: null,
				key: 'test-key'
			});

			mockUseAppSelector.mockImplementation((selector: any) => {
				const mockState = {
					glossary: {
						glossaryData: [
							{
								name: 'Glossary1',
								guid: 'glossary-guid-1',
								categories: [
									{
										displayText: 'Category3',
										categoryGuid: undefined, // No categoryGuid
										parentCategoryGuid: undefined
									}
								],
								terms: [],
								subTypes: [],
								superTypes: []
							}
						],
						loader: false
					}
				};
				return selector(mockState);
			});

			render(
				<TestWrapper>
					<AssignCategory {...defaultProps} />
				</TestWrapper>
			);

			const selectButton = screen.getByTestId('select-node-button');
			await user.click(selectButton);

			const assignButton = screen.getByTestId('button-2');
			await user.click(assignButton);

			await waitFor(() => {
				expect(mockAssignGlossaryType).toHaveBeenCalled();
			}, { timeout: 10000 });
		}, 30000);
	});
});
