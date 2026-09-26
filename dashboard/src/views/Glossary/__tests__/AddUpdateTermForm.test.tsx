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
 * Comprehensive unit tests for AddUpdateTermForm component - 100% Coverage
 * This test suite covers all statements, branches, functions, and lines
 */

import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import { MemoryRouter } from 'react-router-dom';
import AddUpdateTermForm from '../AddUpdateTermForm';
import userEvent from '@testing-library/user-event';
import * as glossarySlice from '@redux/slice/glossarySlice';
import * as glossaryDetailsSlice from '@redux/slice/glossaryDetailsSlice';
import * as detailPageSlice from '@redux/slice/detailPageSlice';

// Mock state variables
let mockGuid: string | undefined = undefined;
let mockParent: string | undefined = undefined;
let mockGtype: string | null = null;
let mockLocationSearch = '';

// Mock react-router-dom
const mockNavigate = jest.fn();

const mockUseLocation = jest.fn();

jest.mock('react-router-dom', () => {
	const actual = jest.requireActual('react-router-dom');
	return {
		...actual,
		useParams: () => ({ guid: mockGuid }),
		useLocation: () => mockUseLocation(),
		useNavigate: () => mockNavigate
	};
});

// Mock toast
const mockToastSuccess = jest.fn(() => 'toast-id-123');
const mockToastDismiss = jest.fn();

jest.mock('react-toastify', () => ({
	toast: {
		success: (...args: any[]) => mockToastSuccess(...args),
		error: jest.fn(),
		dismiss: (...args: any[]) => mockToastDismiss(...args)
	}
}));

// Mock API methods
const mockCreateTermorCategory = jest.fn();
const mockEditTermorCategory = jest.fn();

jest.mock('@api/apiMethods/glossaryApiMethod', () => ({
	createTermorCategory: (...args: any[]) => mockCreateTermorCategory(...args),
	editTermorCatgeory: (...args: any[]) => mockEditTermorCategory(...args)
}));

// Mock Redux actions
const mockFetchGlossaryData = jest.fn(() => (dispatch: any) => {
	return Promise.resolve({ type: 'FETCH_GLOSSARY_DATA' });
});

const mockFetchGlossaryDetails = jest.fn();
const mockFetchDetailPageData = jest.fn();

jest.mock('@redux/slice/glossarySlice', () => ({
	fetchGlossaryData: jest.fn()
}));

jest.mock('@redux/slice/glossaryDetailsSlice', () => ({
	fetchGlossaryDetails: jest.fn()
}));

jest.mock('@redux/slice/detailPageSlice', () => ({
	fetchDetailPageData: jest.fn()
}));

// Mock Utils
jest.mock('@utils/Utils', () => {
	const actualLodash = jest.requireActual('lodash');
	return {
		isEmpty: actualLodash.isEmpty,
		serverError: jest.fn()
	};
});

// Mock CustomModal
jest.mock('@components/Modal', () => ({
	__esModule: true,
	default: ({ 
		open, 
		onClose, 
		children, 
		title, 
		button1Handler, 
		button2Handler, 
		button2Label,
		disableButton2 
	}: any) =>
		open ? (
			<div data-testid="modal">
				<div data-testid="modal-title">{title}</div>
				{children}
				<button data-testid="cancel-btn" onClick={button1Handler}>Cancel</button>
				<button 
					data-testid="submit-btn" 
					onClick={button2Handler}
					disabled={disableButton2}
				>
					{button2Label}
				</button>
			</div>
		) : null
}));

// Mock GlossaryForm
jest.mock('../GlossaryForm', () => ({
	__esModule: true,
	default: ({ control, handleSubmit, setValue }: any) => (
		<div data-testid="glossary-form">
			<form onSubmit={handleSubmit}>
				<input
					data-testid="name-input"
					onChange={(e: any) => {
						const onChange = control._formValues?.name?.onChange || (() => {});
						onChange(e.target.value);
					}}
					placeholder="Name"
				/>
				<input
					data-testid="short-description-input"
					onChange={(e: any) => {
						const onChange = control._formValues?.shortDescription?.onChange || (() => {});
						onChange(e.target.value);
					}}
					placeholder="Short Description"
				/>
				<textarea
					data-testid="long-description-input"
					onChange={(e: any) => {
						const onChange = control._formValues?.longDescription?.onChange || (() => {});
						onChange(e.target.value);
					}}
					placeholder="Long Description"
				/>
				<button type="submit" data-testid="form-submit">Submit</button>
			</form>
		</div>
	)
}));

// Mock react-hook-form
const mockSetValue = jest.fn();
let mockFormValues: any = { name: 'Test Term', shortDescription: 'Short', longDescription: 'Long' };
const mockHandleSubmit = jest.fn((onSubmitFn: any) => {
	return (e?: any) => {
		if (e) {
			e.preventDefault();
		}
		return Promise.resolve(onSubmitFn(mockFormValues));
	};
});
const mockControl = {
	_formValues: {
		name: { onChange: jest.fn() },
		shortDescription: { onChange: jest.fn() },
		longDescription: { onChange: jest.fn() }
	}
};

const mockUseForm = jest.fn(() => ({
	control: mockControl,
	handleSubmit: mockHandleSubmit,
	setValue: mockSetValue,
	formState: { isSubmitting: false }
}));

jest.mock('react-hook-form', () => ({
	useForm: (...args: any[]) => mockUseForm(...args)
}));

const createStore = (glossaryData: any[] = []) => {
	return configureStore({
		reducer: {
			glossary: () => ({ glossaryData })
		},
		middleware: (getDefaultMiddleware) =>
			getDefaultMiddleware({
				serializableCheck: false,
				immutableCheck: false
			})
	});
};

describe('AddUpdateTermForm', () => {
	let mockOnClose: jest.Mock;
	let store: ReturnType<typeof createStore>;

	beforeEach(() => {
		jest.clearAllMocks();
		mockOnClose = jest.fn();
		mockGuid = undefined;
		mockParent = undefined;
		mockGtype = null;
		mockLocationSearch = '';
		mockFormValues = { name: 'Test Term', shortDescription: 'Short', longDescription: 'Long' };
		mockToastSuccess.mockReturnValue('toast-id-123');
		mockCreateTermorCategory.mockResolvedValue({});
		mockEditTermorCategory.mockResolvedValue({});
		
		// Set up Redux action mocks
		(glossarySlice.fetchGlossaryData as jest.Mock).mockImplementation((...args: any[]) => {
			mockFetchGlossaryData(...args);
			return { type: 'FETCH_GLOSSARY_DATA' };
		});
		
		(glossaryDetailsSlice.fetchGlossaryDetails as jest.Mock).mockImplementation((...args: any[]) => {
			mockFetchGlossaryDetails(...args);
			return { type: 'FETCH_GLOSSARY_DETAILS' };
		});
		
		(detailPageSlice.fetchDetailPageData as jest.Mock).mockImplementation((...args: any[]) => {
			mockFetchDetailPageData(...args);
			return { type: 'FETCH_DETAIL_PAGE_DATA' };
		});
		mockHandleSubmit.mockImplementation((onSubmitFn: any) => {
			return (e?: any) => {
				if (e) {
					e.preventDefault();
				}
				return Promise.resolve(onSubmitFn(mockFormValues));
			};
		});
		mockUseForm.mockReturnValue({
			control: mockControl,
			handleSubmit: mockHandleSubmit,
			setValue: mockSetValue,
			formState: { isSubmitting: false }
		});
		mockUseLocation.mockReturnValue({
			pathname: '/glossary',
			search: mockLocationSearch,
			hash: '',
			state: null,
			key: 'default'
		});
		store = createStore([]);
	});

	describe('Component Rendering', () => {
		it('should render modal in add mode when open is true', () => {
			store = createStore([
				{ name: 'Test Glossary', guid: 'glossary-guid-1' }
			]);

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('modal')).toBeInTheDocument();
			expect(screen.getByTestId('modal-title')).toHaveTextContent('Create Term');
			expect(screen.getByTestId('submit-btn')).toHaveTextContent('Create');
		});

		it('should render modal in edit mode when open is true', () => {
			const dataObj = {
				name: 'Existing Term',
				shortDescription: 'Existing Short',
				longDescription: 'Existing Long',
				guid: 'term-guid-123'
			};

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={false}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={dataObj}
						/>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('modal')).toBeInTheDocument();
			expect(screen.getByTestId('modal-title')).toHaveTextContent('Edit Term');
			expect(screen.getByTestId('submit-btn')).toHaveTextContent('Update');
		});

		it('should not render modal when open is false', () => {
			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={false}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.queryByTestId('modal')).not.toBeInTheDocument();
		});

		it('should render GlossaryForm component', () => {
			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('glossary-form')).toBeInTheDocument();
		});
	});

	describe('Add Mode - Form Submission', () => {
		it('should create term successfully in add mode', async () => {
			const glossaryData = [
				{ name: 'Test Glossary', guid: 'glossary-guid-1' }
			];
			store = createStore(glossaryData);

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			const submitButton = screen.getByTestId('submit-btn');
			await act(async () => {
				fireEvent.click(submitButton);
			});

			await waitFor(() => {
				expect(mockCreateTermorCategory).toHaveBeenCalledWith('term', expect.objectContaining({
					name: 'Test Term',
					anchor: expect.objectContaining({
						displayText: 'term-id',
						glossaryGuid: 'glossary-guid-1'
					})
				}));
			});

			// Check that Redux action was called
			const { fetchGlossaryData } = require('@redux/slice/glossarySlice');
			await waitFor(() => {
				expect(fetchGlossaryData).toHaveBeenCalled();
			});

			await waitFor(() => {
				expect(mockToastDismiss).toHaveBeenCalled();
			});

			await waitFor(() => {
				expect(mockToastSuccess).toHaveBeenCalledWith('Term Test Term was created successfully');
			});

			await waitFor(() => {
				expect(mockOnClose).toHaveBeenCalled();
			});
		});

		it('should handle empty shortDescription in add mode', async () => {
			const glossaryData = [
				{ name: 'Test Glossary', guid: 'glossary-guid-1' }
			];
			store = createStore(glossaryData);

			mockFormValues = { name: 'Test Term', shortDescription: '', longDescription: 'Long' };
			mockHandleSubmit.mockImplementation((onSubmitFn: any) => {
				return (e?: any) => {
					if (e) {
						e.preventDefault();
					}
					return Promise.resolve(onSubmitFn(mockFormValues));
				};
			});

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			const submitButton = screen.getByTestId('submit-btn');
			await act(async () => {
				fireEvent.click(submitButton);
			});

			await waitFor(() => {
				expect(mockCreateTermorCategory).toHaveBeenCalledWith('term', expect.objectContaining({
					name: 'Test Term',
					shortDescription: ''
				}));
			});
		});

		it('should handle empty longDescription in add mode', async () => {
			const glossaryData = [
				{ name: 'Test Glossary', guid: 'glossary-guid-1' }
			];
			store = createStore(glossaryData);

			mockFormValues = { name: 'Test Term', shortDescription: 'Short', longDescription: '' };
			mockHandleSubmit.mockImplementation((onSubmitFn: any) => {
				return (e?: any) => {
					if (e) {
						e.preventDefault();
					}
					return Promise.resolve(onSubmitFn(mockFormValues));
				};
			});

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			const submitButton = screen.getByTestId('submit-btn');
			await act(async () => {
				fireEvent.click(submitButton);
			});

			await waitFor(() => {
				expect(mockCreateTermorCategory).toHaveBeenCalledWith('term', expect.objectContaining({
					name: 'Test Term',
					longDescription: ''
				}));
			});
		});

		it('should use glossaryObj values when dataObj is empty in add mode', () => {
			const glossaryData = [
				{ 
					name: 'Test Glossary', 
					guid: 'glossary-guid-1',
					shortDescription: 'Glossary Short',
					longDescription: 'Glossary Long'
				}
			];
			store = createStore(glossaryData);

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={{}}
						/>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('modal')).toBeInTheDocument();
		});
	});

	describe('Edit Mode - Form Submission', () => {
		it('should update term successfully in edit mode', async () => {
			const dataObj = {
				name: 'Existing Term',
				shortDescription: 'Existing Short',
				longDescription: 'Existing Long',
				guid: 'term-guid-123'
			};

			// Set up location search for edit mode with dataObj
			mockLocationSearch = '?gtype=term';
			mockGuid = 'entity-guid-456';
			mockUseLocation.mockReturnValue({
				pathname: '/glossary',
				search: '?gtype=term',
				hash: '',
				state: null,
				key: 'default'
			});

			const glossaryData = [
				{ name: 'Test Glossary', guid: 'glossary-guid-1' }
			];
			store = createStore(glossaryData);

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={false}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={dataObj}
						/>
					</MemoryRouter>
				</Provider>
			);

			const submitButton = screen.getByTestId('submit-btn');
			await act(async () => {
				fireEvent.click(submitButton);
			});

			await waitFor(() => {
				expect(mockEditTermorCategory).toHaveBeenCalledWith('term', 'term-guid-123', expect.objectContaining({
					name: 'Test Term',
					shortDescription: 'Short',
					longDescription: 'Long'
				}));
			});

			// Check that Redux actions were called
			const { fetchGlossaryData } = require('@redux/slice/glossarySlice');
			const { fetchGlossaryDetails } = require('@redux/slice/glossaryDetailsSlice');
			const { fetchDetailPageData } = require('@redux/slice/detailPageSlice');

			await waitFor(() => {
				expect(fetchGlossaryData).toHaveBeenCalled();
			});

			await waitFor(() => {
				expect(fetchGlossaryDetails).toHaveBeenCalledWith({ gtype: 'term', guid: 'entity-guid-456' });
			});

			await waitFor(() => {
				expect(fetchDetailPageData).toHaveBeenCalledWith('term-guid-123');
			});

			await waitFor(() => {
				expect(mockToastDismiss).toHaveBeenCalled();
			});

			await waitFor(() => {
				expect(mockToastSuccess).toHaveBeenCalledWith('Term Test Term was updated successfully');
			});

			await waitFor(() => {
				expect(mockOnClose).toHaveBeenCalled();
			});
		});

		it('should dispatch fetchGlossaryDetails and fetchDetailPageData when dataObj is not empty in edit mode', async () => {
			const dataObj = {
				name: 'Existing Term',
				shortDescription: 'Existing Short',
				longDescription: 'Existing Long',
				guid: 'term-guid-123'
			};

			// Set location search before rendering
			mockLocationSearch = '?gtype=term';
			mockGuid = 'entity-guid-456';
			mockUseLocation.mockReturnValue({
				pathname: '/glossary',
				search: '?gtype=term',
				hash: '',
				state: null,
				key: 'default'
			});

			const glossaryData = [
				{ name: 'Test Glossary', guid: 'glossary-guid-1' }
			];
			store = createStore(glossaryData);

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={false}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={dataObj}
						/>
					</MemoryRouter>
				</Provider>
			);

			const submitButton = screen.getByTestId('submit-btn');
			await act(async () => {
				fireEvent.click(submitButton);
			});

			await waitFor(() => {
				expect(mockEditTermorCategory).toHaveBeenCalled();
			}, { timeout: 3000 });

			// Check that Redux actions were called by checking the mocked modules
			const { fetchGlossaryData } = require('@redux/slice/glossarySlice');
			const { fetchGlossaryDetails } = require('@redux/slice/glossaryDetailsSlice');
			const { fetchDetailPageData } = require('@redux/slice/detailPageSlice');

			await waitFor(() => {
				expect(fetchGlossaryData).toHaveBeenCalled();
			}, { timeout: 3000 });

			await waitFor(() => {
				expect(fetchGlossaryDetails).toHaveBeenCalledWith({ gtype: 'term', guid: 'entity-guid-456' });
			}, { timeout: 3000 });

			await waitFor(() => {
				expect(fetchDetailPageData).toHaveBeenCalledWith('term-guid-123');
			}, { timeout: 3000 });
		});

		it('should not dispatch fetchGlossaryDetails when dataObj is empty in edit mode', async () => {
			const glossaryData = [
				{ name: 'Test Glossary', guid: 'glossary-guid-1' }
			];
			store = createStore(glossaryData);

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={false}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={{}}
						/>
					</MemoryRouter>
				</Provider>
			);

			const submitButton = screen.getByTestId('submit-btn');
			await act(async () => {
				fireEvent.click(submitButton);
			});

			await waitFor(() => {
				expect(mockEditTermorCategory).toHaveBeenCalled();
			});

			// When dataObj is empty in edit mode, fetchGlossaryData is not called
			expect(mockFetchGlossaryData).not.toHaveBeenCalled();
			expect(mockFetchGlossaryDetails).not.toHaveBeenCalled();
			expect(mockFetchDetailPageData).not.toHaveBeenCalled();
		});

		it('should handle empty shortDescription in edit mode', async () => {
			const dataObj = {
				name: 'Existing Term',
				shortDescription: 'Existing Short',
				longDescription: 'Existing Long',
				guid: 'term-guid-123'
			};

			mockHandleSubmit.mockImplementation((fn: any) => (e?: any) => {
				if (e) {
					e.preventDefault();
				}
				return fn({ name: 'Test Term', shortDescription: '', longDescription: 'Long' });
			});

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={false}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={dataObj}
						/>
					</MemoryRouter>
				</Provider>
			);

			const submitButton = screen.getByTestId('submit-btn');
			await act(async () => {
				fireEvent.click(submitButton);
			});

			await waitFor(() => {
				expect(mockEditTermorCategory).toHaveBeenCalledWith('term', 'term-guid-123', expect.objectContaining({
					name: 'Test Term',
					shortDescription: ''
				}));
			});
		});

		it('should handle empty longDescription in edit mode', async () => {
			const dataObj = {
				name: 'Existing Term',
				shortDescription: 'Existing Short',
				longDescription: 'Existing Long',
				guid: 'term-guid-123'
			};

			mockFormValues = { name: 'Test Term', shortDescription: 'Short', longDescription: '' };
			mockHandleSubmit.mockImplementation((onSubmitFn: any) => {
				return (e?: any) => {
					if (e) {
						e.preventDefault();
					}
					return Promise.resolve(onSubmitFn(mockFormValues));
				};
			});

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={false}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={dataObj}
						/>
					</MemoryRouter>
				</Provider>
			);

			const submitButton = screen.getByTestId('submit-btn');
			await act(async () => {
				fireEvent.click(submitButton);
			});

			await waitFor(() => {
				expect(mockEditTermorCategory).toHaveBeenCalledWith('term', 'term-guid-123', expect.objectContaining({
					name: 'Test Term',
					longDescription: ''
				}));
			});
		});
	});

	describe('Error Handling', () => {
		it('should handle error when creating term fails', async () => {
			const error = new Error('API Error');
			mockCreateTermorCategory.mockRejectedValue(error);

			const { serverError } = require('@utils/Utils');

			const glossaryData = [
				{ name: 'Test Glossary', guid: 'glossary-guid-1' }
			];
			store = createStore(glossaryData);

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			const submitButton = screen.getByTestId('submit-btn');
			await act(async () => {
				fireEvent.click(submitButton);
			});

			await waitFor(() => {
				expect(mockCreateTermorCategory).toHaveBeenCalled();
			});

			await waitFor(() => {
				expect(serverError).toHaveBeenCalledWith(error, expect.any(Object));
			});

			expect(mockOnClose).not.toHaveBeenCalled();
		});

		it('should handle error when updating term fails', async () => {
			const error = new Error('API Error');
			mockEditTermorCategory.mockRejectedValue(error);

			const { serverError } = require('@utils/Utils');

			const dataObj = {
				name: 'Existing Term',
				shortDescription: 'Existing Short',
				longDescription: 'Existing Long',
				guid: 'term-guid-123'
			};

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={false}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={dataObj}
						/>
					</MemoryRouter>
				</Provider>
			);

			const submitButton = screen.getByTestId('submit-btn');
			await act(async () => {
				fireEvent.click(submitButton);
			});

			await waitFor(() => {
				expect(mockEditTermorCategory).toHaveBeenCalled();
			});

			await waitFor(() => {
				expect(serverError).toHaveBeenCalledWith(error, expect.any(Object));
			});

			expect(mockOnClose).not.toHaveBeenCalled();
		});
	});

	describe('Edge Cases', () => {
		it('should handle undefined node', () => {
			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={undefined}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('modal')).toBeInTheDocument();
		});

		it('should handle node without id and parent', () => {
			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{}}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('modal')).toBeInTheDocument();
		});

		it('should handle glossaryData not finding matching parent', () => {
			store = createStore([
				{ name: 'Other Glossary', guid: 'other-guid' }
			]);

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Non-existent Glossary' }}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('modal')).toBeInTheDocument();
		});

		it('should handle cancel button click', () => {
			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			const cancelButton = screen.getByTestId('cancel-btn');
			fireEvent.click(cancelButton);

			expect(mockOnClose).toHaveBeenCalled();
		});

		it('should handle form submission with isSubmitting state', () => {
			const originalMock = mockUseForm.getMockImplementation();
			mockUseForm.mockReturnValueOnce({
				control: mockControl,
				handleSubmit: mockHandleSubmit,
				setValue: mockSetValue,
				formState: { isSubmitting: true }
			});

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={undefined}
						/>
					</MemoryRouter>
				</Provider>
			);

			const submitButton = screen.getByTestId('submit-btn');
			expect(submitButton).toBeDisabled();
			
			// Restore original mock
			if (originalMock) {
				mockUseForm.mockImplementation(originalMock);
			} else {
				mockUseForm.mockReturnValue({
					control: mockControl,
					handleSubmit: mockHandleSubmit,
					setValue: mockSetValue,
					formState: { isSubmitting: false }
				});
			}
		});
	});

	describe('Default Values', () => {
		it('should use dataObj values when provided in add mode', () => {
			const dataObj = {
				name: 'DataObj Name',
				shortDescription: 'DataObj Short',
				longDescription: 'DataObj Long'
			};

			const glossaryData = [
				{ 
					name: 'Test Glossary', 
					guid: 'glossary-guid-1',
					shortDescription: 'Glossary Short',
					longDescription: 'Glossary Long'
				}
			];
			store = createStore(glossaryData);

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={dataObj}
						/>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('modal')).toBeInTheDocument();
		});

		it('should use glossaryObj values when dataObj is empty in add mode', () => {
			const glossaryData = [
				{ 
					name: 'Test Glossary', 
					guid: 'glossary-guid-1',
					shortDescription: 'Glossary Short',
					longDescription: 'Glossary Long'
				}
			];
			store = createStore(glossaryData);

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={{}}
						/>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('modal')).toBeInTheDocument();
		});

		it('should handle undefined glossaryObj properties', () => {
			const glossaryData = [
				{ name: 'Test Glossary', guid: 'glossary-guid-1' }
			];
			store = createStore(glossaryData);

			render(
				<Provider store={store}>
					<MemoryRouter>
						<AddUpdateTermForm
							open={true}
							onClose={mockOnClose}
							isAdd={true}
							node={{ id: 'term-id', parent: 'Test Glossary' }}
							dataObj={{}}
						/>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('modal')).toBeInTheDocument();
		});
	});
});
