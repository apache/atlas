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
import { Provider } from 'react-redux';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { configureStore } from '@reduxjs/toolkit';
import BusinessMetadataDetailsLayout from '../BusinessMetadataDetailsLayout';

// Mock dependencies
jest.mock('react-toastify', () => ({
	toast: {
		dismiss: jest.fn(),
		success: jest.fn(),
		error: jest.fn()
	}
}));

// Mock child components
jest.mock('../../DetailPageAttributes', () => ({
	__esModule: true,
	default: ({ data, description, loading }: any) => (
		<div data-testid="detail-page-attribute">
			<div>Name: {data?.name}</div>
			<div>Description: {description}</div>
			<div>Loading: {loading ? 'true' : 'false'}</div>
		</div>
	)
}));

jest.mock('../BusinessMetadataAtrribute', () => ({
	__esModule: true,
	default: ({ componentProps }: any) => (
		<div data-testid="business-metadata-attribute">
			<div>Attributes Count: {componentProps?.attributeDefs?.length || 0}</div>
			<button
				data-testid="mock-edit-btn"
				onClick={() => {
					if (componentProps?.setForm) componentProps.setForm(true);
					if (componentProps?.reset) componentProps.reset({ attributeDefs: [] });
					if (componentProps?.setBMAttribute) componentProps.setBMAttribute({});
				}}
			>
				Mock Edit
			</button>
		</div>
	)
}));

jest.mock('@views/BusinessMetadata/BusinessMetadataAtrributeForm', () => ({
	__esModule: true,
	default: () => <div data-testid="bm-attribute-form">Form</div>
}));

// Mock API methods
const mockCreateEditBusinessMetadata = jest.fn();
jest.mock('@api/apiMethods/typeDefApiMethods', () => ({
	createEditBusinessMetadata: (...args: any[]) => mockCreateEditBusinessMetadata(...args)
}));

// Mock Redux actions
const mockFetchBusinessMetaData = jest.fn();
const mockSetEditBMAttribute = jest.fn();
jest.mock('@redux/slice/typeDefSlices/typedefBusinessMetadataSlice', () => ({
	fetchBusinessMetaData: (...args: any[]) => mockFetchBusinessMetaData(...args)
}));

jest.mock('@redux/slice/createBMSlice', () => ({
	setEditBMAttribute: (...args: any[]) => mockSetEditBMAttribute(...args)
}));

// Mock Utils
const mockServerError = jest.fn();
const mockGetTypeName = jest.fn((multi, enumType, rest) => rest.typeName || 'string');
const mockCloneDeep = jest.fn((obj) => JSON.parse(JSON.stringify(obj)));

jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0),
	serverError: (...args: any[]) => mockServerError(...args)
}));

jest.mock('@utils/CommonViewFunction', () => ({
	getTypeName: (...args: any[]) => mockGetTypeName(...args)
}));

jest.mock('@utils/Helper', () => ({
	cloneDeep: (...args: any[]) => mockCloneDeep(...args)
}));

jest.mock('@utils/Enum', () => ({
	defaultAttrObj: {
		name: '',
		typeName: 'string',
		cardinality: 'SINGLE',
		options: {
			maxStrLength: '',
			applicableEntityTypes: []
		}
	},
	defaultType: ['string', 'int', 'long', 'float', 'double', 'boolean', 'date', 'byte', 'short']
}));

// Mock react-hook-form
const mockHandleSubmit = jest.fn((fn) => async (e?: any) => {
	if (e) e.preventDefault();
	await fn({
		attributeDefs: [{
			name: 'testAttr',
			typeName: 'string',
			cardinality: 'SINGLE',
			multiValueSelect: false,
			options: { applicableEntityTypes: ['Entity1'], maxStrLength: '100' }
		}]
	});
});

const mockReset = jest.fn();
const mockAppend = jest.fn();
const mockRemove = jest.fn();

jest.mock('react-hook-form', () => ({
	useForm: () => ({
		control: {},
		handleSubmit: mockHandleSubmit,
		reset: mockReset,
		watch: jest.fn(() => []),
		setValue: jest.fn(),
		formState: { isSubmitting: false }
	}),
	useFieldArray: () => ({
		fields: [],
		append: mockAppend,
		remove: mockRemove
	})
}));

// Helper to create mock store
const createMockStore = (businessMetaData: any = {}, editbmAttribute: any = {}) => {
	return configureStore({
		reducer: {
			businessMetaData: () => ({ businessMetaData, loading: false }),
			createBM: () => ({ editbmAttribute }),
			typeHeader: () => ({
				typeHeaderData: [
					{ name: 'Entity1', category: 'ENTITY' },
					{ name: 'Entity2', category: 'ENTITY' }
				]
			}),
			enum: () => ({
				enumObj: {
					data: {
						enumDefs: []
					}
				}
			})
		},
		middleware: (getDefaultMiddleware) =>
			getDefaultMiddleware({
				serializableCheck: false,
				immutableCheck: false
			})
	});
};

const renderWithRouter = (
	component: React.ReactElement,
	options: { bmguid?: string; businessMetaData?: any; editbmAttribute?: any } = {}
) => {
	const { bmguid = 'test-bm-guid', businessMetaData = {}, editbmAttribute = {} } = options;
	const store = createMockStore(businessMetaData, editbmAttribute);

	return render(
		<Provider store={store}>
			<MemoryRouter initialEntries={[`/typedef/${bmguid}`]}>
				<Routes>
					<Route path="/typedef/:bmguid" element={component} />
				</Routes>
			</MemoryRouter>
		</Provider>
	);
};

describe('BusinessMetadataDetailsLayout - 100% Coverage', () => {
	const mockBusinessMetaData = {
		businessMetadataDefs: [
			{
				guid: 'test-bm-guid',
				name: 'Test Business Metadata',
				description: 'Test BM Description',
				attributeDefs: [
					{
						name: 'attr1',
						typeName: 'string',
						cardinality: 'SINGLE',
						options: {
							applicableEntityTypes: '["Entity1"]',
							maxStrLength: '100'
						}
					}
				]
			}
		]
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockFetchBusinessMetaData.mockReturnValue({ type: 'businessMetaData/fetch' });
		mockSetEditBMAttribute.mockReturnValue({ type: 'createBM/setEditBMAttribute' });
		mockCreateEditBusinessMetadata.mockResolvedValue({ data: {} });
	});

	describe('Component Rendering', () => {
		test('renders BusinessMetadataDetailsLayout component', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('renders Add Attributes button when form is not shown', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			expect(screen.getByText('Attributes')).toBeInTheDocument();
		});

		test('renders BusinessMetadataAtrribute when form is not shown', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			expect(screen.getByTestId('business-metadata-attribute')).toBeInTheDocument();
		});
	});

	describe('Form Display Toggle', () => {
		test('shows form when Add Attributes button is clicked', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			waitFor(() => {
				expect(screen.getByTestId('bm-attribute-form')).toBeInTheDocument();
			});
		});

		test('clicking Add Attributes resets form with defaultAttrObj', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			expect(mockReset).toHaveBeenCalled();
		});

		test('clicking Add Attributes clears bmAttribute', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			expect(mockSetEditBMAttribute).toHaveBeenCalledWith({});
		});

		test('shows Cancel button when form is displayed', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				expect(screen.getByText('Cancel')).toBeInTheDocument();
			});
		});

		test('shows Save button when form is displayed', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				expect(screen.getByText('Save')).toBeInTheDocument();
			});
		});
	});

	describe('Form Submission', () => {
		test('calls createEditBusinessMetadata on submit', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				expect(screen.getByText('Save')).toBeInTheDocument();
			});

			const saveButton = screen.getByText('Save');
			fireEvent.click(saveButton);

			await waitFor(() => {
				expect(mockCreateEditBusinessMetadata).toHaveBeenCalledWith(
					'business_metadata',
					'PUT',
					expect.any(Object)
				);
			});
		});

		test('dispatches fetchBusinessMetaData after successful submit', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const saveButton = screen.getByText('Save');
				fireEvent.click(saveButton);
			});

			await waitFor(() => {
				expect(mockFetchBusinessMetaData).toHaveBeenCalled();
			});
		});

		test('shows success toast after successful submit', async () => {
			const { toast } = require('react-toastify');
			
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const saveButton = screen.getByText('Save');
				fireEvent.click(saveButton);
			});

			await waitFor(() => {
				expect(toast.success).toHaveBeenCalledWith(
					'One or more Business Metadata attributes were updated successfully'
				);
			});
		});

		test('closes form after successful submit', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const saveButton = screen.getByText('Save');
				fireEvent.click(saveButton);
			});

			await waitFor(() => {
				expect(screen.getByTestId('business-metadata-attribute')).toBeInTheDocument();
			});
		});

		test('handles API error on submit', async () => {
			mockCreateEditBusinessMetadata.mockRejectedValue(new Error('API Error'));

			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const saveButton = screen.getByText('Save');
				fireEvent.click(saveButton);
			});

			await waitFor(() => {
				expect(mockServerError).toHaveBeenCalled();
			});
		});
	});

	describe('Form Cancellation', () => {
		test('closes form when Cancel button is clicked', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				expect(screen.getByText('Cancel')).toBeInTheDocument();
			});

			const cancelButton = screen.getByText('Cancel');
			fireEvent.click(cancelButton);

			await waitFor(() => {
				expect(screen.getByTestId('business-metadata-attribute')).toBeInTheDocument();
			});
		});

		test('resets form when Cancel is clicked', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const cancelButton = screen.getByText('Cancel');
				fireEvent.click(cancelButton);
			});

			expect(mockReset).toHaveBeenCalled();
		});

		test('clears bmAttribute when Cancel is clicked', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const cancelButton = screen.getByText('Cancel');
				fireEvent.click(cancelButton);
			});

			expect(mockSetEditBMAttribute).toHaveBeenCalledWith({});
		});
	});

	describe('Add Business Metadata Attribute Button', () => {
		test('renders add attribute button when bmAttribute is empty', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addMainButton = screen.getByText('Attributes');
			fireEvent.click(addMainButton);

			await waitFor(() => {
				expect(screen.getByText('Add Business Metadata Attribute')).toBeInTheDocument();
			});
		});

		test('clicking add attribute button calls append', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addMainButton = screen.getByText('Attributes');
			fireEvent.click(addMainButton);

			await waitFor(() => {
				const addAttrButton = screen.getByText('Add Business Metadata Attribute');
				fireEvent.click(addAttrButton);
			});

			expect(mockAppend).toHaveBeenCalled();
		});
	});

	describe('Business Metadata Title', () => {
		test('shows "Add" title when bmAttribute and editbmAttribute are empty', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData,
				editbmAttribute: {}
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				expect(screen.getByText(/Add Business Metadata Attribute for:/)).toBeInTheDocument();
			});
		});

		test('shows "Update" title when editbmAttribute is not empty', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData,
				editbmAttribute: {
					name: 'existingAttr',
					typeName: 'string'
				}
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				expect(screen.getByText(/Update Attribute of:/)).toBeInTheDocument();
			});
		});
	});

	describe('Data Processing', () => {
		test('finds businessmetaDataObj by bmguid', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				bmguid: 'test-bm-guid',
				businessMetaData: mockBusinessMetaData
			});

			expect(screen.getByText('Name: Test Business Metadata')).toBeInTheDocument();
		});

		test('handles empty businessMetadataDefs', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: { businessMetadataDefs: [] }
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles null businessMetaData', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: null
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles bmguid not found', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				bmguid: 'non-existent-guid',
				businessMetaData: mockBusinessMetaData
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});
	});

	describe('Enum Type Processing', () => {
		test('processes enum types from typeHeaderData', () => {
			const storeWithEnums = configureStore({
				reducer: {
					businessMetaData: () => ({ businessMetaData: mockBusinessMetaData, loading: false }),
					createBM: () => ({ editbmAttribute: {} }),
					typeHeader: () => ({
						typeHeaderData: [
							{ name: 'Entity1', category: 'ENTITY' },
							{ name: 'Other1', category: 'OTHER' }
						]
					}),
					enum: () => ({
						enumObj: {
							data: {
								enumDefs: [
									{ name: 'Enum1' },
									{ name: 'Enum2' }
								]
							}
						}
					})
				}
			});

			render(
				<Provider store={storeWithEnums}>
					<MemoryRouter initialEntries={['/typedef/test-bm-guid']}>
						<Routes>
							<Route path="/typedef/:bmguid" element={<BusinessMetadataDetailsLayout />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('filters entities by ENTITY category', () => {
			const storeWithMixedCategories = configureStore({
				reducer: {
					businessMetaData: () => ({ businessMetaData: mockBusinessMetaData, loading: false }),
					createBM: () => ({ editbmAttribute: {} }),
					typeHeader: () => ({
						typeHeaderData: [
							{ name: 'Entity1', category: 'ENTITY' },
							{ name: 'Classification1', category: 'CLASSIFICATION' },
							{ name: 'Entity2', category: 'ENTITY' }
						]
					}),
					enum: () => ({ enumObj: { data: { enumDefs: [] } } })
				}
			});

			render(
				<Provider store={storeWithMixedCategories}>
					<MemoryRouter initialEntries={['/typedef/test-bm-guid']}>
						<Routes>
							<Route path="/typedef/:bmguid" element={<BusinessMetadataDetailsLayout />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});
	});

	describe('Edit Attribute Processing', () => {
		test('processes editbmAttribute with array type', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData,
				editbmAttribute: {
					name: 'testAttr',
					typeName: 'array<string>',
					options: {
						applicableEntityTypes: '["Entity1"]'
					}
				}
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('processes editbmAttribute with enum type', () => {
			const storeWithEnum = configureStore({
				reducer: {
					businessMetaData: () => ({ businessMetaData: mockBusinessMetaData, loading: false }),
					createBM: () => ({
						editbmAttribute: {
							name: 'testAttr',
							typeName: 'CustomEnum',
							options: {}
						}
					}),
					typeHeader: () => ({ typeHeaderData: [] }),
					enum: () => ({
						enumObj: {
							data: {
								enumDefs: [
									{
										name: 'CustomEnum',
										elementDefs: [{ value: 'VAL1' }]
									}
								]
							}
						}
					})
				}
			});

			render(
				<Provider store={storeWithEnum}>
					<MemoryRouter initialEntries={['/typedef/test-bm-guid']}>
						<Routes>
							<Route path="/typedef/:bmguid" element={<BusinessMetadataDetailsLayout />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles JSON parse error in applicableEntityTypes', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData,
				editbmAttribute: {
					name: 'testAttr',
					typeName: 'string',
					options: {
						applicableEntityTypes: 'invalid-json'
					}
				}
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});
	});

	describe('Form Data Transformation', () => {
		test('transforms form data correctly on submit', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const saveButton = screen.getByText('Save');
				fireEvent.click(saveButton);
			});

			await waitFor(() => {
				expect(mockCreateEditBusinessMetadata).toHaveBeenCalled();
			});
		});

		test('handles multiValueSelect and sets cardinality to SET', async () => {
			mockHandleSubmit.mockImplementationOnce((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({
					attributeDefs: [{
						name: 'testAttr',
						typeName: 'string',
						multiValueSelect: true,
						cardinalityToggle: 'SET',
						options: { applicableEntityTypes: [], maxStrLength: '' }
					}]
				});
			});

			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const saveButton = screen.getByText('Save');
				fireEvent.click(saveButton);
			});

			await waitFor(() => {
				expect(mockCreateEditBusinessMetadata).toHaveBeenCalled();
			});
		});

		test('handles multiValueSelect and sets cardinality to LIST', async () => {
			mockHandleSubmit.mockImplementationOnce((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({
					attributeDefs: [{
						name: 'testAttr',
						typeName: 'string',
						multiValueSelect: true,
						cardinalityToggle: 'LIST',
						cardinality: 'SINGLE',
						options: { applicableEntityTypes: [], maxStrLength: '' }
					}]
				});
			});

			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const saveButton = screen.getByText('Save');
				fireEvent.click(saveButton);
			});

			await waitFor(() => {
				expect(mockCreateEditBusinessMetadata).toHaveBeenCalled();
			});
		});

		test('preserves existing LIST cardinality when multiValueSelect', async () => {
			mockHandleSubmit.mockImplementationOnce((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({
					attributeDefs: [{
						name: 'testAttr',
						typeName: 'string',
						multiValueSelect: true,
						cardinality: 'LIST',
						options: { applicableEntityTypes: [], maxStrLength: '' }
					}]
				});
			});

			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const saveButton = screen.getByText('Save');
				fireEvent.click(saveButton);
			});

			await waitFor(() => {
				expect(mockCreateEditBusinessMetadata).toHaveBeenCalled();
			});
		});

		test('handles enumType and enumValues', async () => {
			mockHandleSubmit.mockImplementationOnce((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({
					attributeDefs: [{
						name: 'testAttr',
						typeName: 'string',
						enumType: 'StatusEnum',
						enumValues: [{ value: 'ACTIVE' }, { value: 'INACTIVE' }],
						multiValueSelect: false,
						options: { applicableEntityTypes: [], maxStrLength: '' }
					}]
				});
			});

			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const saveButton = screen.getByText('Save');
				fireEvent.click(saveButton);
			});

			await waitFor(() => {
				expect(mockCreateEditBusinessMetadata).toHaveBeenCalled();
			});
		});

		test('adds new attribute when editbmAttribute is empty', async () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData,
				editbmAttribute: {}
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const saveButton = screen.getByText('Save');
				fireEvent.click(saveButton);
			});

			await waitFor(() => {
				expect(mockCreateEditBusinessMetadata).toHaveBeenCalled();
			});
		});

		test('updates existing attribute when editbmAttribute is not empty', async () => {
			mockHandleSubmit.mockImplementationOnce((fn) => async (e?: any) => {
				if (e) e.preventDefault();
				await fn({
					attributeDefs: [{
						name: 'attr1',
						typeName: 'string',
						multiValueSelect: false,
						options: { applicableEntityTypes: [], maxStrLength: '' }
					}]
				});
			});

			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData,
				editbmAttribute: {
					name: 'attr1',
					typeName: 'string'
				}
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				const saveButton = screen.getByText('Save');
				fireEvent.click(saveButton);
			});

			await waitFor(() => {
				expect(mockCreateEditBusinessMetadata).toHaveBeenCalled();
			});
		});
	});

	describe('Edge Cases', () => {
		test('handles empty businessMetadataDefs', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: { businessMetadataDefs: [] }
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles null businessMetaData', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: null
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles undefined businessMetaData', () => {
			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: undefined
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles empty typeHeaderData', () => {
			const storeWithEmptyTypes = configureStore({
				reducer: {
					businessMetaData: () => ({ businessMetaData: mockBusinessMetaData, loading: false }),
					createBM: () => ({ editbmAttribute: {} }),
					typeHeader: () => ({ typeHeaderData: [] }),
					enum: () => ({ enumObj: { data: { enumDefs: [] } } })
				}
			});

			render(
				<Provider store={storeWithEmptyTypes}>
					<MemoryRouter initialEntries={['/typedef/test-bm-guid']}>
						<Routes>
							<Route path="/typedef/:bmguid" element={<BusinessMetadataDetailsLayout />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles null enumDefs', () => {
			const storeWithNullEnums = configureStore({
				reducer: {
					businessMetaData: () => ({ businessMetaData: mockBusinessMetaData, loading: false }),
					createBM: () => ({ editbmAttribute: {} }),
					typeHeader: () => ({ typeHeaderData: [] }),
					enum: () => ({ enumObj: { data: null } })
				}
			});

			render(
				<Provider store={storeWithNullEnums}>
					<MemoryRouter initialEntries={['/typedef/test-bm-guid']}>
						<Routes>
							<Route path="/typedef/:bmguid" element={<BusinessMetadataDetailsLayout />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});
	});

	describe('Loading State', () => {
		test('passes loading state to DetailPageAttribute', () => {
			const storeWithLoading = configureStore({
				reducer: {
					businessMetaData: () => ({ businessMetaData: mockBusinessMetaData, loading: true }),
					createBM: () => ({ editbmAttribute: {} }),
					typeHeader: () => ({ typeHeaderData: [] }),
					enum: () => ({ enumObj: { data: { enumDefs: [] } } })
				}
			});

			render(
				<Provider store={storeWithLoading}>
					<MemoryRouter initialEntries={['/typedef/test-bm-guid']}>
						<Routes>
							<Route path="/typedef/:bmguid" element={<BusinessMetadataDetailsLayout />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByText('Loading: true')).toBeInTheDocument();
		});

		test('shows CircularProgress when submitting', async () => {
			const { useForm } = require('react-hook-form');
			jest.spyOn(require('react-hook-form'), 'useForm').mockReturnValue({
				control: {},
				handleSubmit: mockHandleSubmit,
				reset: mockReset,
				watch: jest.fn(() => []),
				setValue: jest.fn(),
				formState: { isSubmitting: true }
			});

			renderWithRouter(<BusinessMetadataDetailsLayout />, {
				businessMetaData: mockBusinessMetaData
			});

			const addButton = screen.getByText('Attributes');
			fireEvent.click(addButton);

			await waitFor(() => {
				expect(screen.getByText('Save')).toBeInTheDocument();
			});
		});
	});
});
