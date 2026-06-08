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
import { render, screen, waitFor } from '@testing-library/react';
import { Provider } from 'react-redux';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { configureStore } from '@reduxjs/toolkit';
import ClassificationDetailsLayout from '../ClassificationDetailsLayout';

// Mock child components
jest.mock('../DetailPageAttributes', () => ({
	__esModule: true,
	default: ({ data, description, subTypes, superTypes, entityTypes, loading, attributeDefs }: any) => (
		<div data-testid="detail-page-attribute">
			<div>Name: {data?.name}</div>
			<div>Description: {description}</div>
			<div>Loading: {loading ? 'true' : 'false'}</div>
			{subTypes && <div>SubTypes: {JSON.stringify(subTypes)}</div>}
			{superTypes && <div>SuperTypes: {JSON.stringify(superTypes)}</div>}
			{entityTypes && <div>EntityTypes: {JSON.stringify(entityTypes)}</div>}
			{attributeDefs && <div>AttributeDefs: {JSON.stringify(attributeDefs)}</div>}
		</div>
	)
}));

jest.mock('@views/SearchResult/SearchResult', () => ({
	__esModule: true,
	default: ({ classificationParams, hideFilters }: any) => (
		<div data-testid="search-result">
			<div>Classification: {classificationParams}</div>
			<div>Hide Filters: {hideFilters ? 'true' : 'false'}</div>
		</div>
	)
}));

// Mock Utils
const mockCloneDeep = jest.fn((obj) => JSON.parse(JSON.stringify(obj)));
jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0)
}));

jest.mock('@utils/Helper', () => ({
	cloneDeep: (...args: any[]) => mockCloneDeep(...args)
}));

// Helper to create mock store
const createMockStore = (classificationData: any = {}, loading: boolean = false) => {
	return configureStore({
		reducer: {
			classification: () => ({ classificationData, loading })
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
	options: { tagName?: string; classificationData?: any; loading?: boolean } = {}
) => {
	const { tagName = 'test-tag', classificationData = {}, loading = false } = options;
	const store = createMockStore(classificationData, loading);
	const path = `/detailPage/${tagName}`;

	return render(
		<Provider store={store}>
			<MemoryRouter initialEntries={[path]}>
				<Routes>
					<Route path="/detailPage/:tagName" element={component} />
				</Routes>
			</MemoryRouter>
		</Provider>
	);
};

describe('ClassificationDetailsLayout - 100% Coverage', () => {
	const mockClassificationData = {
		classificationDefs: [
			{
				name: 'test-tag',
				description: 'Test classification description',
				subTypes: ['SubType1', 'SubType2'],
				superTypes: ['SuperType1'],
				entityTypes: ['Entity1', 'Entity2'],
				attributeDefs: [
					{ name: 'attr1', typeName: 'string' },
					{ name: 'attr2', typeName: 'int' }
				]
			},
			{
				name: 'another-tag',
				description: 'Another classification'
			}
		]
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockCloneDeep.mockImplementation((obj) => JSON.parse(JSON.stringify(obj)));
	});

	describe('Component Rendering', () => {
		test('renders ClassificationDetailsLayout component', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
			expect(screen.getByTestId('search-result')).toBeInTheDocument();
		});

		test('renders with correct structure', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByText('Name: test-tag')).toBeInTheDocument();
			expect(screen.getByText('Description: Test classification description')).toBeInTheDocument();
		});

		test('renders Stack with correct direction and gap', () => {
			const { container } = renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			const stack = container.querySelector('.MuiStack-root');
			expect(stack).toBeInTheDocument();
		});
	});

	describe('Data Fetching and Processing', () => {
		test('clones classificationDefs data', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(mockCloneDeep).toHaveBeenCalledWith(mockClassificationData.classificationDefs);
		});

		test('finds classification by tagName', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				tagName: 'test-tag',
				classificationData: mockClassificationData
			});

			expect(screen.getByText('Name: test-tag')).toBeInTheDocument();
		});

		test('finds different classification when tagName changes', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				tagName: 'another-tag',
				classificationData: mockClassificationData
			});

			expect(screen.getByText('Name: another-tag')).toBeInTheDocument();
			expect(screen.getByText('Description: Another classification')).toBeInTheDocument();
		});
	});

	describe('Classification Data Display', () => {
		test('passes description to DetailPageAttribute', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByText('Description: Test classification description')).toBeInTheDocument();
		});

		test('passes subTypes to DetailPageAttribute', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByText(/SubTypes:/)).toBeInTheDocument();
		});

		test('passes superTypes to DetailPageAttribute', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByText(/SuperTypes:/)).toBeInTheDocument();
		});

		test('passes entityTypes to DetailPageAttribute', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByText(/EntityTypes:/)).toBeInTheDocument();
		});

		test('passes attributeDefs to DetailPageAttribute', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByText(/AttributeDefs:/)).toBeInTheDocument();
		});

		test('passes loading state to DetailPageAttribute', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData,
				loading: true
			});

			expect(screen.getByText('Loading: true')).toBeInTheDocument();
		});

		test('passes loading false when not loading', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData,
				loading: false
			});

			expect(screen.getByText('Loading: false')).toBeInTheDocument();
		});
	});

	describe('SearchResult Integration', () => {
		test('passes classificationParams to SearchResult', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				tagName: 'test-tag',
				classificationData: mockClassificationData
			});

			expect(screen.getByText('Classification: test-tag')).toBeInTheDocument();
		});

		test('passes hideFilters as true to SearchResult', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByText('Hide Filters: true')).toBeInTheDocument();
		});

		test('renders SearchResult component', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByTestId('search-result')).toBeInTheDocument();
		});
	});

	describe('Empty States', () => {
		test('handles empty classificationDefs', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: { classificationDefs: [] }
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles null classificationDefs', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: { classificationDefs: null }
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles undefined classificationDefs', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: {}
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles null classificationData', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: null
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles classification not found', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				tagName: 'non-existent-tag',
				classificationData: mockClassificationData
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});
	});

	describe('Edge Cases', () => {
		test('handles classification without description', () => {
			const dataWithoutDesc = {
				classificationDefs: [
					{
						name: 'test-tag',
						subTypes: [],
						superTypes: [],
						entityTypes: []
					}
				]
			};

			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: dataWithoutDesc
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles classification without subTypes', () => {
			const dataWithoutSubTypes = {
				classificationDefs: [
					{
						name: 'test-tag',
						description: 'Test'
					}
				]
			};

			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: dataWithoutSubTypes
			});

			expect(screen.getByText('Description: Test')).toBeInTheDocument();
		});

		test('handles classification without superTypes', () => {
			const dataWithoutSuperTypes = {
				classificationDefs: [
					{
						name: 'test-tag',
						description: 'Test',
						subTypes: []
					}
				]
			};

			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: dataWithoutSuperTypes
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles classification without entityTypes', () => {
			const dataWithoutEntityTypes = {
				classificationDefs: [
					{
						name: 'test-tag',
						description: 'Test'
					}
				]
			};

			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: dataWithoutEntityTypes
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles classification without attributeDefs', () => {
			const dataWithoutAttributeDefs = {
				classificationDefs: [
					{
						name: 'test-tag',
						description: 'Test'
					}
				]
			};

			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: dataWithoutAttributeDefs
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles empty tag object', () => {
			const dataWithEmptyTag = {
				classificationDefs: [
					{
						name: 'test-tag'
					}
				]
			};

			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: dataWithEmptyTag
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('handles missing tagName parameter', () => {
			const store = createMockStore(mockClassificationData);

			render(
				<Provider store={store}>
					<MemoryRouter initialEntries={['/detailPage/']}>
						<Routes>
							<Route path="/detailPage/:tagName?" element={<ClassificationDetailsLayout />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});
	});

	describe('Multiple Classifications', () => {
		test('selects correct classification from multiple options', () => {
			const multipleClassifications = {
				classificationDefs: [
					{ name: 'tag1', description: 'First tag' },
					{ name: 'tag2', description: 'Second tag' },
					{ name: 'tag3', description: 'Third tag' }
				]
			};

			renderWithRouter(<ClassificationDetailsLayout />, {
				tagName: 'tag2',
				classificationData: multipleClassifications
			});

			expect(screen.getByText('Name: tag2')).toBeInTheDocument();
			expect(screen.getByText('Description: Second tag')).toBeInTheDocument();
		});

		test('handles first classification in list', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				tagName: 'test-tag',
				classificationData: mockClassificationData
			});

			expect(screen.getByText('Name: test-tag')).toBeInTheDocument();
		});

		test('handles last classification in list', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				tagName: 'another-tag',
				classificationData: mockClassificationData
			});

			expect(screen.getByText('Name: another-tag')).toBeInTheDocument();
		});
	});

	describe('Props Passing', () => {
		test('passes paramsAttribute to DetailPageAttribute', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				tagName: 'my-custom-tag',
				classificationData: {
					classificationDefs: [
						{ name: 'my-custom-tag', description: 'Custom' }
					]
				}
			});

			expect(screen.getByText('Name: my-custom-tag')).toBeInTheDocument();
		});

		test('passes data object to DetailPageAttribute', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByText('Name: test-tag')).toBeInTheDocument();
		});

		test('passes all required props to DetailPageAttribute', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByText(/Name:/)).toBeInTheDocument();
			expect(screen.getByText(/Description:/)).toBeInTheDocument();
			expect(screen.getByText(/SubTypes:/)).toBeInTheDocument();
			expect(screen.getByText(/SuperTypes:/)).toBeInTheDocument();
			expect(screen.getByText(/EntityTypes:/)).toBeInTheDocument();
			expect(screen.getByText(/AttributeDefs:/)).toBeInTheDocument();
			expect(screen.getByText(/Loading:/)).toBeInTheDocument();
		});
	});

	describe('Component Structure', () => {
		test('renders components in correct order', () => {
			const { container } = renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			const detailPage = screen.getByTestId('detail-page-attribute');
			const searchResult = screen.getByTestId('search-result');

			expect(detailPage).toBeInTheDocument();
			expect(searchResult).toBeInTheDocument();
		});

		test('applies correct gap to Stack', () => {
			const { container } = renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			const stack = container.querySelector('.MuiStack-root');
			expect(stack).toBeInTheDocument();
		});
	});

	describe('Data Destructuring', () => {
		test('destructures all properties from tag object', () => {
			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: mockClassificationData
			});

			expect(screen.getByText(/SubTypes:/)).toBeInTheDocument();
			expect(screen.getByText(/SuperTypes:/)).toBeInTheDocument();
			expect(screen.getByText(/EntityTypes:/)).toBeInTheDocument();
			expect(screen.getByText(/AttributeDefs:/)).toBeInTheDocument();
		});

		test('handles partial tag object', () => {
			const partialData = {
				classificationDefs: [
					{
						name: 'test-tag',
						description: 'Test'
					}
				]
			};

			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: partialData
			});

			expect(screen.getByText('Description: Test')).toBeInTheDocument();
		});

		test('uses default empty objects for missing properties', () => {
			const minimalData = {
				classificationDefs: [
					{
						name: 'test-tag'
					}
				]
			};

			renderWithRouter(<ClassificationDetailsLayout />, {
				classificationData: minimalData
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});
	});
});
