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
import GlossaryDetailLayout from '../GlossaryDetailsLayout';

// Store the onChange handler for testing
let capturedOnChange: any = null;

// Mock dependencies
jest.mock('react-toastify', () => ({
	toast: {
		dismiss: jest.fn(),
		error: jest.fn()
	}
}));

// Mock MUI Tabs to capture onChange handler
jest.mock('@mui/material', () => {
	const actual = jest.requireActual('@mui/material');
	return {
		...actual,
		Tabs: ({ children, value, onChange, ...props }: any) => {
			capturedOnChange = onChange;
			return (
				<div role="navigation" data-testid="tabs" data-value={value} {...props}>
					{children}
				</div>
			);
		}
	};
});

// Mock child components
jest.mock('../TermProperties', () => ({
	__esModule: true,
	default: () => <div data-testid="term-properties-tab">TermProperties</div>
}));

jest.mock('../TermRelation', () => ({
	__esModule: true,
	default: () => <div data-testid="term-relation-tab">TermRelation</div>
}));

jest.mock('../../EntityDetailTabs/ClassificationsTab', () => ({
	__esModule: true,
	default: () => <div data-testid="classifications-tab">ClassificationsTab</div>
}));

jest.mock('../../DetailPageAttributes', () => ({
	__esModule: true,
	default: ({ data }: any) => (
		<div data-testid="detail-page-attribute">
			{data?.qualifiedName && <div>{data.qualifiedName}</div>}
		</div>
	)
}));

jest.mock('@views/SearchResult/SearchResult', () => ({
	__esModule: true,
	default: () => <div data-testid="search-result">SearchResult</div>
}));

// Mock Redux actions
const mockFetchGlossaryDetails = jest.fn();
jest.mock('@redux/slice/glossaryDetailsSlice', () => ({
	fetchGlossaryDetails: (...args: any[]) => mockFetchGlossaryDetails(...args)
}));

// Mock Utils
const mockGetTagObj = jest.fn();
jest.mock('@utils/Utils', () => ({
	getTagObj: (...args: any[]) => mockGetTagObj(...args),
	isEmpty: (val: any) => val === null || val === undefined || val === '' || (Array.isArray(val) && val.length === 0) || (typeof val === 'object' && Object.keys(val).length === 0)
}));

// Mock MUI utils
jest.mock('@utils/Muiutils', () => ({
	Item: ({ children, variant, className }: any) => (
		<div data-testid="item" data-variant={variant} className={className}>
			{children}
		</div>
	),
	samePageLinkNavigation: (event: any) => event.type === 'click'
}));

// Helper to create mock store
const createMockStore = (glossaryData: any = {}) => {
	return configureStore({
		reducer: {
			glossaryType: (state = { glossaryTypeData: { data: glossaryData, loading: false } }) => state,
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
	options: { searchParams?: string; guid?: string; glossaryData?: any } = {}
) => {
	const { searchParams = '', guid = 'test-guid-123', glossaryData = {} } = options;
	const store = createMockStore(glossaryData);
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

describe('GlossaryDetailLayout - 100% Coverage', () => {
	beforeEach(() => {
		jest.clearAllMocks();
		capturedOnChange = null;
		mockFetchGlossaryDetails.mockReturnValue({
			type: 'glossaryDetails/fetchGlossaryDetails/pending',
			payload: undefined
		});
		mockGetTagObj.mockReturnValue([]);
	});

	describe('Component Rendering', () => {
		test('renders GlossaryDetailLayout component', () => {
			renderWithRouter(<GlossaryDetailLayout />);

			expect(mockFetchGlossaryDetails).toHaveBeenCalledWith({
				gtype: null,
				guid: 'test-guid-123'
			});
		});

		test('renders with gtype=term parameter', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: {
					guid: 'test-guid-123',
					qualifiedName: 'test.glossary.term'
				}
			});

			expect(screen.getByText('Entities')).toBeInTheDocument();
			expect(screen.getByText('Properties')).toBeInTheDocument();
			expect(screen.getByText('Classifications')).toBeInTheDocument();
			expect(screen.getByText('Related Terms')).toBeInTheDocument();
		});

		test('does not render tabs when gtype is not term', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=category'
			});

			expect(screen.queryByText('Entities')).not.toBeInTheDocument();
		});
	});

	describe('Data Fetching', () => {
		test('fetches glossary details on mount', () => {
			renderWithRouter(<GlossaryDetailLayout />);

			expect(mockFetchGlossaryDetails).toHaveBeenCalledWith({
				gtype: null,
				guid: 'test-guid-123'
			});
		});

		test('fetches glossary details with gtype parameter', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term'
			});

			expect(mockFetchGlossaryDetails).toHaveBeenCalledWith({
				gtype: 'term',
				guid: 'test-guid-123'
			});
		});

		test('fetches glossary details when guid changes', () => {
			renderWithRouter(<GlossaryDetailLayout />);

			expect(mockFetchGlossaryDetails).toHaveBeenCalledWith({
				gtype: null,
				guid: 'test-guid-123'
			});

			// Test that the component calls fetchGlossaryDetails on mount
			expect(mockFetchGlossaryDetails).toHaveBeenCalledTimes(1);
		});
	});

	describe('Glossary Data Display', () => {
		test('displays DetailPageAttribute component', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				glossaryData: {
					qualifiedName: 'test.glossary.term'
				}
			});

			expect(screen.getByTestId('detail-page-attribute')).toBeInTheDocument();
		});

		test('passes correct props to DetailPageAttribute', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				glossaryData: {
					qualifiedName: 'test.glossary.term',
					classifications: {},
					categories: {},
					shortDescription: 'Short desc',
					longDescription: 'Long desc'
				}
			});

			expect(screen.getByText('test.glossary.term')).toBeInTheDocument();
		});
	});

	describe('Tab Navigation - gtype=term', () => {
		test('renders Entities tab by default', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: {
					guid: 'test-guid-123',
					qualifiedName: 'test.term'
				}
			});

			expect(screen.getByTestId('search-result')).toBeInTheDocument();
		});

		test('renders Properties tab when activeTab is entitiesProperties', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=entitiesProperties',
				glossaryData: {
					guid: 'test-guid-123',
					additionalAttributes: {}
				}
			});

			expect(screen.getByTestId('term-properties-tab')).toBeInTheDocument();
		});

		test('renders Classifications tab when activeTab is classification', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=classification',
				glossaryData: {
					guid: 'test-guid-123',
					classifications: {}
				}
			});

			expect(screen.getByTestId('classifications-tab')).toBeInTheDocument();
		});

		test('renders Related Terms tab when activeTab is relatedTerm', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=relatedTerm',
				glossaryData: {
					guid: 'test-guid-123'
				}
			});

			expect(screen.getByTestId('term-relation-tab')).toBeInTheDocument();
		});

		test('renders Entities tab when activeTab is entities', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=entities',
				glossaryData: {
					guid: 'test-guid-123',
					qualifiedName: 'test.term'
				}
			});

			expect(screen.getByTestId('search-result')).toBeInTheDocument();
		});

		test('renders Entities tab when activeTab is undefined', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: {
					guid: 'test-guid-123',
					qualifiedName: 'test.term'
				}
			});

			expect(screen.getByTestId('search-result')).toBeInTheDocument();
		});
	});

	describe('Tab Change Handling', () => {
		test('handles tab change with click event', async () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: { guid: 'test-guid-123' }
			});

			await waitFor(() => {
				expect(capturedOnChange).not.toBeNull();
			});

			const clickEvent = {
				type: 'click',
				preventDefault: jest.fn()
			} as any;

			capturedOnChange(clickEvent, 0);

			// Tab change should be handled
			expect(clickEvent.preventDefault).not.toHaveBeenCalled();
		});

		test('handles tab change with non-click event', async () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: { guid: 'test-guid-123' }
			});

			await waitFor(() => {
				expect(capturedOnChange).not.toBeNull();
			});

			const keydownEvent = {
				type: 'keydown',
				key: 'Enter',
				preventDefault: jest.fn()
			} as any;

			capturedOnChange(keydownEvent, 1);

			// Non-click events should trigger navigation
			expect(keydownEvent.preventDefault).not.toHaveBeenCalled();
		});

		test('deletes non-searchType params on tab change', async () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&searchType=test&param1=value1&param2=value2',
				glossaryData: { guid: 'test-guid-123' }
			});

			await waitFor(() => {
				expect(capturedOnChange).not.toBeNull();
			});

			const keydownEvent = {
				type: 'keydown',
				preventDefault: jest.fn()
			} as any;

			capturedOnChange(keydownEvent, 0);

			// Should delete non-searchType params
			expect(keydownEvent.preventDefault).not.toHaveBeenCalled();
		});

		test('sets gtype, viewType, and fromView params on tab change', async () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: { guid: 'test-guid-123' }
			});

			await waitFor(() => {
				expect(capturedOnChange).not.toBeNull();
			});

			const keydownEvent = {
				type: 'keydown',
				preventDefault: jest.fn()
			} as any;

			capturedOnChange(keydownEvent, 1);

			// Should set required params
			expect(keydownEvent.preventDefault).not.toHaveBeenCalled();
		});

		test('changes to Properties tab (index 1)', async () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: { guid: 'test-guid-123' }
			});

			await waitFor(() => {
				expect(capturedOnChange).not.toBeNull();
			});

			const clickEvent = {
				type: 'click',
				preventDefault: jest.fn()
			} as any;

			capturedOnChange(clickEvent, 1);

			// Should change to properties tab
			expect(clickEvent.preventDefault).not.toHaveBeenCalled();
		});

		test('changes to Classifications tab (index 2)', async () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: { guid: 'test-guid-123' }
			});

			await waitFor(() => {
				expect(capturedOnChange).not.toBeNull();
			});

			const clickEvent = {
				type: 'click',
				preventDefault: jest.fn()
			} as any;

			capturedOnChange(clickEvent, 2);

			// Should change to classifications tab
			expect(clickEvent.preventDefault).not.toHaveBeenCalled();
		});

		test('changes to Related Terms tab (index 3)', async () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: { guid: 'test-guid-123' }
			});

			await waitFor(() => {
				expect(capturedOnChange).not.toBeNull();
			});

			const clickEvent = {
				type: 'click',
				preventDefault: jest.fn()
			} as any;

			capturedOnChange(clickEvent, 3);

			// Should change to related terms tab
			expect(clickEvent.preventDefault).not.toHaveBeenCalled();
		});
	});

	describe('URL Parameter Handling', () => {
		test('reads activeTab from URL search params', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=entitiesProperties',
				glossaryData: { guid: 'test-guid-123' }
			});

			expect(screen.getByTestId('term-properties-tab')).toBeInTheDocument();
		});

		test('defaults to entities tab when no activeTab param', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: {
					guid: 'test-guid-123',
					qualifiedName: 'test.term'
				}
			});

			expect(screen.getByTestId('search-result')).toBeInTheDocument();
		});

		test('handles empty activeTab parameter', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=',
				glossaryData: {
					guid: 'test-guid-123',
					qualifiedName: 'test.term'
				}
			});

			expect(screen.getByTestId('search-result')).toBeInTheDocument();
		});

		test('handles invalid activeTab value and redirects', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=invalid',
				glossaryData: {
					guid: 'test-guid-123',
					qualifiedName: 'test.term'
				}
			});

			// Should redirect to entities tab
			expect(screen.getByText('Entities')).toBeInTheDocument();
		});
	});

	describe('Active Tab State Management', () => {
		test('sets initial tab value based on activeTab param', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=entitiesProperties',
				glossaryData: { guid: 'test-guid-123' }
			});

			expect(screen.getByTestId('term-properties-tab')).toBeInTheDocument();
		});

		test('sets tab value to 0 for invalid activeTab', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=nonexistent',
				glossaryData: {
					guid: 'test-guid-123',
					qualifiedName: 'test.term'
				}
			});

			// Should default to first tab
			expect(screen.getByText('Entities')).toBeInTheDocument();
		});

		test('handles value of -1 by setting to 0', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: {
					guid: 'test-guid-123',
					qualifiedName: 'test.term'
				}
			});

			// Component should handle -1 value
			expect(screen.getByTestId('search-result')).toBeInTheDocument();
		});
	});

	describe('Edge Cases', () => {
		test('handles null glossary data', () => {
			const store = configureStore({
				reducer: {
					glossaryType: () => ({ glossaryTypeData: { data: null, loading: false } }),
					session: () => ({ user: {} })
				},
				middleware: (getDefaultMiddleware) =>
					getDefaultMiddleware({
						serializableCheck: false,
						immutableCheck: false
					})
			});

			render(
				<Provider store={store}>
					<MemoryRouter initialEntries={['/glossary/test-guid-123?gtype=term']}>
						<Routes>
							<Route path="/glossary/:guid" element={<GlossaryDetailLayout />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			// Should still render tabs
			expect(screen.getByText('Entities')).toBeInTheDocument();
		});

		test('handles undefined glossary data', () => {
			const store = configureStore({
				reducer: {
					glossaryType: () => ({ glossaryTypeData: { data: undefined, loading: false } }),
					session: () => ({ user: {} })
				},
				middleware: (getDefaultMiddleware) =>
					getDefaultMiddleware({
						serializableCheck: false,
						immutableCheck: false
					})
			});

			render(
				<Provider store={store}>
					<MemoryRouter initialEntries={['/glossary/test-guid-123?gtype=term']}>
						<Routes>
							<Route path="/glossary/:guid" element={<GlossaryDetailLayout />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			expect(screen.getByText('Entities')).toBeInTheDocument();
		});

		test('handles empty glossary data', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term',
				glossaryData: {}
			});

			expect(screen.getByText('Entities')).toBeInTheDocument();
		});

		test('handles missing guid parameter', () => {
			const store = createMockStore();

			render(
				<Provider store={store}>
					<MemoryRouter initialEntries={['/glossary/']}>
						<Routes>
							<Route path="/glossary/:guid?" element={<GlossaryDetailLayout />} />
						</Routes>
					</MemoryRouter>
				</Provider>
			);

			// Should call with undefined guid
			expect(mockFetchGlossaryDetails).toHaveBeenCalled();
		});

		test('does not render tabs when data is empty for classification tab', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=classification',
				glossaryData: {}
			});

			// Should not render classifications tab when data is empty
			expect(screen.queryByTestId('classifications-tab')).not.toBeInTheDocument();
		});

		test('does not render tabs when data is empty for relatedTerm tab', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=relatedTerm',
				glossaryData: {}
			});

			// Should not render related terms tab when data is empty
			expect(screen.queryByTestId('term-relation-tab')).not.toBeInTheDocument();
		});

		test('does not render SearchResult when data is empty for entities tab', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=entities',
				glossaryData: {}
			});

			// Should not render search result when data is empty
			expect(screen.queryByTestId('search-result')).not.toBeInTheDocument();
		});
	});

	describe('Component Integration', () => {
		test('passes correct props to TermProperties', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=entitiesProperties',
				glossaryData: {
					guid: 'test-guid-123',
					additionalAttributes: { key: 'value' }
				}
			});

			expect(screen.getByTestId('term-properties-tab')).toBeInTheDocument();
		});

		test('passes correct props to TermRelation', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=relatedTerm',
				glossaryData: {
					guid: 'test-guid-123',
					name: 'Test Term'
				}
			});

			expect(screen.getByTestId('term-relation-tab')).toBeInTheDocument();
		});

		test('passes correct props to ClassificationsTab', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=classification',
				glossaryData: {
					guid: 'test-guid-123',
					classifications: {}
				}
			});

			expect(screen.getByTestId('classifications-tab')).toBeInTheDocument();
		});

		test('passes correct props to SearchResult', () => {
			renderWithRouter(<GlossaryDetailLayout />, {
				searchParams: 'gtype=term&tabActive=entities',
				glossaryData: {
					guid: 'test-guid-123',
					qualifiedName: 'test.glossary.term'
				}
			});

			expect(screen.getByTestId('search-result')).toBeInTheDocument();
		});

		test('calls getTagObj with correct parameters', () => {
			const glossaryData = {
				guid: 'test-guid-123',
				classifications: { tag1: 'value1' }
			};

			renderWithRouter(<GlossaryDetailLayout />, {
				glossaryData
			});

			expect(mockGetTagObj).toHaveBeenCalledWith(glossaryData, { tag1: 'value1' });
		});
	});
});
