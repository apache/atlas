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
 * Integration tests for Router
 */

import React, { Suspense } from 'react';
import { act, waitFor } from '@testing-library/react';
import { screen } from '@utils/test-utils';
import Router from '../views/Router';
import { render } from '@testing-library/react';

// Module-level variable to store initial route for HashRouter mock
let mockInitialRoute = '/';

// Mock HashRouter to use MemoryRouter for testing (to avoid Router inside Router)
// This mock must be hoisted before any imports that use react-router-dom
jest.mock('react-router-dom', () => {
	const actual = jest.requireActual('react-router-dom');
	const React = require('react');
	const { MemoryRouter } = actual;
	
	return {
		...actual,
		HashRouter: ({ children }: any) => {
			// Read from module-level variable or window.location.hash as fallback
			const getInitialRoute = () => {
				// Try to read from the module variable first
				if (typeof (global as any).__MOCK_INITIAL_ROUTE__ !== 'undefined') {
					return (global as any).__MOCK_INITIAL_ROUTE__;
				}
				// Fallback to window.location.hash
				try {
					const hash = window.location?.hash || '#/';
					return hash.startsWith('#') ? hash.substring(1) : hash || '/';
				} catch {
					return '/';
				}
			};
			const pathname = getInitialRoute();
			const initialEntries = [pathname];
			return React.createElement(MemoryRouter, { initialEntries }, children);
		}
	};
});

// Mock all lazy-loaded components - hoisted mocks
jest.mock('../views/Layout/Layout', () => {
	const React = require('react');
	const { Outlet } = require('react-router-dom');
	return {
		__esModule: true,
		default: () => (
			<div data-testid="layout">
				<Outlet />
			</div>
		)
	};
});

jest.mock('../views/SearchResult/SearchResult', () => ({
	__esModule: true,
	default: () => <div data-testid="search-result">Search Result</div>
}));

jest.mock('../views/DashBoard', () => ({
	__esModule: true,
	default: () => <div data-testid="dashboard">Dashboard</div>
}));

jest.mock('../views/DetailPage/EntityDetailPage', () => ({
	__esModule: true,
	default: () => <div data-testid="entity-detail">Entity Detail</div>
}));

jest.mock('../views/DetailPage/ClassificationDetailsLayout', () => ({
	__esModule: true,
	default: () => <div data-testid="classification-details">Classification Details</div>
}));

jest.mock('../views/Administrator/AdministratorLayout', () => ({
	__esModule: true,
	default: () => <div data-testid="administrator">Administrator</div>
}));

jest.mock('../views/DetailPage/BusinessMetadataDetails/BusinessMetadataDetailsLayout', () => ({
	__esModule: true,
	default: () => <div data-testid="bm-details">Business Metadata Details</div>
}));

jest.mock('../views/DetailPage/GlossaryDetails/GlossaryDetailsLayout', () => ({
	__esModule: true,
	default: () => <div data-testid="glossary-details">Glossary Details</div>
}));

jest.mock('../views/DetailPage/RelationshipDetails/RelationshipDetailsLayout', () => ({
	__esModule: true,
	default: () => <div data-testid="relationship-details">Relationship Details</div>
}));

jest.mock('../views/Layout/DebugMetrics', () => ({
	__esModule: true,
	default: () => <div data-testid="debug-metrics">Debug Metrics</div>
}));

describe('Router Integration', () => {
	beforeEach(() => {
		// Reset mock initial route before each test
		(global as any).__MOCK_INITIAL_ROUTE__ = '/';
		// Reset window.location.hash before each test
		Object.defineProperty(window, 'location', {
			value: {
				hash: '',
				pathname: '/',
				search: ''
			},
			writable: true,
			configurable: true
		});
	});

	it('should render dashboard route', async () => {
		// Set initial route for HashRouter mock
		(global as any).__MOCK_INITIAL_ROUTE__ = '/search';
		// Set hash for default route (which renders dashboard)
		Object.defineProperty(window, 'location', {
			value: {
				hash: '#/search',
				pathname: '/',
				search: ''
			},
			writable: true,
			configurable: true
		});

		await act(async () => {
			render(
				<Suspense fallback={<div>Loading...</div>}>
					<Router />
				</Suspense>
			);
		});

		await waitFor(
			() => {
				expect(screen.getByTestId('layout')).toBeInTheDocument();
				expect(screen.getByTestId('dashboard')).toBeInTheDocument();
			},
			{ timeout: 10000 }
		);
	}, 30000);

	it('should render search results route', async () => {
		(global as any).__MOCK_INITIAL_ROUTE__ = '/search/searchResult';
		Object.defineProperty(window, 'location', {
			value: {
				hash: '#/search/searchResult',
				pathname: '/',
				search: ''
			},
			writable: true,
			configurable: true
		});

		await act(async () => {
			render(
				<Suspense fallback={<div>Loading...</div>}>
					<Router />
				</Suspense>
			);
		});

		await waitFor(
			() => {
				expect(screen.getByTestId('layout')).toBeInTheDocument();
				expect(screen.getByTestId('search-result')).toBeInTheDocument();
			},
			{ timeout: 10000 }
		);
	}, 30000);

	it('should render entity detail page route', async () => {
		(global as any).__MOCK_INITIAL_ROUTE__ = '/detailPage/test-guid';
		Object.defineProperty(window, 'location', {
			value: {
				hash: '#/detailPage/test-guid',
				pathname: '/',
				search: ''
			},
			writable: true,
			configurable: true
		});

		await act(async () => {
			render(
				<Suspense fallback={<div>Loading...</div>}>
					<Router />
				</Suspense>
			);
		});

		await waitFor(
			() => {
				expect(screen.getByTestId('layout')).toBeInTheDocument();
				expect(screen.getByTestId('entity-detail')).toBeInTheDocument();
			},
			{ timeout: 10000 }
		);
	}, 30000);

	it('should render classification details route', async () => {
		(global as any).__MOCK_INITIAL_ROUTE__ = '/tag/tagAttribute/PII';
		Object.defineProperty(window, 'location', {
			value: {
				hash: '#/tag/tagAttribute/PII',
				pathname: '/',
				search: ''
			},
			writable: true,
			configurable: true
		});

		await act(async () => {
			render(
				<Suspense fallback={<div>Loading...</div>}>
					<Router />
				</Suspense>
			);
		});

		await waitFor(
			() => {
				expect(screen.getByTestId('layout')).toBeInTheDocument();
				expect(screen.getByTestId('classification-details')).toBeInTheDocument();
			},
			{ timeout: 10000 }
		);
	}, 30000);

	it('should render administrator route', async () => {
		(global as any).__MOCK_INITIAL_ROUTE__ = '/administrator';
		Object.defineProperty(window, 'location', {
			value: {
				hash: '#/administrator',
				pathname: '/',
				search: ''
			},
			writable: true,
			configurable: true
		});

		await act(async () => {
			render(
				<Suspense fallback={<div>Loading...</div>}>
					<Router />
				</Suspense>
			);
		});

		await waitFor(
			() => {
				expect(screen.getByTestId('layout')).toBeInTheDocument();
				expect(screen.getByTestId('administrator')).toBeInTheDocument();
			},
			{ timeout: 10000 }
		);
	}, 30000);

	it('should render business metadata details route', async () => {
		(global as any).__MOCK_INITIAL_ROUTE__ = '/administrator/businessMetadata/bm-guid';
		Object.defineProperty(window, 'location', {
			value: {
				hash: '#/administrator/businessMetadata/bm-guid',
				pathname: '/',
				search: ''
			},
			writable: true,
			configurable: true
		});

		await act(async () => {
			render(
				<Suspense fallback={<div>Loading...</div>}>
					<Router />
				</Suspense>
			);
		});

		await waitFor(
			() => {
				expect(screen.getByTestId('layout')).toBeInTheDocument();
				expect(screen.getByTestId('bm-details')).toBeInTheDocument();
			},
			{ timeout: 10000 }
		);
	}, 30000);

	it('should render glossary details route', async () => {
		(global as any).__MOCK_INITIAL_ROUTE__ = '/glossary/glossary-guid';
		Object.defineProperty(window, 'location', {
			value: {
				hash: '#/glossary/glossary-guid',
				pathname: '/',
				search: ''
			},
			writable: true,
			configurable: true
		});

		await act(async () => {
			render(
				<Suspense fallback={<div>Loading...</div>}>
					<Router />
				</Suspense>
			);
		});

		await waitFor(
			() => {
				expect(screen.getByTestId('layout')).toBeInTheDocument();
				expect(screen.getByTestId('glossary-details')).toBeInTheDocument();
			},
			{ timeout: 10000 }
		);
	}, 30000);

	it('should render relationship details route', async () => {
		(global as any).__MOCK_INITIAL_ROUTE__ = '/relationshipDetailPage/rel-guid';
		Object.defineProperty(window, 'location', {
			value: {
				hash: '#/relationshipDetailPage/rel-guid',
				pathname: '/',
				search: ''
			},
			writable: true,
			configurable: true
		});

		await act(async () => {
			render(
				<Suspense fallback={<div>Loading...</div>}>
					<Router />
				</Suspense>
			);
		});

		await waitFor(
			() => {
				expect(screen.getByTestId('layout')).toBeInTheDocument();
				expect(screen.getByTestId('relationship-details')).toBeInTheDocument();
			},
			{ timeout: 10000 }
		);
	}, 30000);

	it('should render debug metrics route', async () => {
		(global as any).__MOCK_INITIAL_ROUTE__ = '/debugMetrics';
		Object.defineProperty(window, 'location', {
			value: {
				hash: '#/debugMetrics',
				pathname: '/',
				search: ''
			},
			writable: true,
			configurable: true
		});

		await act(async () => {
			render(
				<Suspense fallback={<div>Loading...</div>}>
					<Router />
				</Suspense>
			);
		});

		await waitFor(
			() => {
				expect(screen.getByTestId('layout')).toBeInTheDocument();
				expect(screen.getByTestId('debug-metrics')).toBeInTheDocument();
			},
			{ timeout: 10000 }
		);
	}, 30000);

	it('should handle 404 route (fallback to dashboard)', async () => {
		(global as any).__MOCK_INITIAL_ROUTE__ = '/nonexistent-route';
		Object.defineProperty(window, 'location', {
			value: {
				hash: '#/nonexistent-route',
				pathname: '/',
				search: ''
			},
			writable: true,
			configurable: true
		});

		await act(async () => {
			render(
				<Suspense fallback={<div>Loading...</div>}>
					<Router />
				</Suspense>
			);
		});

		await waitFor(
			() => {
				expect(screen.getByTestId('layout')).toBeInTheDocument();
				expect(screen.getByTestId('dashboard')).toBeInTheDocument();
			},
			{ timeout: 10000 }
		);
	}, 30000);
});
