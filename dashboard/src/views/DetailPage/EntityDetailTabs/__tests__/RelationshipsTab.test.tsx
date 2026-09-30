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
import { act, render, screen, fireEvent, waitFor } from '@utils/test-utils';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import RelationshipsTab from '../RelationshipsTab';
import { getRelationShipV2 } from '@api/apiMethods/searchApiMethod';

jest.mock('@api/apiMethods/searchApiMethod', () => ({
	getRelationShipV2: jest.fn().mockResolvedValue({
		data: { entities: [], approximateCount: 0 }
	})
}));

jest.mock('@utils/Utils', () => ({
	...jest.requireActual('@utils/Utils'),
	getBaseUrl: () => ''
}));

jest.mock('../RelationshipLineage', () => ({
	__esModule: true,
	default: ({ entity }: { entity?: { guid?: string } }) => (
		<div data-testid="relationship-lineage">
			<div data-testid="lineage-entity-guid">{entity?.guid || 'no-guid'}</div>
			Lineage
		</div>
	)
}));

jest.mock('../RelationshipCard', () => ({
	__esModule: true,
	default: ({ attributeName }: { attributeName: string }) => (
		<div data-testid={`relationship-card-${attributeName}`}>
			{attributeName}
		</div>
	)
}));

jest.mock('../RelationshipCardSkeleton', () => ({
	__esModule: true,
	default: () => <div data-testid="relationship-card-skeleton" />
}));

jest.mock('@utils/Muiutils', () => ({
	AntSwitch: React.forwardRef(
		(
			{
				checked,
				onChange,
				onClick,
				inputProps,
				sx: _sx,
				...rest
			}: Record<string, unknown>,
			ref: React.Ref<HTMLInputElement>
		) => (
			<input
				ref={ref}
				type="checkbox"
				data-testid="ant-switch"
				checked={checked as boolean}
				onChange={onChange as React.ChangeEventHandler<HTMLInputElement>}
				onClick={onClick as React.MouseEventHandler<HTMLInputElement>}
				{...(inputProps as object)}
				{...rest}
			/>
		)
	)
}));

const theme = createTheme();

/** TestWrapper includes MemoryRouter; disable duplicate BrowserRouter from test-utils. */
const renderRelationships = (
	ui: React.ReactElement,
	options?: Parameters<typeof render>[1]
) => render(ui, { withRouter: false, ...options });

const buildEntityPreloadedState = (
	relationshipAttributeDefs: Array<{ name: string }> = []
) => ({
	entity: {
		entityData: {
			entityDefs: [
				{
					name: 'DataSet',
					attributeDefs: [],
					relationshipAttributeDefs
				}
			]
		}
	}
});

const createRelationshipsStore = (
	relationshipAttributeDefs: Array<{ name: string }> = []
) =>
	configureStore({
		reducer: {
			entity: (state: { entityData?: { entityDefs?: unknown[] } } = {}) => state
		},
		preloadedState: buildEntityPreloadedState(relationshipAttributeDefs)
	});

interface TestWrapperProps {
	children: React.ReactElement;
	store: ReturnType<typeof createRelationshipsStore>;
	initialPath?: string;
}

const TestWrapper: React.FC<TestWrapperProps> = ({
	children,
	store,
	initialPath = '/detailPage/test-guid-123'
}) => (
	<Provider store={store}>
		<ThemeProvider theme={theme}>
			<MemoryRouter initialEntries={[initialPath]}>
				<Routes>
					<Route path="/detailPage/:guid" element={children} />
				</Routes>
			</MemoryRouter>
		</ThemeProvider>
	</Provider>
);

describe('RelationshipsTab', () => {
	let store: ReturnType<typeof createRelationshipsStore>;

	const mockEntity = {
		guid: 'test-guid-123',
		typeName: 'DataSet',
		attributes: {
			name: 'Test Dataset'
		},
		relationshipAttributes: {
			inputToProcesses: [
				{ guid: 'proc-1', typeName: 'Process', attributes: { name: 'P1' } }
			]
		}
	};

	const mockReferredEntities = {
		'proc-1': { typeName: 'Process', attributes: { name: 'P1' } }
	};

	const defaultProps = {
		entity: mockEntity,
		referredEntities: mockReferredEntities,
		loading: false
	};

	beforeEach(() => {
		store = createRelationshipsStore([]);
		jest.clearAllMocks();
		(getRelationShipV2 as jest.Mock).mockResolvedValue({
			data: { entities: [], approximateCount: 0 }
		});
	});

	it('renders with Redux provider and route params (guid)', () => {
		renderRelationships(
			<TestWrapper store={store}>
				<RelationshipsTab {...defaultProps} />
			</TestWrapper>
		);

		expect(document.querySelector('.properties-container')).toBeInTheDocument();
	});

	it('renders Graph and Table view toggles', () => {
		renderRelationships(
			<TestWrapper store={store}>
				<RelationshipsTab {...defaultProps} />
			</TestWrapper>
		);

		expect(screen.getByRole('button', { name: /^graph$/i })).toBeInTheDocument();
		expect(screen.getByRole('button', { name: /^table$/i })).toBeInTheDocument();
	});

	it('shows empty state after skeleton when no relationship attribute defs exist', () => {
		jest.useFakeTimers();
		renderRelationships(
			<TestWrapper store={store}>
				<RelationshipsTab {...defaultProps} />
			</TestWrapper>
		);

		expect(screen.getAllByTestId('relationship-card-skeleton').length).toBeGreaterThan(0);

		act(() => {
			jest.advanceTimersByTime(6000);
		});

		expect(
			screen.getByText(/No relationship data available/i)
		).toBeInTheDocument();
		jest.useRealTimers();
	});

	it('hides Show Empty Values switch in graph view', () => {
		renderRelationships(
			<TestWrapper store={store}>
				<RelationshipsTab {...defaultProps} />
			</TestWrapper>
		);

		expect(screen.getByTestId('ant-switch')).toBeInTheDocument();

		fireEvent.click(screen.getByRole('button', { name: /^graph$/i }));

		expect(screen.queryByTestId('ant-switch')).not.toBeInTheDocument();
	});

	it('shows RelationshipLineage in graph view with entity guid', () => {
		renderRelationships(
			<TestWrapper store={store}>
				<RelationshipsTab {...defaultProps} />
			</TestWrapper>
		);

		fireEvent.click(screen.getByRole('button', { name: /^graph$/i }));

		expect(screen.getByTestId('relationship-lineage')).toBeInTheDocument();
		expect(screen.getByTestId('lineage-entity-guid')).toHaveTextContent(
			'test-guid-123'
		);
	});

	it('fetches relationship cards when typedef defines relationship attributes', async () => {
		store = createRelationshipsStore([{ name: 'inputToProcesses' }]);
		(getRelationShipV2 as jest.Mock).mockResolvedValue({
			data: {
				entities: [
					{ guid: 'rel-1', typeName: 'Process', attributes: { name: 'P1' } }
				],
				approximateCount: 1
			}
		});

		renderRelationships(
			<TestWrapper store={store}>
				<RelationshipsTab {...defaultProps} />
			</TestWrapper>
		);

		await waitFor(() => {
			expect(getRelationShipV2).toHaveBeenCalled();
		});

		await waitFor(() => {
			expect(
				screen.getByTestId('relationship-card-inputToProcesses')
			).toBeInTheDocument();
		});
	});

	it('toggles Show Empty Values switch in table view', () => {
		renderRelationships(
			<TestWrapper store={store}>
				<RelationshipsTab {...defaultProps} />
			</TestWrapper>
		);

		const switchEl = screen.getByTestId('ant-switch') as HTMLInputElement;
		expect(switchEl.checked).toBe(false);
		fireEvent.change(switchEl, { target: { checked: true } });
		expect(switchEl.checked).toBe(true);
	});
});
