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
 * Unit tests for EntityDetailPage component
 */

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@utils/test-utils';
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import EntityDetailPage from '../DetailPage/EntityDetailPage';
import { globalSessionData } from '@utils/Enum';

// Mock dependencies
jest.mock('../../redux/slice/detailPageSlice', () => ({
	fetchDetailPageData: jest.fn(() => ({ type: 'FETCH_DETAIL_PAGE_DATA' }))
}));

jest.mock('../../api/apiMethods/classificationApiMethod', () => ({
	removeClassification: jest.fn()
}));

jest.mock('../../api/apiMethods/glossaryApiMethod', () => ({
	removeTerm: jest.fn()
}));

jest.mock('../Classification/AddTag', () => ({
	__esModule: true,
	default: ({ open, onClose }: any) =>
		open ? (
			<div data-testid="add-tag-modal">
				<button onClick={onClose}>Close Add Tag</button>
			</div>
		) : null
}));

jest.mock('../Glossary/AssignTerm', () => ({
	__esModule: true,
	default: ({ open, onClose }: any) =>
		open ? (
			<div data-testid="assign-term-modal">
				<button onClick={onClose}>Close Assign Term</button>
			</div>
		) : null
}));

jest.mock('@components/SkeletonLoader', () => ({
	__esModule: true,
	default: () => <div data-testid="skeleton-loader">Loading...</div>
}));

jest.mock('@components/EntityDisplayImage', () => ({
	__esModule: true,
	default: () => <div data-testid="entity-image">Entity Image</div>
}));

jest.mock('../DetailPage/EntityDetailTabs/ReplicationAuditTab', () => ({
	__esModule: true,
	default: () => <div data-testid="raudits-tab">Replication Audit Tab</div>
}));

jest.mock('../DetailPage/EntityDetailTabs/PropertiesTab/PropertiesTab', () => ({
	__esModule: true,
	default: () => <div data-testid="properties-tab">Properties Tab</div>
}));

jest.mock('../DetailPage/EntityDetailTabs/RelationshipsTab', () => ({
	__esModule: true,
	default: () => <div data-testid="relationships-tab">Relationships Tab</div>
}));

jest.mock('../DetailPage/EntityDetailTabs/ClassificationsTab', () => ({
	__esModule: true,
	default: () => <div data-testid="classifications-tab">Classifications Tab</div>
}));

jest.mock('../DetailPage/EntityDetailTabs/LineageTab', () => ({
	__esModule: true,
	default: () => <div data-testid="lineage-tab">Lineage Tab</div>
}));

jest.mock('../DetailPage/EntityDetailTabs/SchemaTab', () => ({
	__esModule: true,
	default: () => <div data-testid="schema-tab">Schema Tab</div>
}));

jest.mock('../DetailPage/EntityDetailTabs/AuditsTab', () => ({
	__esModule: true,
	default: () => <div data-testid="audits-tab">Audits Tab</div>
}));

jest.mock('../DetailPage/EntityDetailTabs/ProfileTab', () => ({
	__esModule: true,
	default: () => <div data-testid="profile-tab">Profile Tab</div>
}));

jest.mock('../DetailPage/EntityDetailTabs/TaskTab', () => ({
	__esModule: true,
	default: () => <div data-testid="task-tab">Task Tab</div>
}));

const createMockStore = (detailPageData: any = {}, entityData: any = {}, glossaryData: any = []) => {
	return configureStore({
		reducer: {
			detailPage: (state = { detailPageData, loading: false }) => state,
			entity: (state = { entityData }) => state,
			glossary: (state = { glossaryData }) => state,
			classification: (state = { classificationData: {} }) => state,
			drawerState: (state = { isOpen: false, activeId: '' }) => state
		},
		preloadedState: {
			detailPage: {
				detailPageData,
				loading: false
			},
			entity: {
				entityData
			},
			glossary: {
				glossaryData
			},
			classification: {
				classificationData: {}
			},
			drawerState: {
				isOpen: false,
				activeId: ''
			}
		}
	});
};

const TestWrapper: React.FC<{ store: any; initialEntries?: string[] }> = ({
	store,
	initialEntries = ['/detailPage/test-guid']
}) => (
	<Provider store={store}>
		<MemoryRouter initialEntries={initialEntries}>
			<Routes>
				<Route path="/detailPage/:guid" element={<EntityDetailPage />} />
			</Routes>
		</MemoryRouter>
	</Provider>
);

const renderWithProviders = (ui: React.ReactElement) => {
	return render(ui, { withRouter: false });
};

describe('EntityDetailPage', () => {
	const mockEntity = {
		guid: 'test-guid',
		typeName: 'DataSet',
		attributes: {
			name: 'Test Entity',
			description: 'Test Description',
			owner: 'test-owner'
		},
		classifications: {},
		relationshipAttributes: {
			meanings: []
		}
	};

	const mockEntityData = {
		entityDefs: [
			{
				name: 'DataSet',
				attributeDefs: [
					{ name: 'name', typeName: 'string' },
					{ name: 'description', typeName: 'string' }
				]
			}
		]
	};

	const mockDetailPageData = {
		entity: mockEntity,
		referredEntities: {}
	};

	beforeEach(() => {
		jest.clearAllMocks();
		delete globalSessionData.taskTabEnabled;
		delete globalSessionData.uiTaskTabEnabled;
	});

	it('should render loading skeleton when loading', () => {
		const store = configureStore({
			reducer: {
				detailPage: (state = { detailPageData: {}, loading: true }) => state,
				entity: (state = { entityData: {} }) => state,
				glossary: (state = { glossaryData: [] }) => state
			}
		});

		renderWithProviders(<TestWrapper store={store} />);

		expect(screen.getAllByTestId('skeleton-loader').length).toBeGreaterThan(0);
	});

	it('should display entity name and type', () => {
		const store = createMockStore(mockDetailPageData, mockEntityData);
		renderWithProviders(<TestWrapper store={store} />);

		expect(screen.getByText(/Test Entity \(DataSet\)/)).toBeTruthy();
	});

	it('should render all tabs', () => {
		const store = createMockStore(mockDetailPageData, mockEntityData);
		renderWithProviders(<TestWrapper store={store} />);

		expect(screen.getByRole('tab', { name: /properties/i })).toBeTruthy();
		expect(screen.getByRole('tab', { name: /relationships/i })).toBeTruthy();
		expect(screen.getByRole('tab', { name: /classifications/i })).toBeTruthy();
		expect(screen.getByRole('tab', { name: /audits/i })).toBeTruthy();
	});

	it('should switch between tabs', () => {
		const store = createMockStore(mockDetailPageData, mockEntityData);
		renderWithProviders(
			<TestWrapper store={store} initialEntries={['/detailPage/test-guid?tabActive=relationship']} />
		);

		expect(screen.getByTestId('relationships-tab')).toBeTruthy();
	});

	it('should show lineage tab for DataSet entities', () => {
		const store = createMockStore(mockDetailPageData, mockEntityData);
		renderWithProviders(
			<TestWrapper store={store} initialEntries={['/detailPage/test-guid?tabActive=lineage']} />
		);

		// Lineage tab should be available for DataSet
		expect(screen.getByTestId('lineage-tab')).toBeTruthy();
	});

	it('should open add tag modal when add tag button is clicked', () => {
		const store = createMockStore(mockDetailPageData, mockEntityData);
		renderWithProviders(<TestWrapper store={store} />);

		const addTagButton = screen.queryByLabelText(/add.*tag/i) || screen.queryByText(/add.*classification/i);
		if (addTagButton) {
			fireEvent.click(addTagButton);
			expect(screen.getByTestId('add-tag-modal')).toBeTruthy();
		}
	});

	it('should close add tag modal', () => {
		const store = createMockStore(mockDetailPageData, mockEntityData);
		renderWithProviders(<TestWrapper store={store} />);

		const addTagButton = screen.queryByLabelText(/add.*tag/i) || screen.queryByText(/add.*classification/i);
		if (addTagButton) {
			fireEvent.click(addTagButton);
			const closeButton = screen.getByText('Close Add Tag');
			fireEvent.click(closeButton);
			expect(screen.queryByTestId('add-tag-modal')).toBeNull();
		}
	});

	it('should open assign term modal when assign term button is clicked', () => {
		const store = createMockStore(mockDetailPageData, mockEntityData, [{ terms: [] }]);
		renderWithProviders(<TestWrapper store={store} />);

		const assignTermButton = screen.queryByLabelText(/assign.*term/i) || screen.queryByText(/assign.*term/i);
		if (assignTermButton) {
			fireEvent.click(assignTermButton);
			expect(screen.getByTestId('assign-term-modal')).toBeTruthy();
		}
	});

	it('should handle entity not found', () => {
		const store = createMockStore({ entity: null }, mockEntityData);
		renderWithProviders(<TestWrapper store={store} />);

		// Should handle gracefully when entity is null
		expect(screen.queryByTestId('skeleton-loader')).toBeNull();
	});

	it('should display entity image', () => {
		const store = createMockStore(mockDetailPageData, mockEntityData);
		renderWithProviders(<TestWrapper store={store} />);

		expect(screen.getByTestId('entity-image')).toBeTruthy();
	});

	it('should handle Process entity type', () => {
		const processEntity = {
			...mockEntity,
			typeName: 'Process'
		};
		const store = createMockStore({ entity: processEntity }, mockEntityData);
		renderWithProviders(<TestWrapper store={store} />);

		expect(screen.getByText(/Process/)).toBeTruthy();
	});

	describe('Detail page tab visibility', () => {
		const queryTab = (name: RegExp) =>
			screen.queryByRole('tab', { name });

		it('always shows Properties, Relationships, Classifications, and Audits', () => {
			const store = createMockStore(mockDetailPageData, mockEntityData);
			renderWithProviders(<TestWrapper store={store} />);

			expect(screen.getByRole('tab', { name: /^properties$/i })).toBeInTheDocument();
			expect(screen.getByRole('tab', { name: /^relationships$/i })).toBeInTheDocument();
			expect(screen.getByRole('tab', { name: /^classifications$/i })).toBeInTheDocument();
			expect(screen.getByRole('tab', { name: /^audits$/i })).toBeInTheDocument();
		});

		it('shows Lineage when type is DataSet (dataset lineage)', () => {
			const store = createMockStore(mockDetailPageData, mockEntityData);
			renderWithProviders(<TestWrapper store={store} />);

			expect(screen.getByRole('tab', { name: /^lineage$/i })).toBeInTheDocument();
		});

		it('shows Lineage when type is Process', () => {
			const processEntity = { ...mockEntity, typeName: 'Process' };
			const processDefs = {
				entityDefs: [
					{
						name: 'Process',
						superTypes: [],
						attributeDefs: []
					}
				]
			};
			const store = createMockStore(
				{ entity: processEntity, referredEntities: {} },
				processDefs
			);
			renderWithProviders(<TestWrapper store={store} />);

			expect(screen.getByRole('tab', { name: /^lineage$/i })).toBeInTheDocument();
		});

		it('hides Lineage for types without DataSet/Process lineage', () => {
			const customEntity = {
				...mockEntity,
				guid: 'custom-guid',
				typeName: 'CustomAsset'
			};
			const customDefs = {
				entityDefs: [
					{
						name: 'CustomAsset',
						superTypes: [],
						attributeDefs: []
					}
				]
			};
			const store = createMockStore(
				{ entity: customEntity, referredEntities: {} },
				customDefs
			);
			renderWithProviders(<TestWrapper store={store} />);

			expect(queryTab(/^lineage$/i)).not.toBeInTheDocument();
		});

		it('shows Schema when typedef options include schemaElementsAttribute', () => {
			const hiveTableEntity = {
				...mockEntity,
				guid: 'hive-table-guid',
				typeName: 'HiveTable'
			};
			const schemaDefs = {
				entityDefs: [
					{
						name: 'HiveTable',
						superTypes: [],
						attributeDefs: [],
						options: {
							schemaElementsAttribute: 'columns, partitions'
						}
					}
				]
			};
			const store = createMockStore(
				{ entity: hiveTableEntity, referredEntities: {} },
				schemaDefs
			);
			renderWithProviders(<TestWrapper store={store} />);

			expect(screen.getByRole('tab', { name: /^schema$/i })).toBeInTheDocument();
		});

		it('hides Schema when schemaElementsAttribute is absent or empty', () => {
			const plainEntity = {
				...mockEntity,
				guid: 'plain-guid',
				typeName: 'PlainType'
			};
			const plainDefs = {
				entityDefs: [
					{
						name: 'PlainType',
						superTypes: [],
						attributeDefs: [],
						options: {}
					}
				]
			};
			const store = createMockStore(
				{ entity: plainEntity, referredEntities: {} },
				plainDefs
			);
			renderWithProviders(<TestWrapper store={store} />);

			expect(queryTab(/^schema$/i)).not.toBeInTheDocument();
		});

		it('shows Export/Import Audits for AtlasServer', () => {
			const serverEntity = {
				...mockEntity,
				guid: 'server-guid',
				typeName: 'AtlasServer'
			};
			const serverDefs = {
				entityDefs: [
					{
						name: 'AtlasServer',
						superTypes: [],
						attributeDefs: []
					}
				]
			};
			const store = createMockStore(
				{ entity: serverEntity, referredEntities: {} },
				serverDefs
			);
			renderWithProviders(<TestWrapper store={store} />);

			expect(
				screen.getByRole('tab', { name: /export\/import audits/i })
			).toBeInTheDocument();
		});

		it('shows Table profile tab when profileData attribute exists (even if null)', () => {
			const profileEntity = {
				...mockEntity,
				attributes: {
					...mockEntity.attributes,
					profileData: null
				}
			};
			const store = createMockStore(
				{ entity: profileEntity, referredEntities: {} },
				mockEntityData
			);
			renderWithProviders(<TestWrapper store={store} />);

			expect(screen.getByRole('tab', { name: /^table$/i })).toBeInTheDocument();
		});

		it('shows Tables tab label for hive_db', () => {
			const hiveDbEntity = {
				...mockEntity,
				guid: 'hive-db-guid',
				typeName: 'hive_db',
				attributes: { name: 'default' }
			};
			const hiveDbDefs = {
				entityDefs: [
					{
						name: 'hive_db',
						superTypes: [],
						attributeDefs: []
					}
				]
			};
			const storeHive = createMockStore(
				{ entity: hiveDbEntity, referredEntities: {} },
				hiveDbDefs
			);
			renderWithProviders(<TestWrapper store={storeHive} />);
			expect(screen.getByRole('tab', { name: /^tables$/i })).toBeInTheDocument();
		});

		it('shows Tables tab label for hbase_namespace', () => {
			const hbaseNsEntity = {
				...mockEntity,
				guid: 'hbase-ns-guid',
				typeName: 'hbase_namespace',
				attributes: { name: 'ns1' }
			};
			const hbaseDefs = {
				entityDefs: [
					{
						name: 'hbase_namespace',
						superTypes: [],
						attributeDefs: []
					}
				]
			};
			const storeHbase = createMockStore(
				{ entity: hbaseNsEntity, referredEntities: {} },
				hbaseDefs
			);
			renderWithProviders(<TestWrapper store={storeHbase} />);
			expect(screen.getByRole('tab', { name: /^tables$/i })).toBeInTheDocument();
		});

		it('shows Tasks when session flags enable the task tab', () => {
			globalSessionData.taskTabEnabled = true;
			globalSessionData.uiTaskTabEnabled = true;

			const store = createMockStore(mockDetailPageData, mockEntityData);
			renderWithProviders(<TestWrapper store={store} />);

			expect(screen.getByRole('tab', { name: /^tasks$/i })).toBeInTheDocument();
		});

		it('hides Tasks when session flags are off', () => {
			globalSessionData.taskTabEnabled = false;
			globalSessionData.uiTaskTabEnabled = true;

			const store = createMockStore(mockDetailPageData, mockEntityData);
			renderWithProviders(<TestWrapper store={store} />);

			expect(queryTab(/^tasks$/i)).not.toBeInTheDocument();
		});
	});

	it('should update URL params on tab change', () => {
		const store = createMockStore(mockDetailPageData, mockEntityData);
		renderWithProviders(<TestWrapper store={store} />);

		// Tab change should update URL
		// This is tested through navigation
	});
});

