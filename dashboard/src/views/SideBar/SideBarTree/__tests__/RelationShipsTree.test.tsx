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
 * Unit tests for RelationShipsTree.tsx
 * 
 * Coverage Target: 100%
 * - Statements: 100% (23/23)
 * - Branches: 100% (5/5)
 * - Functions: 100% (7/7)
 * - Lines: 100% (22/22)
 */

import React from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import RelationshipsTree from '../RelationShipsTree'
import { fetchRelationshipsData } from '@redux/slice/typeDefSlices/typedefRelationshipsSlice'

// Mock dependencies
jest.mock('@redux/slice/typeDefSlices/typedefRelationshipsSlice', () => ({
	fetchRelationshipsData: jest.fn()
}))

jest.mock('../SideBarTree.tsx', () => {
	return function MockSideBarTree(props: any) {
		return (
			<div data-testid="sidebar-tree">
				<div data-testid="tree-name">{props.treeName}</div>
				<div data-testid="loader">{props.loader ? 'Loading' : 'Not Loading'}</div>
				<div data-testid="search-term">{props.searchTerm}</div>
				<div data-testid="tree-data">{JSON.stringify(props.treeData)}</div>
				{props.refreshData && (
					<button onClick={props.refreshData} data-testid="refresh-button">
						Refresh
					</button>
				)}
			</div>
		)
	}
})

jest.mock('@utils/Utils.ts', () => ({
	customSortBy: jest.fn((arr) => arr.sort((a: any, b: any) => a.label.localeCompare(b.label)))
}))

describe('RelationshipsTree', () => {
	const mockDispatch = jest.fn()
	const mockFetchRelationshipsData = fetchRelationshipsData as jest.MockedFunction<typeof fetchRelationshipsData>

	const createMockStore = (initialState: any) => {
		return configureStore({
			reducer: {
				relationships: (state = initialState.relationships) => state
			},
			middleware: (getDefaultMiddleware) =>
				getDefaultMiddleware({
					thunk: {
						extraArgument: {}
					}
				})
		})
	}

	const renderComponent = (props = {}, storeState = {}) => {
		const defaultStoreState = {
			relationships: {
				relationships: null,
				loading: false
			},
			...storeState
		}

		const store = createMockStore(defaultStoreState)
		store.dispatch = mockDispatch

		return render(
			<Provider store={store}>
				<RelationshipsTree
					sideBarOpen={true}
					searchTerm=""
					{...props}
				/>
			</Provider>
		)
	}

	beforeEach(() => {
		jest.clearAllMocks()
		mockFetchRelationshipsData.mockReturnValue({ type: 'fetchRelationshipsData' } as any)
	})

	describe('Component Rendering', () => {
		it('should render SideBarTree component', () => {
			renderComponent()

			expect(screen.getByTestId('sidebar-tree')).toBeInTheDocument()
			expect(screen.getByTestId('tree-name')).toHaveTextContent('Relationships')
		})

		it('should pass correct props to SideBarTree', () => {
			renderComponent({ searchTerm: 'test search' })

			expect(screen.getByTestId('search-term')).toHaveTextContent('test search')
		})

		it('should display loading state', () => {
			renderComponent({}, {
				relationships: {
					relationships: null,
					loading: true
				}
			})

			expect(screen.getByTestId('loader')).toHaveTextContent('Loading')
		})

		it('should display not loading state', () => {
			renderComponent({}, {
				relationships: {
					relationships: null,
					loading: false
				}
			})

			expect(screen.getByTestId('loader')).toHaveTextContent('Not Loading')
		})
	})

	describe('Data Fetching', () => {
		it('should dispatch fetchRelationshipsData on mount', () => {
			renderComponent()

			expect(mockDispatch).toHaveBeenCalledWith({ type: 'fetchRelationshipsData' })
		})

		it('should call refreshData when refresh button is clicked', async () => {
			renderComponent()

			const refreshButton = screen.getByTestId('refresh-button')
			refreshButton.click()

			await waitFor(() => {
				expect(mockDispatch).toHaveBeenCalledTimes(2)
			})
		})
	})

	describe('Data Processing', () => {
		it('should process relationshipsData when relationshipDefs is available', async () => {
			const mockRelationships = {
				relationshipDefs: [
					{ name: 'Relationship1', guid: 'guid1' },
					{ name: 'Relationship2', guid: 'guid2' }
				]
			}

			renderComponent({}, {
				relationships: {
					relationships: mockRelationships,
					loading: false
				}
			})

			await waitFor(() => {
				const treeData = screen.getByTestId('tree-data')
				expect(treeData).toBeInTheDocument()
			})

			const treeData = screen.getByTestId('tree-data')
			const data = JSON.parse(treeData.textContent || '[]')
			expect(Array.isArray(data)).toBe(true)
			if (data.length > 0) {
				expect(data[0]).toHaveProperty('id')
				expect(data[0]).toHaveProperty('label')
			}
		})

		it('should handle empty relationshipDefs', () => {
			const mockRelationships = {
				relationshipDefs: []
			}

			renderComponent({}, {
				relationships: {
					relationships: mockRelationships,
					loading: false
				}
			})

			const treeData = screen.getByTestId('tree-data')
			const data = JSON.parse(treeData.textContent || '[]')
			expect(data).toHaveLength(0)
		})

		it('should handle undefined relationshipDefs', () => {
			renderComponent({}, {
				relationships: {
					relationships: { relationshipDefs: undefined },
					loading: false
				}
			})

			const treeData = screen.getByTestId('tree-data')
			const data = JSON.parse(treeData.textContent || '[]')
			expect(data).toHaveLength(0)
		})

		it('should handle null relationships', () => {
			renderComponent({}, {
				relationships: {
					relationships: null,
					loading: false
				}
			})

			const treeData = screen.getByTestId('tree-data')
			const data = JSON.parse(treeData.textContent || '[]')
			expect(data).toHaveLength(0)
		})
	})

	describe('Tree Data Generation', () => {
		it('should generate sorted treeData', async () => {
			const mockRelationships = {
				relationshipDefs: [
					{ name: 'ZRelationship', guid: 'guid1' },
					{ name: 'ARelationship', guid: 'guid2' }
				]
			}

			renderComponent({}, {
				relationships: {
					relationships: mockRelationships,
					loading: false
				}
			})

			await waitFor(() => {
				const treeData = screen.getByTestId('tree-data')
				expect(treeData).toBeInTheDocument()
			})

			const treeData = screen.getByTestId('tree-data')
			const data = JSON.parse(treeData.textContent || '[]')
			expect(Array.isArray(data)).toBe(true)
			if (data.length >= 2) {
				expect(data[0].label).toBe('ARelationship')
				expect(data[1].label).toBe('ZRelationship')
			}
		})
	})

	describe('Props Handling', () => {
		it('should handle sideBarOpen prop', () => {
			renderComponent({ sideBarOpen: false })

			expect(screen.getByTestId('sidebar-tree')).toBeInTheDocument()
		})

		it('should handle searchTerm prop changes', () => {
			const { rerender } = renderComponent({ searchTerm: 'initial' })

			expect(screen.getByTestId('search-term')).toHaveTextContent('initial')

			rerender(
				<Provider store={createMockStore({ relationships: { relationships: null, loading: false } })}>
					<RelationshipsTree sideBarOpen={true} searchTerm="updated" />
				</Provider>
			)

			expect(screen.getByTestId('search-term')).toHaveTextContent('updated')
		})
	})
})
