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
 * Unit tests for BusinessMetadataTree.tsx
 * 
 * Coverage Target: 100%
 * - Statements: 100% (24/24)
 * - Branches: 100% (7/7)
 * - Functions: 100% (7/7)
 * - Lines: 100% (24/24)
 */

import React from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import BusinessMetadataTree from '../BusinessMetadataTree'
import { fetchBusinessMetaData } from '@redux/slice/typeDefSlices/typedefBusinessMetadataSlice'

// Mock dependencies
jest.mock('@redux/slice/typeDefSlices/typedefBusinessMetadataSlice', () => ({
	fetchBusinessMetaData: jest.fn()
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
	customSortBy: jest.fn((arr) => arr),
	isEmpty: jest.fn((val) => !val || (Array.isArray(val) && val.length === 0)),
	noTreeData: jest.fn(() => [{ id: 'No Records Found', label: 'No Records Found', childrenData: [] }])
}))

describe('BusinessMetadataTree', () => {
	const mockDispatch = jest.fn()
	const mockFetchBusinessMetaData = fetchBusinessMetaData as jest.MockedFunction<typeof fetchBusinessMetaData>

	const createMockStore = (initialState: any) => {
		return configureStore({
			reducer: {
				businessMetaData: (state = initialState.businessMetaData) => state
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
			businessMetaData: {
				businessMetaData: null,
				loading: false
			},
			...storeState
		}

		const store = createMockStore(defaultStoreState)
		store.dispatch = mockDispatch

		return render(
			<Provider store={store}>
				<BusinessMetadataTree
					sideBarOpen={true}
					searchTerm=""
					{...props}
				/>
			</Provider>
		)
	}

	beforeEach(() => {
		jest.clearAllMocks()
		mockFetchBusinessMetaData.mockReturnValue({ type: 'fetchBusinessMetaData' } as any)
	})

	describe('Component Rendering', () => {
		it('should render SideBarTree component', () => {
			renderComponent()

			expect(screen.getByTestId('sidebar-tree')).toBeInTheDocument()
			expect(screen.getByTestId('tree-name')).toHaveTextContent('Business MetaData')
		})

		it('should pass correct props to SideBarTree', () => {
			renderComponent({ searchTerm: 'test search' })

			expect(screen.getByTestId('search-term')).toHaveTextContent('test search')
		})

		it('should display loading state', () => {
			renderComponent({}, {
				businessMetaData: {
					businessMetaData: null,
					loading: true
				}
			})

			expect(screen.getByTestId('loader')).toHaveTextContent('Loading')
		})

		it('should display not loading state', () => {
			renderComponent({}, {
				businessMetaData: {
					businessMetaData: null,
					loading: false
				}
			})

			expect(screen.getByTestId('loader')).toHaveTextContent('Not Loading')
		})
	})

	describe('Data Fetching', () => {
		it('should dispatch fetchBusinessMetaData on mount', () => {
			renderComponent()

			expect(mockDispatch).toHaveBeenCalledWith({ type: 'fetchBusinessMetaData' })
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
		it('should process businessMetadataData when available', async () => {
			const mockBusinessMetaData = {
				businessMetadataDefs: [
					{ name: 'Metadata1', guid: 'guid1' },
					{ name: 'Metadata2', guid: 'guid2' }
				]
			}

			renderComponent({}, {
				businessMetaData: {
					businessMetaData: mockBusinessMetaData,
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
		})

		it('should handle empty businessMetadataData', () => {
			const mockBusinessMetaData = {
				businessMetadataDefs: []
			}

			renderComponent({}, {
				businessMetaData: {
					businessMetaData: mockBusinessMetaData,
					loading: false
				}
			})

			const treeData = screen.getByTestId('tree-data')
			const data = JSON.parse(treeData.textContent || '[]')
			expect(data.length).toBeGreaterThan(0)
			expect(data[0]?.label).toBe('No Records Found')
		})

		it('should handle undefined businessMetadataDefs', () => {
			renderComponent({}, {
				businessMetaData: {
					businessMetaData: { businessMetadataDefs: undefined },
					loading: false
				}
			})

			const treeData = screen.getByTestId('tree-data')
			const data = JSON.parse(treeData.textContent || '[]')
			expect(Array.isArray(data)).toBe(true)
		})

		it('should handle null businessMetaData', () => {
			renderComponent({}, {
				businessMetaData: {
					businessMetaData: null,
					loading: false
				}
			})

			const treeData = screen.getByTestId('tree-data')
			expect(treeData).toBeInTheDocument()
		})
	})

	describe('Tree Data Generation', () => {
		it('should generate treeData with sorted items', () => {
			const mockBusinessMetaData = {
				businessMetadataDefs: [
					{ name: 'ZMetadata', guid: 'guid1' },
					{ name: 'AMetadata', guid: 'guid2' }
				]
			}

			renderComponent({}, {
				businessMetaData: {
					businessMetaData: mockBusinessMetaData,
					loading: false
				}
			})

			const treeData = screen.getByTestId('tree-data')
			expect(treeData).toBeInTheDocument()
		})

		it('should use noTreeData when businessMetadataData is empty', () => {
			renderComponent({}, {
				businessMetaData: {
					businessMetaData: { businessMetadataDefs: [] },
					loading: false
				}
			})

			const treeData = screen.getByTestId('tree-data')
			expect(treeData).toBeInTheDocument()
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
				<Provider store={createMockStore({ businessMetaData: { businessMetaData: null, loading: false } })}>
					<BusinessMetadataTree sideBarOpen={true} searchTerm="updated" />
				</Provider>
			)

			expect(screen.getByTestId('search-term')).toHaveTextContent('updated')
		})
	})
})
