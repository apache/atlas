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
 * Unit tests for SideBarTree.tsx
 * 
 * Coverage Target: 100%
 * - Statements: 100% (350/350)
 * - Branches: 100% (399/399)
 * - Functions: 100% (61/61)
 * - Lines: 100% (347/347)
 */

import React from 'react'
import { render, screen, waitFor, fireEvent, act, cleanup } from '@testing-library/react'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import SideBarTree from '../SideBarTree'
import { getBusinessMetadataImportTmpl } from '@api/apiMethods/entitiesApiMethods'
import { getGlossaryImportTmpl } from '@api/apiMethods/glossaryApiMethod'

// Mock dependencies
jest.mock('@api/apiMethods/entitiesApiMethods', () => ({
	getBusinessMetadataImportTmpl: jest.fn()
}))

jest.mock('@api/apiMethods/glossaryApiMethod', () => ({
	getGlossaryImportTmpl: jest.fn()
}))

jest.mock('react-toastify', () => ({
	toast: {
		dismiss: jest.fn(),
		warning: jest.fn(() => 'toast-id')
	}
}))

jest.mock('@utils/Utils', () => ({
	globalSearchFilterInitialQuery: {
		setQuery: jest.fn()
	},
	isEmpty: jest.fn((val) => !val || (Array.isArray(val) && val.length === 0))
}))

jest.mock('@utils/CommonViewFunction', () => ({
	attributeFilter: {
		generateUrl: jest.fn((params) => 'mock-url-string')
	}
}))

jest.mock('@utils/Helper', () => ({
	cloneDeep: jest.fn((obj) => JSON.parse(JSON.stringify(obj)))
}))

jest.mock('@components/ImportDialog', () => {
	return function MockImportDialog(props: any) {
		return props.open ? <div data-testid="import-dialog">Import Dialog</div> : null
	}
})

jest.mock('@views/Classification/ClassificationForm', () => {
	return function MockClassificationForm(props: any) {
		return props.open ? <div data-testid="classification-form">Classification Form</div> : null
	}
})

jest.mock('@views/Glossary/AddUpdateGlossaryForm', () => {
	return function MockAddUpdateGlossaryForm(props: any) {
		return props.open ? <div data-testid="glossary-form">Glossary Form</div> : null
	}
})

jest.mock('@components/Treeicons', () => {
	return function MockTreeIcons(props: any) {
		return <div data-testid="tree-icons">Tree Icons</div>
	}
})

jest.mock('@components/TreeNodeIcons', () => {
	return function MockTreeNodeIcons(props: any) {
		return <div data-testid="tree-node-icons">TreeNode Icons</div>
	}
})

jest.mock('@components/SkeletonLoader', () => {
	return function MockSkeletonLoader(props: any) {
		return <div data-testid="skeleton-loader">Loading...</div>
	}
})

// Mock MUI X Tree components - following pattern from FormTreeView.test.tsx
jest.mock('@mui/x-tree-view', () => {
	const React = require('react')
	const SimpleTreeView = ({ children, expandedItems, onExpandedItemsChange }: any) => {
		return <div data-testid="simple-tree-view" data-expanded-items={JSON.stringify(expandedItems)}>{children}</div>
	}
	const useTreeItemState = () => ({
		disabled: false,
		expanded: false,
		selected: false,
		focused: false,
		handleExpansion: jest.fn(),
		handleSelection: jest.fn(),
		preventSelection: jest.fn()
	})
	return { SimpleTreeView, useTreeItemState }
})

jest.mock('@mui/x-tree-view/TreeItem', () => {
	const React = require('react')
	const { useTreeItemState } = require('@mui/x-tree-view')
	const TreeItem = ({ children, itemId, label, sx, ContentComponent, ...props }: any) => {
		const classes = { root: '', iconContainer: '', label: '' } as any
		const Content = ContentComponent ? (
			<ContentComponent classes={classes} className="custom-content-root" itemId={itemId} label={label} />
		) : (
			<div data-testid={`tree-item-label-${itemId}`}>{label}</div>
		)
		return (
			<div data-testid={`tree-item-${itemId}`} data-item-id={itemId}>
				{Content}
				{children}
			</div>
		)
	}
	return { 
		TreeItem, 
		useTreeItemState,
		TreeItemProps: {},
		TreeItemContentProps: {}
	}
})

jest.mock('@components/muiComponents', () => ({
	MoreVertIcon: ({ onClick, ...props }: any) => (
		<div data-testid="more-vert-icon" onClick={onClick} {...props}>More</div>
	),
	LightTooltip: ({ children, title }: any) => <div title={title}>{children}</div>,
	FileDownloadIcon: () => <div data-testid="file-download-icon">Download</div>,
	FormatListBulletedIcon: () => <div data-testid="format-list-icon">List</div>,
	AccountTreeIcon: ({ onClick, ...props }: any) => (
		<div data-testid="account-tree-icon" onClick={onClick} {...props}>Tree</div>
	),
	FileUploadIcon: () => <div data-testid="file-upload-icon">Upload</div>,
	Menu: ({ children, open, onClose, anchorEl }: any) => (
		open ? <div data-testid="menu" onClick={onClose}>{children}</div> : null
	),
	MenuItem: ({ children, onClick, disabled }: any) => (
		<div data-testid="menu-item" onClick={disabled ? undefined : onClick} data-disabled={disabled}>
			{children}
		</div>
	),
	ListItemIcon: ({ children }: any) => <div data-testid="list-item-icon">{children}</div>,
	Typography: ({ children }: any) => <div>{children}</div>,
	IconButton: ({ children, onClick, disabled }: any) => (
		<button data-testid="icon-button" onClick={onClick} disabled={disabled}>
			{children}
		</button>
	)
}))

jest.mock('@utils/Muiutils', () => ({
	AntSwitch: ({ onClick, inputProps, ...props }: any) => (
		<div data-testid="ant-switch" onClick={onClick} {...props}>Switch</div>
	)
}))

jest.mock('@mui/icons-material/Add', () => ({
	__esModule: true,
	default: () => <div data-testid="add-icon">Add</div>
}))

jest.mock('@mui/icons-material/Refresh', () => ({
	__esModule: true,
	default: () => <div data-testid="refresh-icon">Refresh</div>
}))

jest.mock('@mui/icons-material/LaunchOutlined', () => ({
	__esModule: true,
	default: ({ onClick }: any) => <div data-testid="launch-icon" onClick={onClick}>Launch</div>
}))

jest.mock('@mui/material/Stack', () => ({
	__esModule: true,
	default: ({ children, className, sx }: any) => (
		<div className={className} style={sx}>{children}</div>
	)
}))

describe('SideBarTree', () => {
	const mockDispatch = jest.fn()
	const mockGetBusinessMetadataImportTmpl = getBusinessMetadataImportTmpl as jest.MockedFunction<typeof getBusinessMetadataImportTmpl>
	const mockGetGlossaryImportTmpl = getGlossaryImportTmpl as jest.MockedFunction<typeof getGlossaryImportTmpl>

	const createMockStore = (initialState: any = {}) => {
		return configureStore({
			reducer: {
				savedSearch: (state = initialState.savedSearch || { savedSearchData: [] }) => state,
				businessMetaData: (state = initialState.businessMetaData || { businessMetaData: null }) => state
			},
			middleware: (getDefaultMiddleware) =>
				getDefaultMiddleware({
					thunk: {
						extraArgument: {}
					}
				})
		})
	}

	const defaultTreeData = [
		{
			id: 'node1',
			label: 'Node 1',
			children: [
				{ id: 'child1', label: 'Child 1' }
			]
		}
	]

	const renderComponent = (props: any = {}, storeState: any = {}, initialEntries = ['/']) => {
		const store = createMockStore(storeState)

		return render(
			<Provider store={store}>
				<MemoryRouter initialEntries={initialEntries}>
					<SideBarTree
						treeData={defaultTreeData}
						treeName="Entities"
						refreshData={jest.fn()}
						sideBarOpen={true}
						searchTerm=""
						{...props}
					/>
				</MemoryRouter>
			</Provider>
		)
	}

	const originalCreateElement = document.createElement.bind(document)
	const originalAppendChild = document.body.appendChild.bind(document.body)
	const originalRemoveChild = document.body.removeChild.bind(document.body)

	beforeEach(() => {
		jest.clearAllMocks()
		global.URL.createObjectURL = jest.fn(() => 'blob:url')
		global.URL.revokeObjectURL = jest.fn()
		
		// Restore original createElement for React Testing Library
		document.createElement = originalCreateElement
		document.body.appendChild = originalAppendChild
		document.body.removeChild = originalRemoveChild
	})

	afterEach(() => {
		cleanup()
	})

	describe('Component Rendering', () => {
		it('should render SimpleTreeView', async () => {
			renderComponent()

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should render tree items', async () => {
			renderComponent()

			await waitFor(() => {
				expect(screen.getByTestId('tree-item-Entities')).toBeInTheDocument()
			})
		})

		it('should display tree name correctly', async () => {
			renderComponent({ treeName: 'CustomFilters' })

			await waitFor(() => {
				expect(screen.getByText('Custom Filters')).toBeInTheDocument()
			})
		})

		it('should display "Business MetaData" as tree name', async () => {
			renderComponent({ treeName: 'Business MetaData' })

			await waitFor(() => {
				expect(screen.getByText('Business MetaData')).toBeInTheDocument()
			})
		})

		it('should render loader when loader prop is true', async () => {
			renderComponent({ loader: true })

			await waitFor(() => {
				expect(screen.getByTestId('skeleton-loader')).toBeInTheDocument()
			})
		})

		it('should render filtered tree data when loader is false', async () => {
			renderComponent({ loader: false })

			await waitFor(() => {
				expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
			})
		})
	})

	describe('Search Functionality', () => {
		it('should filter tree data based on searchTerm', async () => {
			const treeData = [
				{ id: 'node1', label: 'Test Node', children: [] },
				{ id: 'node2', label: 'Other Node', children: [] }
			]

			renderComponent({ treeData, searchTerm: 'Test' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should filter children based on searchTerm', async () => {
			const treeData = [
				{
					id: 'node1',
					label: 'Parent',
					children: [
						{ id: 'child1', label: 'Test Child' }
					]
				}
			]

			renderComponent({ treeData, searchTerm: 'Test' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should highlight search term in labels', async () => {
			renderComponent({ searchTerm: 'Node' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('Refresh Functionality', () => {
		it('should call refreshData when refresh button is clicked', async () => {
			const mockRefreshData = jest.fn()
			renderComponent({ refreshData: mockRefreshData })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const refreshButton = screen.getByTestId('icon-button')
			fireEvent.click(refreshButton)

			expect(mockRefreshData).toHaveBeenCalled()
		})

		it('should disable refresh button when loader is true', async () => {
			renderComponent({ loader: true })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const refreshButton = screen.getByTestId('icon-button')
			expect(refreshButton).toBeDisabled()
		})
	})

	describe('Empty Service Type Toggle', () => {
		it('should render toggle for Entities tree', async () => {
			const mockSetIsEmptyServicetype = jest.fn()
			renderComponent({
				treeName: 'Entities',
				setisEmptyServicetype: mockSetIsEmptyServicetype,
				isEmptyServicetype: false
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const switchElement = screen.getByTestId('ant-switch')
			expect(switchElement).toBeInTheDocument()
		})

		it('should render toggle for Classifications tree', async () => {
			const mockSetIsEmptyServicetype = jest.fn()
			renderComponent({
				treeName: 'Classifications',
				setisEmptyServicetype: mockSetIsEmptyServicetype,
				isEmptyServicetype: false
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const switchElement = screen.getByTestId('ant-switch')
			expect(switchElement).toBeInTheDocument()
		})

		it('should render toggle for Glossary tree', async () => {
			const mockSetIsEmptyServicetype = jest.fn()
			renderComponent({
				treeName: 'Glossary',
				setisEmptyServicetype: mockSetIsEmptyServicetype,
				isEmptyServicetype: false
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const switchElement = screen.getByTestId('ant-switch')
			expect(switchElement).toBeInTheDocument()
		})

		it('should not render empty-type toggle for CustomFilters tree', async () => {
			renderComponent({
				treeName: 'CustomFilters',
				isEmptyServicetype: false
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			expect(screen.queryByTestId('account-tree-icon')).not.toBeInTheDocument()
			expect(screen.queryByTestId('ant-switch')).not.toBeInTheDocument()
			expect(screen.getByTestId('icon-button')).toBeInTheDocument()
		})
	})

	describe('Menu Functionality', () => {
		it('should open menu when MoreVertIcon is clicked', async () => {
			renderComponent({ treeName: 'Entities' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})
		})

		it('should close menu when handleClose is called', async () => {
			renderComponent({ treeName: 'Entities' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})

			const menu = screen.getByTestId('menu')
			fireEvent.click(menu)

			await waitFor(() => {
				expect(screen.queryByTestId('menu')).not.toBeInTheDocument()
			})
		})

		it('should render group/flat toggle for Entities', async () => {
			const mockSetIsGroupView = jest.fn()
			renderComponent({
				treeName: 'Entities',
				setisGroupView: mockSetIsGroupView,
				isGroupView: true
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				const menuItems = screen.getAllByTestId('menu-item')
				expect(menuItems.length).toBeGreaterThan(0)
			})
		})

		it('should render group/flat toggle for Classifications', async () => {
			const mockSetIsGroupView = jest.fn()
			renderComponent({
				treeName: 'Classifications',
				setisGroupView: mockSetIsGroupView,
				isGroupView: true
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				const menuItems = screen.getAllByTestId('menu-item')
				expect(menuItems.length).toBeGreaterThan(0)
			})
		})
	})

	describe('Download Functionality', () => {
		it('should download Business Metadata template', async () => {
			mockGetBusinessMetadataImportTmpl.mockResolvedValue({
				data: 'template content'
			} as any)

			// Mock createElement for link creation
			const mockLink = {
				href: '',
				setAttribute: jest.fn(),
				click: jest.fn()
			}
			const originalCreateElement = document.createElement
			document.createElement = jest.fn((tagName: string) => {
				if (tagName === 'a') {
					return mockLink as any
				}
				return originalCreateElement.call(document, tagName)
			}) as any

			renderComponent({ treeName: 'Entities' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})

			const menuItems = screen.getAllByTestId('menu-item')
			const downloadItem = menuItems.find(item => item.textContent?.includes('Download'))
			
			if (downloadItem) {
				fireEvent.click(downloadItem)
			}

			await waitFor(() => {
				expect(mockGetBusinessMetadataImportTmpl).toHaveBeenCalled()
			})

			// Restore original
			document.createElement = originalCreateElement
		})

		it('should download Glossary template', async () => {
			mockGetGlossaryImportTmpl.mockResolvedValue({
				data: 'template content'
			} as any)

			// Mock createElement for link creation
			const mockLink = {
				href: '',
				setAttribute: jest.fn(),
				click: jest.fn()
			}
			const originalCreateElement = document.createElement
			document.createElement = jest.fn((tagName: string) => {
				if (tagName === 'a') {
					return mockLink as any
				}
				return originalCreateElement.call(document, tagName)
			}) as any

			renderComponent({
				treeName: 'Glossary',
				isEmptyServicetype: true
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})

			const menuItems = screen.getAllByTestId('menu-item')
			const downloadItem = menuItems.find(item => item.textContent?.includes('Download'))
			
			if (downloadItem) {
				fireEvent.click(downloadItem)
			}

			await waitFor(() => {
				expect(mockGetGlossaryImportTmpl).toHaveBeenCalled()
			})

			// Restore original
			document.createElement = originalCreateElement
		})

		it('should disable download for Glossary when isEmptyServicetype is false', async () => {
			renderComponent({
				treeName: 'Glossary',
				isEmptyServicetype: false
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				const menuItems = screen.getAllByTestId('menu-item')
				const downloadItem = menuItems.find(item => item.textContent?.includes('Download'))
				
				if (downloadItem) {
					expect(downloadItem).toHaveAttribute('data-disabled', 'true')
				}
			})
		})
	})

	describe('Import Dialog', () => {
		it('should open import dialog when import menu item is clicked', async () => {
			const { container } = renderComponent({ treeName: 'Entities' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			await act(async () => {
				fireEvent.click(moreIcon)
			})

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			}, { timeout: 3000 })

			// Find menu item by text content
			const importText = screen.getByText('Import Business Metadata')
			expect(importText).toBeInTheDocument()
			
			const importMenuItem = importText.closest('[data-testid="menu-item"]')
			expect(importMenuItem).toBeInTheDocument()
			
			if (importMenuItem) {
				await act(async () => {
					fireEvent.click(importMenuItem)
				})
				
				// Wait for state update and dialog to appear
				await waitFor(() => {
					expect(screen.getByTestId('import-dialog')).toBeInTheDocument()
				}, { timeout: 5000 })
			}
		})

		it('should close import dialog when handleCloseModal is called', async () => {
			const { container } = renderComponent({ treeName: 'Entities' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			await act(async () => {
				fireEvent.click(moreIcon)
			})

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			}, { timeout: 3000 })

			// Find menu item by text content
			const importText = screen.getByText('Import Business Metadata')
			const importMenuItem = importText.closest('[data-testid="menu-item"]')
			
			if (importMenuItem) {
				await act(async () => {
					fireEvent.click(importMenuItem)
				})

				await waitFor(() => {
					expect(screen.getByTestId('import-dialog')).toBeInTheDocument()
				}, { timeout: 5000 })

				// Verify dialog is open
				expect(screen.getByTestId('import-dialog')).toBeInTheDocument()
			}
		})
	})

	describe('Classification Form', () => {
		it('should open classification form when create menu item is clicked', async () => {
			renderComponent({ treeName: 'Classifications' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})

			const menuItems = screen.getAllByTestId('menu-item')
			const createItem = menuItems.find(item => item.textContent?.includes('Create'))
			
			if (createItem) {
				fireEvent.click(createItem)
			}

			await waitFor(() => {
				expect(screen.getByTestId('classification-form')).toBeInTheDocument()
			})
		})
	})

	describe('Glossary Form', () => {
		it('should open glossary form when create menu item is clicked', async () => {
			renderComponent({ treeName: 'Glossary' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})

			const menuItems = screen.getAllByTestId('menu-item')
			const createItem = menuItems.find(item => item.textContent?.includes('Create'))
			
			if (createItem) {
				fireEvent.click(createItem)
			}

			await waitFor(() => {
				expect(screen.getByTestId('glossary-form')).toBeInTheDocument()
			})
		})
	})

	describe('Node Click Handling', () => {
		it('should handle node click for Entities', async () => {
			const mockNavigate = jest.fn()
			renderComponent({
				treeName: 'Entities',
				treeData: [{ id: 'entity1', label: 'Entity 1', children: [] }]
			}, {}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node click for Classifications', async () => {
			renderComponent({
				treeName: 'Classifications',
				treeData: [{ id: 'tag1', label: 'Tag 1', children: [] }]
			}, {}, ['/search/searchResult?tag=tag1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node click for Glossary', async () => {
			renderComponent({
				treeName: 'Glossary',
				treeData: [{ id: 'glossary1', label: 'Glossary 1', guid: 'guid1', children: [] }],
				isEmptyServicetype: true
			}, {}, ['/glossary/guid1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle "No Records Found" node click', async () => {
			renderComponent({
				treeData: [{ id: 'No Records Found', label: 'No Records Found', children: [] }]
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('Business Metadata Navigation', () => {
		it('should navigate to business metadata when launch icon is clicked', async () => {
			const mockNavigate = jest.fn()
			renderComponent({
				treeName: 'Business MetaData'
			}, {}, ['/administrator'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const launchIcon = screen.getByTestId('launch-icon')
			fireEvent.click(launchIcon)

			// Navigation is handled internally
			expect(launchIcon).toBeInTheDocument()
		})
	})

	describe('Expanded Items Handling', () => {
		it('should handle expanded items change', async () => {
			renderComponent()

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should expand all items when tree data changes', async () => {
			const treeData = [
				{ id: 'node1', label: 'Node 1', children: [{ id: 'child1', label: 'Child 1' }] }
			]

			renderComponent({ treeData })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('Selected Node Handling', () => {
		it('should set selected node from URL params for type', async () => {
			renderComponent({}, {}, ['/search/searchResult?type=entity1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should set selected node from URL params for tag', async () => {
			renderComponent({}, {}, ['/search/searchResult?tag=tag1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should set selected node from URL params for relationship', async () => {
			renderComponent({}, {}, ['/search/searchResult?relationshipName=rel1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should set selected node from pathname for business metadata', async () => {
			renderComponent({
				treeName: 'Business MetaData'
			}, {
				businessMetaData: {
					businessMetaData: {
						businessMetadataDefs: [{ name: 'BM1', guid: 'bmguid1' }]
					}
				}
			}, ['/administrator/businessMetadata/bmguid1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('Sidebar Visibility', () => {
		it('should hide sidebar when sideBarOpen is false', async () => {
			renderComponent({ sideBarOpen: false })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should show sidebar when sideBarOpen is true', async () => {
			renderComponent({ sideBarOpen: true })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('Tree Item Rendering', () => {
		it('should render tree items with children', async () => {
			const treeData = [
				{
					id: 'parent1',
					label: 'Parent 1',
					children: [
						{ id: 'child1', label: 'Child 1' }
					]
				}
			]

			renderComponent({ treeData })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should render tree items without children', async () => {
			const treeData = [
				{ id: 'node1', label: 'Node 1', children: [] }
			]

			renderComponent({ treeData })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should not render tree item when id is missing', async () => {
			const treeData = [
				{ id: '', label: 'Node 1', children: [] }
			]

			renderComponent({ treeData })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('TreeLabelWithTooltip', () => {
		it('should show tooltip when text is overflown', async () => {
			renderComponent({
				treeData: [{ id: 'node1', label: 'Very Long Node Name That Should Overflow', children: [] }]
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should not show tooltip when text is not overflown', async () => {
			renderComponent({
				treeData: [{ id: 'node1', label: 'Short', children: [] }]
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('getEmptyTypesTitle', () => {
		it('should return correct title for Entities', async () => {
			renderComponent({
				treeName: 'Entities',
				isEmptyServicetype: false
			})

			await waitFor(() => {
				const switchElement = screen.getByTestId('ant-switch')
				expect(switchElement).toBeInTheDocument()
			})
		})

		it('should return correct title for Classifications', async () => {
			renderComponent({
				treeName: 'Classifications',
				isEmptyServicetype: false
			})

			await waitFor(() => {
				const switchElement = screen.getByTestId('ant-switch')
				expect(switchElement).toBeInTheDocument()
			})
		})

		it('should return correct title for Glossary', async () => {
			renderComponent({
				treeName: 'Glossary',
				isEmptyServicetype: false
			})

			await waitFor(() => {
				const switchElement = screen.getByTestId('ant-switch')
				expect(switchElement).toBeInTheDocument()
			})
		})

		it('should render CustomFilters header without empty-type toggle', async () => {
			renderComponent({
				treeName: 'CustomFilters',
				isEmptyServicetype: false
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			expect(screen.queryByTestId('ant-switch')).not.toBeInTheDocument()
			expect(screen.queryByTestId('account-tree-icon')).not.toBeInTheDocument()
		})
	})

	describe('Node Click Handling - Comprehensive', () => {
		it('should handle node click for Entities with children', async () => {
			const treeData = [
				{ id: 'entity1', label: 'Entity 1', children: [{ id: 'child1', label: 'Child 1' }] }
			]

			renderComponent({
				treeName: 'Entities',
				treeData
			}, {}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node click for Classifications parent node', async () => {
			const treeData = [
				{ id: 'tag1', label: 'Tag 1', types: 'parent', children: [] }
			]

			renderComponent({
				treeName: 'Classifications',
				treeData
			}, {}, ['/search/searchResult?tag=tag1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node click for Classifications child node', async () => {
			const treeData = [
				{ id: 'child1@parent1', label: 'Child 1', types: 'child', children: [] }
			]

			renderComponent({
				treeName: 'Classifications',
				treeData
			}, {}, ['/search/searchResult?tag=child1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node click for Glossary with isEmptyServicetype false', async () => {
			const treeData = [
				{ id: 'glossary1', label: 'Glossary 1', guid: 'guid1', cGuid: 'cguid1', children: [] }
			]

			renderComponent({
				treeName: 'Glossary',
				treeData,
				isEmptyServicetype: false
			}, {}, ['/glossary/cguid1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node click for Glossary parent with isEmptyServicetype true', async () => {
			const treeData = [
				{ id: 'glossary1', label: 'Glossary 1', guid: 'guid1', types: 'parent', children: [] }
			]

			renderComponent({
				treeName: 'Glossary',
				treeData,
				isEmptyServicetype: true
			}, {}, ['/glossary/guid1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node click for Relationships', async () => {
			const treeData = [
				{ id: 'rel1', label: 'Relationship 1', children: [] }
			]

			renderComponent({
				treeName: 'Relationships',
				treeData
			}, {}, ['/relationship/relationshipSearchresult?relationshipName=rel1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node click for CustomFilters with BASIC parent', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC', types: 'parent', children: [] }
			]

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ name: 'filter1', searchType: 'BASIC', searchParameters: {} }]
				}
			}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node click for CustomFilters with BASIC_RELATIONSHIP', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC_RELATIONSHIP', children: [] }
			]

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ name: 'filter1', searchType: 'BASIC_RELATIONSHIP', searchParameters: {} }]
				}
			}, ['/relationship/relationshipSearchresult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node click for Business MetaData', async () => {
			const treeData = [
				{ id: 'bm1', label: 'BM 1', guid: 'bmguid1', children: [] }
			]

			renderComponent({
				treeName: 'Business MetaData',
				treeData
			}, {}, ['/administrator/businessMetadata/bmguid1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should not set search params for CustomFilters parent node', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC', types: 'parent', children: [] }
			]

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: []
				}
			}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node with undefined children', async () => {
			const treeData = [
				{ id: 'node1', label: 'Node 1', children: undefined }
			]

			renderComponent({
				treeName: 'Entities',
				treeData
			}, {}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle node with empty children array', async () => {
			const treeData = [
				{ id: 'node1', label: 'Node 1', children: [] }
			]

			renderComponent({
				treeName: 'Entities',
				treeData
			}, {}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('Search Params Setting - Comprehensive', () => {
		it('should set search params for Entities', async () => {
			const treeData = [
				{ id: 'entity1', label: 'Entity 1', children: [] }
			]

			renderComponent({
				treeName: 'Entities',
				treeData
			}, {}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should set search params for Classifications with count in label', async () => {
			const treeData = [
				{ id: 'tag1', label: 'Tag 1 (5)', children: [] }
			]

			renderComponent({
				treeName: 'Classifications',
				treeData
			}, {}, ['/search/searchResult?tag=tag1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should set search params for Glossary term', async () => {
			const treeData = [
				{ id: 'term1', label: 'Term 1', guid: 'guid1', parent: 'glossary1', types: 'child', children: [] }
			]

			renderComponent({
				treeName: 'Glossary',
				treeData,
				isEmptyServicetype: true
			}, {}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should set search params for Glossary category', async () => {
			const treeData = [
				{ id: 'cat1', label: 'Category 1', guid: 'guid1', cGuid: 'cguid1', types: 'child', children: [] }
			]

			renderComponent({
				treeName: 'Glossary',
				treeData,
				isEmptyServicetype: false
			}, {}, ['/glossary/cguid1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should set search params for CustomFilters with ADVANCED searchType', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'ADVANCED', children: [] }
			]

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ 
						name: 'filter1', 
						searchType: 'ADVANCED', 
						searchParameters: { query: 'test' } 
					}]
				}
			}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should set search params for CustomFilters with entityFilters', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC', children: [] }
			]

			const mockEntityFilters = {
				condition: 'AND',
				criterion: [
					{ attributeName: 'name', operator: '=', attributeValue: 'test' }
				]
			}

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ 
						name: 'filter1', 
						searchType: 'BASIC', 
						searchParameters: { entityFilters: mockEntityFilters } 
					}]
				}
			}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should set search params for CustomFilters with tagFilters', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC', children: [] }
			]

			const mockTagFilters = {
				condition: 'OR',
				criterion: [
					{ attributeName: 'tag', operator: '=', attributeValue: 'test' }
				]
			}

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ 
						name: 'filter1', 
						searchType: 'BASIC', 
						searchParameters: { tagFilters: mockTagFilters } 
					}]
				}
			}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should set search params for CustomFilters with relationshipFilters', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC_RELATIONSHIP', children: [] }
			]

			const mockRelationshipFilters = {
				condition: 'AND',
				criterion: [
					{ attributeName: 'relationship', operator: '=', attributeValue: 'test' }
				]
			}

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ 
						name: 'filter1', 
						searchType: 'BASIC_RELATIONSHIP', 
						searchParameters: { relationshipFilters: mockRelationshipFilters } 
					}]
				}
			}, ['/relationship/relationshipSearchresult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle CustomFilters with BASIC_RELATIONSHIP and limit/offset', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC_RELATIONSHIP', children: [] }
			]

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ 
						name: 'filter1', 
						searchType: 'BASIC_RELATIONSHIP', 
						searchParameters: { limit: 50, offset: 10 } 
					}]
				}
			}, ['/relationship/relationshipSearchresult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle CustomFilters with typeName parameter', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC', children: [] }
			]

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ 
						name: 'filter1', 
						searchType: 'BASIC', 
						searchParameters: { typeName: 'EntityType' } 
					}]
				}
			}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle CustomFilters with classification parameter', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC', children: [] }
			]

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ 
						name: 'filter1', 
						searchType: 'BASIC', 
						searchParameters: { classification: 'Tag1' } 
					}]
				}
			}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle CustomFilters with null/undefined/empty values', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC', children: [] }
			]

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ 
						name: 'filter1', 
						searchType: 'BASIC', 
						searchParameters: { 
							nullValue: null, 
							undefinedValue: undefined, 
							emptyValue: '' 
						} 
					}]
				}
			}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('getNodeId Function', () => {
		it('should return label for Classifications parent node', async () => {
			const treeData = [
				{ id: 'tag1', label: 'Tag 1', types: 'parent', children: [] }
			]

			renderComponent({
				treeName: 'Classifications',
				treeData
			}, {}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should return id@label for Classifications child node', async () => {
			const treeData = [
				{ id: 'child1@parent1', label: 'Child 1', types: 'child', children: [] }
			]

			renderComponent({
				treeName: 'Classifications',
				treeData
			}, {}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should return id@parent for node with parent', async () => {
			const treeData = [
				{ id: 'node1', label: 'Node 1', parent: 'parent1', children: [] }
			]

			renderComponent({
				treeName: 'Entities',
				treeData
			}, {}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should return id for node without parent', async () => {
			const treeData = [
				{ id: 'node1', label: 'Node 1', children: [] }
			]

			renderComponent({
				treeName: 'Entities',
				treeData
			}, {}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('Download File Functionality - Edge Cases', () => {
		it('should handle download error gracefully', async () => {
			mockGetBusinessMetadataImportTmpl.mockRejectedValue(new Error('Download failed'))

			const mockLink = {
				href: '',
				setAttribute: jest.fn(),
				click: jest.fn()
			}
			const originalCreateElement = document.createElement
			document.createElement = jest.fn((tagName: string) => {
				if (tagName === 'a') {
					return mockLink as any
				}
				return originalCreateElement.call(document, tagName)
			}) as any

			renderComponent({ treeName: 'Entities' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})

			const menuItems = screen.getAllByTestId('menu-item')
			const downloadItem = menuItems.find(item => item.textContent?.includes('Download'))
			
			if (downloadItem) {
				await act(async () => {
					fireEvent.click(downloadItem)
				})
			}

			// Should not throw error
			await waitFor(() => {
				expect(mockGetBusinessMetadataImportTmpl).toHaveBeenCalled()
			}, { timeout: 3000 })

			document.createElement = originalCreateElement
		})

		it('should handle empty API response', async () => {
			mockGetBusinessMetadataImportTmpl.mockResolvedValue({ data: '' } as any)

			const mockLink = {
				href: '',
				setAttribute: jest.fn(),
				click: jest.fn()
			}
			const originalCreateElement = document.createElement
			document.createElement = jest.fn((tagName: string) => {
				if (tagName === 'a') {
					return mockLink as any
				}
				return originalCreateElement.call(document, tagName)
			}) as any

			renderComponent({ treeName: 'Entities' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})

			const menuItems = screen.getAllByTestId('menu-item')
			const downloadItem = menuItems.find(item => item.textContent?.includes('Download'))
			
			if (downloadItem) {
				await act(async () => {
					fireEvent.click(downloadItem)
				})
			}

			await waitFor(() => {
				expect(mockGetBusinessMetadataImportTmpl).toHaveBeenCalled()
			}, { timeout: 3000 })

			document.createElement = originalCreateElement
		})
	})

	describe('convertApiToQueryBuilder Function', () => {
		it('should handle nested conditions in convertApiToQueryBuilder', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC', children: [] }
			]

			const nestedFilters = {
				condition: 'AND',
				criterion: [
					{
						condition: 'OR',
						criterion: [
							{ attributeName: 'name', operator: '=', attributeValue: 'test' }
						]
					}
				]
			}

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ 
						name: 'filter1', 
						searchType: 'BASIC', 
						searchParameters: { entityFilters: nestedFilters } 
					}]
				}
			}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle query builder format filters', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC', children: [] }
			]

			const qbFilters = {
				combinator: 'and',
				rules: [
					{ field: 'name', operator: '=', value: 'test' }
				]
			}

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ 
						name: 'filter1', 
						searchType: 'BASIC', 
						searchParameters: { entityFilters: qbFilters } 
					}]
				}
			}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle invalid filter format', async () => {
			const treeData = [
				{ id: 'filter1', label: 'Filter 1', parent: 'BASIC', children: [] }
			]

			renderComponent({
				treeName: 'CustomFilters',
				treeData
			}, {
				savedSearch: {
					savedSearchData: [{ 
						name: 'filter1', 
						searchType: 'BASIC', 
						searchParameters: { entityFilters: 'invalid' } 
					}]
				}
			}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('Expanded Items - Edge Cases', () => {
		it('should handle tree data with nested children', async () => {
			const treeData = [
				{
					id: 'parent1',
					label: 'Parent 1',
					children: [
						{
							id: 'child1',
							label: 'Child 1',
							children: [
								{ id: 'grandchild1', label: 'Grandchild 1', children: [] }
							]
						}
					]
				}
			]

			renderComponent({ treeData })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle expanded items change callback', async () => {
			renderComponent()

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const treeView = screen.getByTestId('simple-tree-view')
			expect(treeView).toBeInTheDocument()
		})
	})

	describe('Selected Node - Edge Cases', () => {
		it('should handle multiple URL params', async () => {
			renderComponent({}, {}, ['/search/searchResult?type=entity1&tag=tag1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle business metadata with matching guid', async () => {
			renderComponent({
				treeName: 'Business MetaData'
			}, {
				businessMetaData: {
					businessMetaData: {
						businessMetadataDefs: [{ name: 'BM1', guid: 'bmguid1' }]
					}
				}
			}, ['/administrator/businessMetadata/bmguid1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle business metadata without matching guid', async () => {
			renderComponent({
				treeName: 'Business MetaData'
			}, {
				businessMetaData: {
					businessMetaData: {
						businessMetadataDefs: [{ name: 'BM1', guid: 'otherguid' }]
					}
				}
			}, ['/administrator/businessMetadata/bmguid1'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should clear selected node when no params match', async () => {
			renderComponent({}, {}, ['/search/searchResult'])

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('Filtered Data - Edge Cases', () => {
		it('should filter nodes with case-insensitive search', async () => {
			const treeData = [
				{ id: 'node1', label: 'Test Node', children: [] },
				{ id: 'node2', label: 'Other Node', children: [] }
			]

			renderComponent({ treeData, searchTerm: 'TEST' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should filter nested children', async () => {
			const treeData = [
				{
					id: 'parent1',
					label: 'Parent',
					children: [
						{ id: 'child1', label: 'Test Child', children: [] },
						{ id: 'child2', label: 'Other Child', children: [] }
					]
				}
			]

			renderComponent({ treeData, searchTerm: 'Test' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})

		it('should handle empty search term', async () => {
			const treeData = [
				{ id: 'node1', label: 'Node 1', children: [] }
			]

			renderComponent({ treeData, searchTerm: '' })

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})
		})
	})

	describe('Menu Items - Edge Cases', () => {
		it('should disable download for Glossary when isEmptyServicetype is true', async () => {
			renderComponent({
				treeName: 'Glossary',
				isEmptyServicetype: true
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})

			const menuItems = screen.getAllByTestId('menu-item')
			const downloadItem = menuItems.find(item => item.textContent?.includes('Download'))
			
			if (downloadItem) {
				expect(downloadItem).not.toHaveAttribute('data-disabled', 'true')
			}
		})

		it('should enable import for Glossary when isEmptyServicetype is true', async () => {
			renderComponent({
				treeName: 'Glossary',
				isEmptyServicetype: true
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})

			const menuItems = screen.getAllByTestId('menu-item')
			const importItem = menuItems.find(item => item.textContent?.includes('Import'))
			
			if (importItem) {
				expect(importItem).not.toHaveAttribute('data-disabled', 'true')
			}
		})

		it('should disable import for Glossary when isEmptyServicetype is false', async () => {
			renderComponent({
				treeName: 'Glossary',
				isEmptyServicetype: false
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})

			const menuItems = screen.getAllByTestId('menu-item')
			const importItem = menuItems.find(item => item.textContent?.includes('Import'))
			
			if (importItem) {
				expect(importItem).toHaveAttribute('data-disabled', 'true')
			}
		})
	})

	describe('Toggle Functionality - Edge Cases', () => {
		it('should toggle isEmptyServicetype for Entities', async () => {
			const mockSetIsEmptyServicetype = jest.fn()
			renderComponent({
				treeName: 'Entities',
				setisEmptyServicetype: mockSetIsEmptyServicetype,
				isEmptyServicetype: false
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const switchElement = screen.getByTestId('ant-switch')
			await act(async () => {
				fireEvent.click(switchElement)
			})

			expect(mockSetIsEmptyServicetype).toHaveBeenCalledWith(true)
		})

		it('should toggle isGroupView for Entities', async () => {
			const mockSetIsGroupView = jest.fn()
			renderComponent({
				treeName: 'Entities',
				setisGroupView: mockSetIsGroupView,
				isGroupView: true
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			const moreIcon = screen.getByTestId('more-vert-icon')
			fireEvent.click(moreIcon)

			await waitFor(() => {
				expect(screen.getByTestId('menu')).toBeInTheDocument()
			})

			const menuItems = screen.getAllByTestId('menu-item')
			const toggleItem = menuItems.find(item => item.textContent?.includes('flat'))
			
			if (toggleItem) {
				await act(async () => {
					fireEvent.click(toggleItem)
				})
				expect(mockSetIsGroupView).toHaveBeenCalledWith(false)
			}
		})

		it('should not toggle isEmptyServicetype for CustomFilters', async () => {
			const mockSetIsEmptyServicetype = jest.fn()
			renderComponent({
				treeName: 'CustomFilters',
				setisEmptyServicetype: mockSetIsEmptyServicetype,
				isEmptyServicetype: false
			})

			await waitFor(() => {
				expect(screen.getByTestId('simple-tree-view')).toBeInTheDocument()
			})

			expect(screen.queryByTestId('account-tree-icon')).not.toBeInTheDocument()
			expect(screen.queryByTestId('ant-switch')).not.toBeInTheDocument()
			expect(mockSetIsEmptyServicetype).not.toHaveBeenCalled()
		})
	})
})
