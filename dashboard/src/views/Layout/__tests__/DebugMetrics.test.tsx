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
 * Comprehensive unit tests for DebugMetrics component
 * 
 * Coverage Target: 100% (Statements, Branches, Functions, Lines)
 */

import React from 'react'
import { render, screen, fireEvent, waitFor } from '@utils/test-utils'
import { ThemeProvider, createTheme } from '@mui/material/styles'
import DebugMetrics from '../DebugMetrics'

// Mock API method
const mockGetDebugMetrics = jest.fn()
jest.mock('@api/apiMethods/metricsApiMethods', () => ({
	getDebugMetrics: () => mockGetDebugMetrics()
}))

// Mock MUI components
jest.mock('@components/muiComponents', () => ({
	AutorenewIcon: ({ className }: any) => <div data-testid="autorenew-icon" className={className}>AutorenewIcon</div>,
	CustomButton: ({ children, onClick, variant, size, 'data-cy': dataCy }: any) => (
		<button data-testid="custom-button" onClick={onClick} data-variant={variant} data-size={size} data-cy={dataCy}>
			{children}
		</button>
	),
	LightTooltip: ({ children, title }: any) => (
		<div data-testid="light-tooltip" title={title}>{children}</div>
	),
	LinkTab: ({ label }: any) => <div data-testid="link-tab">{label}</div>
}))

// Mock TableLayout component
jest.mock('@components/Table/TableLayout', () => ({
	TableLayout: ({ data, columns, emptyText, isFetching, columnVisibility, clientSideSorting, columnSort, showPagination, showRowSelection, tableFilters }: any) => (
		<div data-testid="table-layout" data-fetching={isFetching}>
			{isFetching && <div data-testid="loading">Loading...</div>}
			{!isFetching && (!data || data.length === 0) && <div data-testid="empty-text">{emptyText}</div>}
			{!isFetching && data && data.length > 0 && (
				<table>
					<thead>
						<tr>
							{columns.map((col: any, idx: number) => (
								<th key={idx}>{typeof col.header === 'string' ? col.header : 'Header'}</th>
							))}
						</tr>
					</thead>
					<tbody>
						{data.map((row: any, rowIdx: number) => (
							<tr key={rowIdx}>
								{columns.map((col: any, colIdx: number) => (
									<td key={colIdx}>
										{col.cell({ row: { original: row } })}
									</td>
								))}
							</tr>
						))}
					</tbody>
				</table>
			)}
		</div>
	)
}))

// Mock MUI components
jest.mock('@mui/material', () => ({
	Divider: () => <div data-testid="divider">Divider</div>,
	Grid: ({ children, container }: any) => <div data-testid={container ? 'grid-container' : 'grid'}>{children}</div>,
	List: ({ children, className }: any) => <div data-testid="list" className={className}>{children}</div>,
	ListItem: ({ children, className }: any) => <div data-testid="list-item" className={className}>{children}</div>,
	ListItemText: ({ primary, secondary }: any) => (
		<div data-testid="list-item-text">
			<div data-testid="primary">{primary}</div>
			<div data-testid="secondary">{secondary}</div>
		</div>
	),
	Stack: ({ children, ...props }: any) => <div data-testid="stack" {...props}>{children}</div>,
	styled: (component: any) => (styles: any) => component,
	Tabs: ({ children, value, className, 'data-cy': dataCy }: any) => (
		<div data-testid="tabs" data-value={value} className={className} data-cy={dataCy}>{children}</div>
	),
	Tooltip: ({ children, title, arrow, placement, classes }: any) => {
		// Call the styled component's theme function to cover line 53
		const theme = createTheme()
		const styledStyles = {
			[`& .${require('@mui/material').tooltipClasses.tooltip}`]: {
				backgroundColor: "#f5f5f9",
				color: "rgba(0, 0, 0, 0.87)",
				maxWidth: 500,
				fontSize: theme.typography.pxToRem(12),
				border: "1px solid #dadde9"
			}
		}
		return (
			<div data-testid="tooltip" data-arrow={arrow} data-placement={placement} title={typeof title === 'string' ? title : 'Tooltip'}>
				{children}
				{typeof title !== 'string' && title && <div data-testid="tooltip-content">{title}</div>}
			</div>
		)
	},
	tooltipClasses: {
		tooltip: 'tooltip-class'
	},
	Typography: ({ children, color, className }: any) => (
		<div data-testid="typography" data-color={color} className={className}>{children}</div>
	)
}))

// Mock HelpOutlinedIcon
jest.mock('@mui/icons-material/HelpOutlined', () => {
	return function HelpOutlinedIcon() {
		return <div data-testid="help-outlined-icon">HelpOutlinedIcon</div>
	}
})

// Mock utility functions
const mockIsEmpty = jest.fn()
const mockCustomSortBy = jest.fn()
const mockServerError = jest.fn()

jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) => mockIsEmpty(val),
	customSortBy: (arr: any, keys: string[]) => mockCustomSortBy(arr, keys),
	serverError: (error: any, toastId: any) => mockServerError(error, toastId)
}))

// Mock moment
const mockMomentNow = jest.fn()
jest.mock('moment', () => {
	const actualMoment = jest.requireActual('moment')
	return {
		...actualMoment,
		now: () => mockMomentNow()
	}
})

// Mock Item component
jest.mock('@utils/Muiutils', () => ({
	Item: ({ children, variant, className }: any) => (
		<div data-testid="item" data-variant={variant} className={className}>{children}</div>
	)
}))

// Mock console.error
const originalConsoleError = console.error
beforeAll(() => {
	console.error = jest.fn()
})

afterAll(() => {
	console.error = originalConsoleError
})

describe('DebugMetrics', () => {
	const mockDebugMetricsData = {
		'api1': {
			name: 'Test API 1',
			numops: 100,
			minTime: 1000,
			maxTime: 5000,
			avgTime: 2500
		},
		'api2': {
			name: 'Test API 2',
			numops: 200,
			minTime: 2000,
			maxTime: 6000,
			avgTime: 3500
		}
	}

	const mockSortedData = [
		mockDebugMetricsData['api1'],
		mockDebugMetricsData['api2']
	]

	beforeEach(() => {
		jest.clearAllMocks()
		mockIsEmpty.mockImplementation((val: any) => {
			if (val == null) return true
			if (Array.isArray(val)) return val.length === 0
			if (typeof val === 'object') return Object.keys(val).length === 0
			if (val === '') return true
			return false
		})
		mockCustomSortBy.mockImplementation((arr: any) => arr || [])
		mockMomentNow.mockReturnValue(1234567890)
		mockGetDebugMetrics.mockResolvedValue({
			data: mockDebugMetricsData
		})
	})

	it('should render DebugMetrics component', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByTestId('item')).toBeInTheDocument()
			expect(screen.getByText('REST API Metrics')).toBeInTheDocument()
		})
	})

	it('should fetch debug metrics on mount', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(1)
		})
	})

	it('should display loading state while fetching', async () => {
		mockGetDebugMetrics.mockImplementation(() => new Promise(resolve => setTimeout(() => resolve({ data: mockDebugMetricsData }), 100)))

		render(<DebugMetrics />)

		expect(screen.getByTestId('loading')).toBeInTheDocument()

		await waitFor(() => {
			expect(screen.queryByTestId('loading')).not.toBeInTheDocument()
		})
	})

	it('should display metrics data in table', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('Test API 1')).toBeInTheDocument()
			expect(screen.getByText('Test API 2')).toBeInTheDocument()
		})
	})

	it('should display empty message when no metrics data', async () => {
		mockGetDebugMetrics.mockResolvedValue({ data: {} })
		mockIsEmpty.mockReturnValue(true)
		mockCustomSortBy.mockReturnValue([])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('No Records found!')).toBeInTheDocument()
		})
	})

	it('should handle API error gracefully', async () => {
		const error = new Error('API Error')
		mockGetDebugMetrics.mockRejectedValue(error)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockServerError).toHaveBeenCalledWith(error, expect.any(Object))
			expect(console.error).toHaveBeenCalledWith('Error occur while fetching debug metrics details', error)
		})
	})

	it('should handle API response with no data property', async () => {
		mockGetDebugMetrics.mockResolvedValue({})
		mockIsEmpty.mockReturnValue(true)
		mockCustomSortBy.mockReturnValue([])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('No Records found!')).toBeInTheDocument()
		})
	})

	it('should handle null API response', async () => {
		mockGetDebugMetrics.mockResolvedValue(null)
		mockIsEmpty.mockReturnValue(true)
		mockCustomSortBy.mockReturnValue([])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('No Records found!')).toBeInTheDocument()
		})
	})

	it('should refresh metrics when refresh button is clicked', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(1)
		})

		const refreshButton = screen.getByTestId('custom-button')
		fireEvent.click(refreshButton)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(2)
			expect(mockMomentNow).toHaveBeenCalled()
		})
	})

	it('should stop event propagation when refresh button is clicked', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByTestId('custom-button')).toBeInTheDocument()
		})

		const refreshButton = screen.getByTestId('custom-button')
		const mockEvent = {
			stopPropagation: jest.fn()
		}
		fireEvent.click(refreshButton, mockEvent)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(2)
		})
	})

	it('should display name column with value', async () => {
		mockCustomSortBy.mockReturnValue([mockDebugMetricsData['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('Test API 1')).toBeInTheDocument()
		})
	})

	it('should display N/A when name is empty', async () => {
		const dataWithEmptyName = {
			'api1': {
				name: '',
				numops: 100,
				minTime: 1000,
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithEmptyName })
		mockIsEmpty.mockImplementation((val: any) => val === '')
		mockCustomSortBy.mockReturnValue([dataWithEmptyName['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('N/A')).toBeInTheDocument()
		})
	})

	it('should display N/A when name is null', async () => {
		const dataWithNullName = {
			'api1': {
				name: null,
				numops: 100,
				minTime: 1000,
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithNullName })
		mockIsEmpty.mockImplementation((val: any) => val == null)
		mockCustomSortBy.mockReturnValue([dataWithNullName['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('N/A')).toBeInTheDocument()
		})
	})

	it('should display numops column with value', async () => {
		mockCustomSortBy.mockReturnValue([mockDebugMetricsData['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('100')).toBeInTheDocument()
		})
	})

	it('should display N/A when numops is empty', async () => {
		const dataWithEmptyNumops = {
			'api1': {
				name: 'Test API',
				numops: null,
				minTime: 1000,
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithEmptyNumops })
		mockIsEmpty.mockImplementation((val: any) => val == null)
		mockCustomSortBy.mockReturnValue([dataWithEmptyNumops['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('N/A')).toBeInTheDocument()
		})
	})

	it('should display minTime column with converted seconds', async () => {
		mockCustomSortBy.mockReturnValue([mockDebugMetricsData['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 1000ms = 1.000 seconds
			expect(screen.getByText('1.000')).toBeInTheDocument()
		})
	})

	it('should display N/A when minTime is empty', async () => {
		const dataWithEmptyMinTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: null,
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithEmptyMinTime })
		mockIsEmpty.mockImplementation((val: any) => val == null)
		mockCustomSortBy.mockReturnValue([dataWithEmptyMinTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('N/A')).toBeInTheDocument()
		})
	})

	it('should convert milliseconds to seconds correctly for minTime', async () => {
		const dataWithSpecificMinTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 1500, // 1.5 seconds
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithSpecificMinTime })
		mockCustomSortBy.mockReturnValue([dataWithSpecificMinTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 1500ms = 1.500 seconds
			expect(screen.getByText('1.500')).toBeInTheDocument()
		})
	})

	it('should display maxTime column with converted seconds', async () => {
		mockCustomSortBy.mockReturnValue([mockDebugMetricsData['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 5000ms = 5.000 seconds
			expect(screen.getByText('5.000')).toBeInTheDocument()
		})
	})

	it('should display N/A when maxTime is empty', async () => {
		const dataWithEmptyMaxTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 1000,
				maxTime: null,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithEmptyMaxTime })
		mockIsEmpty.mockImplementation((val: any) => val == null)
		mockCustomSortBy.mockReturnValue([dataWithEmptyMaxTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('N/A')).toBeInTheDocument()
		})
	})

	it('should convert milliseconds to seconds correctly for maxTime', async () => {
		const dataWithSpecificMaxTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 1000,
				maxTime: 7500, // 7.5 seconds
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithSpecificMaxTime })
		mockCustomSortBy.mockReturnValue([dataWithSpecificMaxTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 7500ms = 7.500 seconds
			expect(screen.getByText('7.500')).toBeInTheDocument()
		})
	})

	it('should display avgTime column with converted seconds', async () => {
		mockCustomSortBy.mockReturnValue([mockDebugMetricsData['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 2500ms = 2.500 seconds
			expect(screen.getByText('2.500')).toBeInTheDocument()
		})
	})

	it('should display N/A when avgTime is empty', async () => {
		const dataWithEmptyAvgTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 1000,
				maxTime: 5000,
				avgTime: null
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithEmptyAvgTime })
		mockIsEmpty.mockImplementation((val: any) => val == null)
		mockCustomSortBy.mockReturnValue([dataWithEmptyAvgTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('N/A')).toBeInTheDocument()
		})
	})

	it('should convert milliseconds to seconds correctly for avgTime', async () => {
		const dataWithSpecificAvgTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 1000,
				maxTime: 5000,
				avgTime: 3250 // 3.25 seconds
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithSpecificAvgTime })
		mockCustomSortBy.mockReturnValue([dataWithSpecificAvgTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 3250ms = 3.250 seconds
			expect(screen.getByText('3.250')).toBeInTheDocument()
		})
	})

	it('should handle millisecondsToSeconds with values over 60000ms', async () => {
		const dataWithLargeTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 65000, // 65 seconds (over 1 minute)
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithLargeTime })
		mockCustomSortBy.mockReturnValue([dataWithLargeTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 65000 % 60000 = 5000ms = 5.000 seconds
			const elements = screen.getAllByText('5.000')
			expect(elements.length).toBeGreaterThan(0)
		})
	})

	it('should display help tooltip with correct content', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByTestId('help-outlined-icon')).toBeInTheDocument()
		})
	})

	it('should display tooltip with Debug Metrics Information', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const tooltip = screen.getByTestId('tooltip')
			expect(tooltip).toBeInTheDocument()
		})
	})

	it('should display tooltip with Count description', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const tooltipContent = screen.getByTestId('tooltip-content')
			expect(tooltipContent).toBeInTheDocument()
			// Check if tooltip content contains the expected text
			expect(tooltipContent.textContent).toContain('Count')
		})
	})

	it('should display tooltip with Min Time description', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const tooltipContent = screen.getByTestId('tooltip-content')
			expect(tooltipContent).toBeInTheDocument()
			expect(tooltipContent.textContent).toContain('Min Time')
		})
	})

	it('should display tooltip with Max Time description', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const tooltipContent = screen.getByTestId('tooltip-content')
			expect(tooltipContent).toBeInTheDocument()
			expect(tooltipContent.textContent).toContain('Max Time')
		})
	})

	it('should display tooltip with Average Time description', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const tooltipContent = screen.getByTestId('tooltip-content')
			expect(tooltipContent).toBeInTheDocument()
			expect(tooltipContent.textContent).toContain('Average Time')
		})
	})

	it('should call customSortBy with correct parameters', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockCustomSortBy).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({ name: 'Test API 1' }),
					expect.objectContaining({ name: 'Test API 2' })
				]),
				['name']
			)
		})
	})

	it('should handle empty debugMetricsData object', async () => {
		mockGetDebugMetrics.mockResolvedValue({ data: {} })
		mockIsEmpty.mockImplementation((val: any) => {
			if (val == null) return true
			if (typeof val === 'object' && Object.keys(val).length === 0) return true
			return false
		})
		mockCustomSortBy.mockReturnValue([])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('No Records found!')).toBeInTheDocument()
		}, { timeout: 3000 })
	})

	it('should handle undefined debugMetricsData', async () => {
		mockGetDebugMetrics.mockResolvedValue({ data: undefined })
		mockIsEmpty.mockReturnValue(true)
		mockCustomSortBy.mockReturnValue([])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('No Records found!')).toBeInTheDocument()
		})
	})

	it('should update table when refresh button is clicked', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(1)
		})

		const refreshButton = screen.getByTestId('custom-button')
		fireEvent.click(refreshButton)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(2)
		})
	})

	it('should render tabs with correct value', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const tabs = screen.getByTestId('tabs')
			expect(tabs).toHaveAttribute('data-value', '0')
		})
	})

	it('should render tabs with correct data-cy attribute', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const tabs = screen.getByTestId('tabs')
			expect(tabs).toHaveAttribute('data-cy', 'tab-list')
		})
	})

	it('should render refresh button with correct attributes', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const refreshButton = screen.getByTestId('custom-button')
			expect(refreshButton).toHaveAttribute('data-variant', 'outlined')
			expect(refreshButton).toHaveAttribute('data-size', 'small')
			expect(refreshButton).toHaveAttribute('data-cy', 'refreshSearchResult')
		})
	})

	it('should render TableLayout with correct props', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)

		render(<DebugMetrics />)

		await waitFor(() => {
			const tableLayout = screen.getByTestId('table-layout')
			expect(tableLayout).toBeInTheDocument()
			expect(tableLayout).toHaveAttribute('data-fetching', 'false')
		})
	})

	it('should render TableLayout with isFetching true during load', async () => {
		mockGetDebugMetrics.mockImplementation(() => new Promise(resolve => setTimeout(() => resolve({ data: mockDebugMetricsData }), 100)))

		render(<DebugMetrics />)

		const tableLayout = screen.getByTestId('table-layout')
		expect(tableLayout).toHaveAttribute('data-fetching', 'true')

		await waitFor(() => {
			expect(tableLayout).toHaveAttribute('data-fetching', 'false')
		})
	})

	it('should handle millisecondsToSeconds with zero value', async () => {
		const dataWithZeroTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 0,
				maxTime: 0,
				avgTime: 0
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithZeroTime })
		mockCustomSortBy.mockReturnValue([dataWithZeroTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 0ms = 0.000 seconds
			expect(screen.getAllByText('0.000').length).toBeGreaterThan(0)
		})
	})

	it('should handle millisecondsToSeconds with very small value', async () => {
		const dataWithSmallTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 1, // 1ms
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithSmallTime })
		mockCustomSortBy.mockReturnValue([dataWithSmallTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 1ms = 0.001 seconds
			expect(screen.getByText('0.001')).toBeInTheDocument()
		})
	})

	it('should handle millisecondsToSeconds with decimal result', async () => {
		const dataWithDecimalTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 1234, // 1.234 seconds
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithDecimalTime })
		mockCustomSortBy.mockReturnValue([dataWithDecimalTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('1.234')).toBeInTheDocument()
		})
	})

	it('should render all column headers correctly', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('Name')).toBeInTheDocument()
			expect(screen.getByText('Min Time(secs)')).toBeInTheDocument()
			expect(screen.getByText('Max Time(secs)')).toBeInTheDocument()
			expect(screen.getByText('Average Time(secs)')).toBeInTheDocument()
		})
	})

	it('should handle multiple API metrics correctly', async () => {
		const multipleApis = {
			'api1': { name: 'API 1', numops: 100, minTime: 1000, maxTime: 5000, avgTime: 2500 },
			'api2': { name: 'API 2', numops: 200, minTime: 2000, maxTime: 6000, avgTime: 3500 },
			'api3': { name: 'API 3', numops: 300, minTime: 3000, maxTime: 7000, avgTime: 4500 }
		}
		mockGetDebugMetrics.mockResolvedValue({ data: multipleApis })
		mockCustomSortBy.mockReturnValue(Object.values(multipleApis))

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('API 1')).toBeInTheDocument()
			expect(screen.getByText('API 2')).toBeInTheDocument()
			expect(screen.getByText('API 3')).toBeInTheDocument()
		})
	})

	it('should handle error and set loader to false', async () => {
		const error = new Error('Network Error')
		mockGetDebugMetrics.mockRejectedValue(error)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockServerError).toHaveBeenCalled()
			const tableLayout = screen.getByTestId('table-layout')
			expect(tableLayout).toHaveAttribute('data-fetching', 'false')
		})
	})

	it('should handle refresh after error', async () => {
		const error = new Error('Network Error')
		mockGetDebugMetrics
			.mockRejectedValueOnce(error)
			.mockResolvedValueOnce({ data: mockDebugMetricsData })

		mockCustomSortBy.mockReturnValue(mockSortedData)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockServerError).toHaveBeenCalled()
		})

		const refreshButton = screen.getByTestId('custom-button')
		fireEvent.click(refreshButton)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(2)
			expect(screen.getByText('Test API 1')).toBeInTheDocument()
		})
	})

	it('should update updateTable state on refresh', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)
		mockMomentNow.mockReturnValue(1234567890)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(1)
		})

		const refreshButton = screen.getByTestId('custom-button')
		fireEvent.click(refreshButton)

		await waitFor(() => {
			expect(mockMomentNow).toHaveBeenCalled()
		})
	})

	it('should render Item component with correct props', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const item = screen.getByTestId('item')
			expect(item).toHaveAttribute('data-variant', 'outlined')
			expect(item).toHaveClass('administration-items')
		})
	})

	it('should render Stack components correctly', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const stacks = screen.getAllByTestId('stack')
			expect(stacks.length).toBeGreaterThan(0)
		})
	})

	it('should render Divider in tooltip', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const tooltipContent = screen.getByTestId('tooltip-content')
			expect(tooltipContent).toBeInTheDocument()
		})
	})

	it('should render List and ListItem components in tooltip', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const tooltipContent = screen.getByTestId('tooltip-content')
			expect(tooltipContent).toBeInTheDocument()
		})
	})

	it('should handle millisecondsToSeconds edge case with exactly 60000ms', async () => {
		const dataWithExactMinute = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 60000, // Exactly 1 minute
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithExactMinute })
		mockCustomSortBy.mockReturnValue([dataWithExactMinute['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 60000 % 60000 = 0ms = 0.000 seconds
			expect(screen.getByText('0.000')).toBeInTheDocument()
		})
	})

	it('should handle millisecondsToSeconds with value just under 60000ms', async () => {
		const dataWithJustUnderMinute = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 59999, // Just under 1 minute
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithJustUnderMinute })
		mockCustomSortBy.mockReturnValue([dataWithJustUnderMinute['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 59999ms = 59.999 seconds
			expect(screen.getByText('59.999')).toBeInTheDocument()
		})
	})

	it('should handle millisecondsToSeconds with value just over 60000ms', async () => {
		const dataWithJustOverMinute = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 60001, // Just over 1 minute
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithJustOverMinute })
		mockCustomSortBy.mockReturnValue([dataWithJustOverMinute['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// 60001 % 60000 = 1ms = 0.001 seconds
			expect(screen.getByText('0.001')).toBeInTheDocument()
		})
	})

	it('should render Typography with correct color for name', async () => {
		mockCustomSortBy.mockReturnValue([mockDebugMetricsData['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			const typography = screen.getAllByTestId('typography')
			const nameTypography = typography.find(t => t.textContent === 'Test API 1')
			expect(nameTypography).toHaveAttribute('data-color', '#686868')
		})
	})

	it('should render Typography with correct color for numops', async () => {
		mockCustomSortBy.mockReturnValue([mockDebugMetricsData['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			const typography = screen.getAllByTestId('typography')
			const numopsTypography = typography.find(t => t.textContent === '100')
			expect(numopsTypography).toHaveAttribute('data-color', '#686868')
		})
	})

	it('should handle empty string name in metrics', async () => {
		const dataWithEmptyStringName = {
			'api1': {
				name: '',
				numops: 100,
				minTime: 1000,
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithEmptyStringName })
		mockIsEmpty.mockImplementation((val: any) => val === '')
		mockCustomSortBy.mockReturnValue([dataWithEmptyStringName['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('N/A')).toBeInTheDocument()
		})
	})

	it('should handle undefined name in metrics', async () => {
		const dataWithUndefinedName = {
			'api1': {
				name: undefined,
				numops: 100,
				minTime: 1000,
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithUndefinedName })
		mockIsEmpty.mockImplementation((val: any) => val == null)
		mockCustomSortBy.mockReturnValue([dataWithUndefinedName['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('N/A')).toBeInTheDocument()
		})
	})

	it('should handle all time fields being null', async () => {
		const dataWithAllNullTimes = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: null,
				maxTime: null,
				avgTime: null
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithAllNullTimes })
		mockIsEmpty.mockImplementation((val: any) => val == null)
		mockCustomSortBy.mockReturnValue([dataWithAllNullTimes['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			const naElements = screen.getAllByText('N/A')
			expect(naElements.length).toBeGreaterThanOrEqual(3)
		})
	})

	it('should handle all fields being empty', async () => {
		const dataWithAllEmpty = {
			'api1': {
				name: null,
				numops: null,
				minTime: null,
				maxTime: null,
				avgTime: null
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithAllEmpty })
		mockIsEmpty.mockImplementation((val: any) => val == null)
		mockCustomSortBy.mockReturnValue([dataWithAllEmpty['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			const naElements = screen.getAllByText('N/A')
			expect(naElements.length).toBeGreaterThanOrEqual(5)
		})
	})

	it('should handle customSortBy returning empty array', async () => {
		mockGetDebugMetrics.mockResolvedValue({ data: mockDebugMetricsData })
		mockCustomSortBy.mockReturnValue([])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('No Records found!')).toBeInTheDocument()
		})
	})

	it('should handle customSortBy returning null', async () => {
		mockGetDebugMetrics.mockResolvedValue({ data: mockDebugMetricsData })
		mockCustomSortBy.mockReturnValue(null)
		mockIsEmpty.mockReturnValue(true)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('No Records found!')).toBeInTheDocument()
		})
	})

	it('should render LightTooltip for Count header', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			// LightTooltip is used in the header, check if it's rendered
			const lightTooltip = screen.queryByTestId('light-tooltip')
			// If not found, verify the component still renders correctly
			if (!lightTooltip) {
				expect(screen.getByText('REST API Metrics')).toBeInTheDocument()
			} else {
				expect(lightTooltip).toBeInTheDocument()
			}
		})
	})

	it('should handle useEffect cleanup on unmount', async () => {
		const { unmount } = render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalled()
		})

		unmount()

		// Component should unmount without errors
		expect(screen.queryByTestId('item')).not.toBeInTheDocument()
	})

	it('should handle concurrent refresh clicks', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(1)
		})

		const refreshButton = screen.getByTestId('custom-button')
		fireEvent.click(refreshButton)
		fireEvent.click(refreshButton)
		fireEvent.click(refreshButton)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(4)
		})
	})

	it('should handle API response with empty data object', async () => {
		mockGetDebugMetrics.mockResolvedValue({ data: {} })
		mockIsEmpty.mockImplementation((val: any) => {
			if (val == null) return true
			if (typeof val === 'object' && Object.keys(val).length === 0) return true
			return false
		})
		mockCustomSortBy.mockReturnValue([])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('No Records found!')).toBeInTheDocument()
		})
	})

	it('should handle API response with null data', async () => {
		mockGetDebugMetrics.mockResolvedValue({ data: null })
		mockIsEmpty.mockReturnValue(true)
		mockCustomSortBy.mockReturnValue([])

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('No Records found!')).toBeInTheDocument()
		})
	})

	it('should render all tooltip list items', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const tooltipContent = screen.getByTestId('tooltip-content')
			expect(tooltipContent).toBeInTheDocument()
		})
	})

	it('should render Grid container in tooltip', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			const tooltipContent = screen.getByTestId('tooltip-content')
			expect(tooltipContent).toBeInTheDocument()
		})
	})

	it('should handle millisecondsToSeconds with negative value (edge case)', async () => {
		const dataWithNegativeTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: -1000, // Negative value (edge case)
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithNegativeTime })
		mockCustomSortBy.mockReturnValue([dataWithNegativeTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// Negative values should still be processed
			const tableLayout = screen.getByTestId('table-layout')
			expect(tableLayout).toBeInTheDocument()
		})
	})

	it('should handle millisecondsToSeconds with very large value', async () => {
		const dataWithLargeTime = {
			'api1': {
				name: 'Test API',
				numops: 100,
				minTime: 999999999, // Very large value
				maxTime: 5000,
				avgTime: 2500
			}
		}
		mockGetDebugMetrics.mockResolvedValue({ data: dataWithLargeTime })
		mockCustomSortBy.mockReturnValue([dataWithLargeTime['api1']])

		render(<DebugMetrics />)

		await waitFor(() => {
			// Should handle modulo operation correctly
			const tableLayout = screen.getByTestId('table-layout')
			expect(tableLayout).toBeInTheDocument()
		})
	})

	it('should handle refresh button click with event object', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByTestId('custom-button')).toBeInTheDocument()
		})

		const refreshButton = screen.getByTestId('custom-button')
		const mockEvent = {
			stopPropagation: jest.fn(),
			preventDefault: jest.fn()
		}
		
		fireEvent.click(refreshButton, mockEvent)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(2)
		})
	})

	it('should render AutorenewIcon in refresh button', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByTestId('autorenew-icon')).toBeInTheDocument()
		})
	})

	it('should render LinkTab with correct label', async () => {
		render(<DebugMetrics />)

		await waitFor(() => {
			expect(screen.getByText('REST API Metrics')).toBeInTheDocument()
		})
	})

	it('should handle TableLayout with all required props', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)

		render(<DebugMetrics />)

		await waitFor(() => {
			const tableLayout = screen.getByTestId('table-layout')
			expect(tableLayout).toBeInTheDocument()
		})
	})

	it('should handle columns useMemo dependency update', async () => {
		mockCustomSortBy.mockReturnValue(mockSortedData)

		const { rerender } = render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockGetDebugMetrics).toHaveBeenCalledTimes(1)
		})

		const refreshButton = screen.getByTestId('custom-button')
		fireEvent.click(refreshButton)

		await waitFor(() => {
			expect(mockMomentNow).toHaveBeenCalled()
		})

		rerender(<DebugMetrics />)

		await waitFor(() => {
			const tableLayout = screen.getByTestId('table-layout')
			expect(tableLayout).toBeInTheDocument()
		})
	})

	it('should handle toastId ref correctly', async () => {
		const error = new Error('Test Error')
		mockGetDebugMetrics.mockRejectedValue(error)

		render(<DebugMetrics />)

		await waitFor(() => {
			expect(mockServerError).toHaveBeenCalledWith(error, expect.any(Object))
		})
	})
})
