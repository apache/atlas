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
 * Comprehensive unit tests for Statistics component
 * 
 * Coverage Target: 100% (Statements, Branches, Functions, Lines)
 */

import React from 'react'
import { render, screen, fireEvent, waitFor } from '@utils/test-utils'
import Statistics, { getStatsValue } from '../Statistics'

const mockGetMetricsStats = jest.fn()
const mockDispatch = jest.fn()
const mockFetchMetricEntity = jest.fn()

jest.mock('../../../api/apiMethods/metricsApiMethods', () => ({
	getMetricsStats: (...args: any[]) => mockGetMetricsStats(...args)
}))

jest.mock('../../../hooks/reducerHook', () => ({
	useAppDispatch: () => mockDispatch,
	useAppSelector: jest.fn((selector: any) =>
		selector({
			metrics: {
				metricsData: {
					data: {
						general: { entityCount: 100 }
					}
				}
			}
		})
	)
}))

jest.mock('../../../redux/slice/metricsSlice', () => ({
	fetchMetricEntity: () => mockFetchMetricEntity()
}))

const mockToastSuccess = jest.fn(() => 'toast-id-123')
const mockToastDismiss = jest.fn()

jest.mock('react-toastify', () => ({
	toast: {
		success: (...args: any[]) => mockToastSuccess(...args),
		dismiss: (...args: any[]) => mockToastDismiss(...args)
	}
}))

jest.mock('@components/Modal', () => ({
	__esModule: true,
	default: ({ open, onClose, title, button2Label, button2Handler, postTitleIcon, children }: any) =>
		open ? (
			<div data-testid="modal">
				<div>{title}</div>
				{postTitleIcon}
				{children}
				<button onClick={button2Handler}>{button2Label}</button>
				<button onClick={onClose}>Close</button>
			</div>
		) : null
}))

jest.mock('../EntityStats', () => ({
	__esModule: true,
	default: ({ selectedValue, handleClose, currentMetricsData, setLoading }: any) => (
		<div data-testid="entity-stats">
			EntityStats - {selectedValue.label}
		</div>
	)
}))

jest.mock('../ClassificationStats', () => ({
	__esModule: true,
	default: ({ handleClose }: any) => <div data-testid="classification-stats">ClassificationStats</div>
}))

jest.mock('../ServerStats', () => ({
	__esModule: true,
	default: ({ selectedValue, currentMetricsData }: any) => (
		<div data-testid="server-stats">ServerStats - {selectedValue.label}</div>
	)
}))

jest.mock('../SystemDetails', () => ({
	__esModule: true,
	default: () => <div data-testid="system-details">SystemDetails</div>
}))

jest.mock('@components/SkeletonLoader', () => ({
	__esModule: true,
	default: ({ count }: any) => <div data-testid="skeleton-loader">Loading {count} items</div>
}))

jest.mock('@utils/Utils', () => ({
	formatedDate: jest.fn(({ date }: any) => `Formatted: ${date}`),
	millisecondsToTime: jest.fn((ms: number) => `${ms}ms`),
	serverError: jest.fn()
}))

jest.mock('@utils/Helper', () => ({
	numberFormatWithComma: jest.fn((val: number) => val.toLocaleString())
}))

jest.mock('@mui/material', () => {
	const actual = jest.requireActual('@mui/material')
	return {
		...actual,
		Autocomplete: ({ value, onChange, options, getOptionLabel, renderInput, isOptionEqualToValue }: any) => (
			<div data-testid="autocomplete">
				<input
					data-testid="autocomplete-input"
					value={value?.label || ''}
					onChange={(e: any) => {
						const option = options.find((opt: any) => opt.label === e.target.value)
						if (option) onChange(null, option)
					}}
				/>
				<select
					data-testid="autocomplete-select"
					value={value?.value || ''}
					onChange={(e: any) => {
						const option = options.find((opt: any) => opt.value === e.target.value)
						if (option) onChange(null, option)
					}}
				>
					{options.map((opt: any) => (
						<option key={opt.value} value={opt.value}>
							{getOptionLabel(opt)}
						</option>
					))}
				</select>
			</div>
		),
		Stack: ({ children, ...props }: any) => <div {...props}>{children}</div>,
		IconButton: ({ onClick, children }: any) => (
			<button onClick={onClick} data-testid="refresh-button">
				{children}
			</button>
		),
		TextField: ({ ...props }: any) => <input {...props} />
	}
})

jest.mock('@components/muiComponents', () => ({
	LightTooltip: ({ children, title }: any) => <div title={title}>{children}</div>
}))

jest.mock('@mui/icons-material', () => ({
	Refresh: () => <span>RefreshIcon</span>
}))

describe('Statistics', () => {
	const mockHandleClose = jest.fn()

	beforeEach(() => {
		jest.clearAllMocks()
		mockGetMetricsStats.mockResolvedValue({
			data: [
				{ collectionTime: '2024-01-01T00:00:00Z' },
				{ collectionTime: '2024-01-02T00:00:00Z' }
			]
		})
	})

	it('renders modal when open is true', () => {
		render(<Statistics open={true} handleClose={mockHandleClose} />)

		expect(screen.getByTestId('modal')).toBeInTheDocument()
		expect(screen.getByText('Statistics')).toBeInTheDocument()
	})

	it('does not render modal when open is false', () => {
		render(<Statistics open={false} handleClose={mockHandleClose} />)

		expect(screen.queryByTestId('modal')).not.toBeInTheDocument()
	})

	it('fetches metrics stats on mount', async () => {
		render(<Statistics open={true} handleClose={mockHandleClose} />)

		await waitFor(() => {
			expect(mockGetMetricsStats).toHaveBeenCalledWith('Current')
		})
	})

	it('shows loading skeleton initially', () => {
		render(<Statistics open={true} handleClose={mockHandleClose} />)

		expect(screen.getByTestId('skeleton-loader')).toBeInTheDocument()
	})

	it('renders child components after loading', async () => {
		render(<Statistics open={true} handleClose={mockHandleClose} />)

		await waitFor(() => {
			expect(screen.getByTestId('entity-stats')).toBeInTheDocument()
			expect(screen.getByTestId('classification-stats')).toBeInTheDocument()
			expect(screen.getByTestId('server-stats')).toBeInTheDocument()
			expect(screen.getByTestId('system-details')).toBeInTheDocument()
		})
	})

	it('handles refresh button click', async () => {
		render(<Statistics open={true} handleClose={mockHandleClose} />)

		await waitFor(() => {
			expect(screen.getByTestId('refresh-button')).toBeInTheDocument()
		})

		fireEvent.click(screen.getByTestId('refresh-button'))

		await waitFor(() => {
			expect(mockGetMetricsStats).toHaveBeenCalledTimes(2)
			expect(mockDispatch).toHaveBeenCalled()
			expect(mockToastSuccess).toHaveBeenCalledWith('Metric data is refreshed')
		})
	})

	it('handles autocomplete change to non-Current value', async () => {
		mockGetMetricsStats.mockResolvedValueOnce({
			data: [{ collectionTime: '2024-01-01T00:00:00Z' }]
		}).mockResolvedValueOnce({
			data: { metrics: { data: {} } }
		})

		render(<Statistics open={true} handleClose={mockHandleClose} />)

		await waitFor(() => {
			expect(screen.getByTestId('autocomplete-select')).toBeInTheDocument()
		})

		const select = screen.getByTestId('autocomplete-select')
		fireEvent.change(select, { target: { value: '2024-01-01T00:00:00Z' } })

		await waitFor(() => {
			expect(mockGetMetricsStats).toHaveBeenCalledWith('2024-01-01T00:00:00Z')
		})
	})

	it('handles autocomplete change to Current value', async () => {
		render(<Statistics open={true} handleClose={mockHandleClose} />)

		await waitFor(() => {
			expect(screen.getByTestId('autocomplete-select')).toBeInTheDocument()
		})

		const select = screen.getByTestId('autocomplete-select')
		fireEvent.change(select, { target: { value: 'Current' } })

		await waitFor(() => {
			expect(mockGetMetricsStats).toHaveBeenCalled()
		})
	})

	it('handles autocomplete onChange with null value', async () => {
		render(<Statistics open={true} handleClose={mockHandleClose} />)

		await waitFor(() => {
			expect(screen.getByTestId('autocomplete')).toBeInTheDocument()
		})

		const autocomplete = screen.getByTestId('autocomplete')
		const input = autocomplete.querySelector('input')
		if (input) {
			fireEvent.change(input, { target: { value: '' } })
		}
	})

	it('handles error in fetchMetricsStatsDetails', async () => {
		const { serverError } = require('@utils/Utils')
		const error = new Error('API Error')
		mockGetMetricsStats.mockRejectedValueOnce(error)

		render(<Statistics open={true} handleClose={mockHandleClose} />)

		await waitFor(() => {
			expect(serverError).toHaveBeenCalled()
		})
	})

	it('handles error in fetchCurrentMetricsDetails', async () => {
		const { serverError } = require('@utils/Utils')
		mockGetMetricsStats
			.mockResolvedValueOnce({
				data: [{ collectionTime: '2024-01-01T00:00:00Z' }]
			})
			.mockRejectedValueOnce(new Error('API Error'))

		render(<Statistics open={true} handleClose={mockHandleClose} />)

		await waitFor(() => {
			expect(screen.getByTestId('autocomplete-select')).toBeInTheDocument()
		})

		const select = screen.getByTestId('autocomplete-select')
		fireEvent.change(select, { target: { value: '2024-01-01T00:00:00Z' } })

		await waitFor(() => {
			expect(serverError).toHaveBeenCalled()
		})
	})

	it('handles cancel button click', async () => {
		render(<Statistics open={true} handleClose={mockHandleClose} />)

		await waitFor(() => {
			expect(screen.getByText('Cancel')).toBeInTheDocument()
		})

		fireEvent.click(screen.getByText('Cancel'))
		expect(mockHandleClose).toHaveBeenCalled()
	})

	it('handles close button click', async () => {
		render(<Statistics open={true} handleClose={mockHandleClose} />)

		await waitFor(() => {
			expect(screen.getByText('Close')).toBeInTheDocument()
		})

		fireEvent.click(screen.getByText('Close'))
		expect(mockHandleClose).toHaveBeenCalled()
	})

	it('dismisses existing toast before showing new one on refresh', async () => {
		// Ensure mockToastSuccess returns a consistent value
		mockToastSuccess.mockReturnValue('toast-id-123')
		
		render(<Statistics open={true} handleClose={mockHandleClose} />)

		await waitFor(() => {
			expect(screen.getByTestId('refresh-button')).toBeInTheDocument()
		})

		// Setup mocks for first refresh - getMetricsStats is called in fetchMetricsStatsDetails
		mockGetMetricsStats.mockResolvedValueOnce({
			data: [{ collectionTime: '2024-01-01T00:00:00Z' }]
		})
		mockDispatch.mockResolvedValueOnce(undefined)

		// First refresh - this will set toastId.current to 'toast-id-123'
		fireEvent.click(screen.getByTestId('refresh-button'))
		
		// Wait for the first refresh to complete
		await waitFor(() => {
			expect(mockToastSuccess).toHaveBeenCalledWith('Metric data is refreshed')
		})

		// Verify toastId was set (by checking success was called)
		expect(mockToastSuccess).toHaveBeenCalledTimes(1)

		// Clear dismiss mock to track only the second call
		mockToastDismiss.mockClear()
		mockToastSuccess.mockClear()

		// Setup mocks for second refresh
		mockGetMetricsStats.mockResolvedValueOnce({
			data: [{ collectionTime: '2024-01-01T00:00:00Z' }]
		})
		mockDispatch.mockResolvedValueOnce(undefined)

		// Second refresh - this should dismiss the previous toast since toastId.current is now set
		fireEvent.click(screen.getByTestId('refresh-button'))

		// Wait for the second refresh to complete
		await waitFor(() => {
			expect(mockToastSuccess).toHaveBeenCalledWith('Metric data is refreshed')
		})

		// Check that dismiss was called with the toast ID from the first refresh
		// The dismiss should be called before success in handleRefresh
		expect(mockToastDismiss).toHaveBeenCalledWith('toast-id-123')
	})
})

describe('getStatsValue', () => {
	const { millisecondsToTime, formatedDate } = require('@utils/Utils')
	const { numberFormatWithComma } = require('@utils/Helper')

	beforeEach(() => {
		jest.clearAllMocks()
	})

	it('returns millisecondsToTime for time type', () => {
		millisecondsToTime.mockReturnValue('1h 2m 3s')
		const result = getStatsValue({ value: 3600000, type: 'time' })
		expect(result).toBe('1h 2m 3s')
		expect(millisecondsToTime).toHaveBeenCalledWith(3600000)
	})

	it('returns formatedDate for day type', () => {
		formatedDate.mockReturnValue('2024-01-01')
		const result = getStatsValue({ value: '2024-01-01', type: 'day' })
		expect(result).toBe('2024-01-01')
		expect(formatedDate).toHaveBeenCalledWith({ date: '2024-01-01' })
	})

	it('returns numberFormatWithComma for number type', () => {
		numberFormatWithComma.mockReturnValue('1,000')
		const result = getStatsValue({ value: 1000, type: 'number' })
		expect(result).toBe('1,000')
		expect(numberFormatWithComma).toHaveBeenCalledWith(1000)
	})

	it('returns numberFormatWithComma + " millisecond/s" for millisecond type', () => {
		numberFormatWithComma.mockReturnValue('1,000')
		const result = getStatsValue({ value: 1000, type: 'millisecond' })
		expect(result).toBe('1,000 millisecond/s')
		expect(numberFormatWithComma).toHaveBeenCalledWith(1000)
	})

	it('returns Badge component for status-html type', () => {
		const result = getStatsValue({ value: 'active', type: 'status-html' })
		expect(result).toBeDefined()
		expect(React.isValidElement(result)).toBe(true)
	})

	it('returns value as-is for other types', () => {
		const result = getStatsValue({ value: 'test', type: 'unknown' })
		expect(result).toBe('test')
	})

	it('returns value as-is when type is undefined', () => {
		const result = getStatsValue({ value: 'test', type: undefined })
		expect(result).toBe('test')
	})
})
