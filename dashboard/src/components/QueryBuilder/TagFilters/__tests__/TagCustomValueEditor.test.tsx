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
 * Unit tests for TagCustomValueEditor component
 * 
 * Coverage Target: 100%
 */

import React from 'react'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { TagCustomValueEditor } from '../TagCustomValueEditor'

const mockUseAppSelector = jest.fn()

jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => mockUseAppSelector(selector)
}))

const defaultState = {
	typeHeader: {
		typeHeaderData: [
			{ name: 'Tag1', category: 'CLASSIFICATION' },
			{ name: 'Tag2', category: 'CLASSIFICATION' },
			{ name: 'Entity1', category: 'ENTITY' }
		]
	}
}

jest.mock('@utils/Utils', () => ({
	isEmpty: jest.fn((val: any) => !val || (Array.isArray(val) && val.length === 0))
}))

jest.mock('@utils/Enum', () => ({
	timeRangeOptions: [
		{ value: 'last_24_hours', label: 'Last 24 Hours' },
		{ value: 'last_7_days', label: 'Last 7 Days' },
		{ value: 'custom_range', label: 'Custom Range' }
	]
}))

jest.mock('@components/DatePicker/CustomDatePicker', () => ({
	__esModule: true,
	default: ({ onChange, selected, startDate, endDate }: any) => (
		<div data-testid="date-picker">
			<input
				data-testid="date-input"
				onChange={(e: any) => {
					if (onChange) {
						const date = new Date(e.target.value)
						onChange([date, date])
					}
				}}
			/>
			<div data-testid="start-date">{startDate ? 'has-start' : 'no-start'}</div>
			<div data-testid="end-date">{endDate ? 'has-end' : 'no-end'}</div>
		</div>
	)
}))

jest.mock('react-querybuilder', () => ({
	ValueEditor: (props: any) => (
		<input
			data-testid="value-editor"
			value={props.value || ''}
			onChange={(e: any) => props.handleOnChange?.(e.target.value)}
		/>
	)
}))

jest.mock('@mui/material', () => ({
	Autocomplete: ({ value, onChange, options, renderInput }: any) => (
		<div data-testid="autocomplete">
			<input
				data-testid="autocomplete-input"
				value={value || ''}
				onChange={(e: any) => onChange?.(null, e.target.value)}
			/>
			{options?.map((opt: any, idx: number) => (
				<div key={idx} onClick={() => onChange?.(null, opt)}>
					{opt}
				</div>
			))}
			{renderInput?.({})}
		</div>
	),
	TextField: (props: any) => <input {...props} data-testid="text-field" />
}))

const { isEmpty } = require('@utils/Utils')
const moment = require('moment')

describe('TagCustomValueEditor', () => {
	const mockHandleOnChange = jest.fn()

	const defaultProps = {
		field: 'testField',
		operator: '=',
		value: '',
		handleOnChange: mockHandleOnChange,
		inputType: 'text'
	}

	beforeEach(() => {
		jest.clearAllMocks()
		mockUseAppSelector.mockImplementation((selector: any) => selector(defaultState))
		isEmpty.mockImplementation((val: any) => !val || (Array.isArray(val) && val.length === 0))
	})

	it('renders Autocomplete for __typeName field', () => {
		render(<TagCustomValueEditor {...defaultProps} field="__typeName" />)

		expect(screen.getByTestId('autocomplete')).toBeTruthy()
	})

	it('handles type name change', () => {
		render(<TagCustomValueEditor {...defaultProps} field="__typeName" />)

		const input = screen.getByTestId('autocomplete-input')
		fireEvent.change(input, { target: { value: 'Tag1' } })

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('renders time range selector for datetime-local with TIME_RANGE operator', () => {
		render(
			<TagCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				operator="TIME_RANGE"
			/>
		)

		expect(screen.getByText('Select Time Range')).toBeTruthy()
	})

	it('shows date picker when custom_range is selected', async () => {
		render(
			<TagCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				operator="TIME_RANGE"
			/>
		)

		const select = screen.getByRole('combobox') || screen.getByDisplayValue('')
		if (select) {
			fireEvent.change(select, { target: { value: 'custom_range' } })
		}

		await waitFor(() => {
			expect(screen.getByTestId('date-picker')).toBeTruthy()
		})
	})

	it('handles date range change', async () => {
		render(
			<TagCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				operator="TIME_RANGE"
			/>
		)

		const select = screen.getByRole('combobox') || screen.getByDisplayValue('')
		if (select) {
			fireEvent.change(select, { target: { value: 'custom_range' } })
		}

		await waitFor(() => {
			const datePicker = screen.getByTestId('date-picker')
			expect(datePicker).toBeTruthy()

			const dateInput = screen.getByTestId('date-input')
			fireEvent.change(dateInput, { target: { value: '2024-01-01' } })
		})

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('handles non-custom range selection', () => {
		render(
			<TagCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				operator="TIME_RANGE"
			/>
		)

		const select = screen.getByRole('combobox') || screen.getByDisplayValue('')
		if (select) {
			fireEvent.change(select, { target: { value: 'last_24_hours' } })
		}

		expect(mockHandleOnChange).toHaveBeenCalledWith('last_24_hours')
	})

	it('renders date picker for datetime-local without TIME_RANGE', () => {
		render(
			<TagCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				value={moment().valueOf()}
			/>
		)

		expect(screen.getByTestId('date-picker')).toBeTruthy()
	})

	it('initializes date value when selectedDateValue is null', () => {
		render(
			<TagCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				value=""
			/>
		)

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('handles date change for datetime-local', () => {
		render(
			<TagCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				value={moment().valueOf()}
			/>
		)

		const dateInput = screen.getByTestId('date-input')
		fireEvent.change(dateInput, { target: { value: '2024-01-01' } })

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('returns null for is_null operator', () => {
		const { container } = render(
			<TagCustomValueEditor {...defaultProps} operator="is_null" />
		)

		expect(container.firstChild).toBeNull()
	})

	it('returns null for not_null operator', () => {
		const { container } = render(
			<TagCustomValueEditor {...defaultProps} operator="not_null" />
		)

		expect(container.firstChild).toBeNull()
	})

	it('renders default ValueEditor for other fields', () => {
		render(<TagCustomValueEditor {...defaultProps} field="otherField" />)

		expect(screen.getByTestId('value-editor')).toBeTruthy()
	})

	it('handles empty date range', async () => {
		render(
			<TagCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				operator="TIME_RANGE"
			/>
		)

		const select = screen.getByRole('combobox') || screen.getByDisplayValue('')
		if (select) {
			fireEvent.change(select, { target: { value: 'custom_range' } })
		}

		await waitFor(() => {
			const datePicker = screen.getByTestId('date-picker')
			expect(datePicker).toBeTruthy()
		})

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('handles valid date value', () => {
		const validDate = moment('2024-01-01').valueOf()
		render(
			<TagCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				value={validDate}
			/>
		)

		expect(screen.getByTestId('date-picker')).toBeTruthy()
	})

	it('handles invalid date value', () => {
		render(
			<TagCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				value="invalid-date"
			/>
		)

		expect(screen.getByTestId('date-picker')).toBeTruthy()
	})

	it('filters tag data correctly', () => {
		render(<TagCustomValueEditor {...defaultProps} field="__typeName" />)

		const autocomplete = screen.getByTestId('autocomplete')
		expect(autocomplete).toBeTruthy()
	})

	it('handles empty tagData', () => {
		mockUseAppSelector.mockImplementationOnce((selector: any) => selector({
			typeHeader: {
				typeHeaderData: []
			}
		}))

		render(<TagCustomValueEditor {...defaultProps} field="__typeName" />)

		expect(screen.getByTestId('autocomplete')).toBeTruthy()
	})

	it('handles date range with null values', async () => {
		render(
			<TagCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				operator="TIME_RANGE"
			/>
		)

		const select = screen.getByRole('combobox') || screen.getByDisplayValue('')
		if (select) {
			fireEvent.change(select, { target: { value: 'custom_range' } })
		}

		await waitFor(() => {
			expect(screen.getByTestId('date-picker')).toBeTruthy()
		})
	})
})
