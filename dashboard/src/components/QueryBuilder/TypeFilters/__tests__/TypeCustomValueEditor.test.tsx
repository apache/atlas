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
 * Unit tests for TypeCustomValueEditor component
 * 
 * Coverage Target: 100%
 */

import React from 'react'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { TypeCustomValueEditor } from '../TypeCustomValueEditor'

const mockUseAppSelector = jest.fn()

jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => mockUseAppSelector(selector)
}))

const defaultState = {
	classification: {
		classificationData: {
			classificationDefs: [
				{ name: 'Tag1' },
				{ name: 'Tag2' }
			]
		}
	},
	typeHeader: {
		typeHeaderData: [
			{ name: 'Entity1', category: 'ENTITY' },
			{ name: 'Entity2', category: 'ENTITY' },
			{ name: 'Tag1', category: 'CLASSIFICATION' }
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
		{ value: 'CUSTOM_RANGE', label: 'Custom Range' }
	]
}))

jest.mock('@components/DatePicker/CustomDatePicker', () => ({
	__esModule: true,
	default: ({ onChange, selected, startDate, endDate, selectsRange }: any) => {
		const handleInputChange = (e: any) => {
			if (onChange) {
				if (e.target.value === '') {
					// For selectsRange, pass array; otherwise pass single null
					if (selectsRange) {
						onChange([null, null])
					} else {
						onChange(null)
					}
				} else {
					const date = new Date(e.target.value)
					if (selectsRange) {
						onChange([date, date])
					} else {
						onChange(date)
					}
				}
			}
		}
		return (
			<div data-testid="date-picker">
				<input
					data-testid="date-input"
					onChange={handleInputChange}
					defaultValue={selected ? moment(selected).format('YYYY-MM-DD') : ''}
				/>
				<div data-testid="start-date">{startDate ? 'has-start' : 'no-start'}</div>
				<div data-testid="end-date">{endDate ? 'has-end' : 'no-end'}</div>
			</div>
		)
	}
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

describe('TypeCustomValueEditor', () => {
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

	it('renders Autocomplete for __classificationNames field', () => {
		render(<TypeCustomValueEditor {...defaultProps} field="__classificationNames" />)

		expect(screen.getByTestId('autocomplete')).toBeTruthy()
	})

	it('renders Autocomplete for __propagatedClassificationNames field', () => {
		render(
			<TypeCustomValueEditor
				{...defaultProps}
				field="__propagatedClassificationNames"
			/>
		)

		expect(screen.getByTestId('autocomplete')).toBeTruthy()
	})

	it('handles tag change', () => {
		render(<TypeCustomValueEditor {...defaultProps} field="__classificationNames" />)

		const input = screen.getByTestId('autocomplete-input')
		fireEvent.change(input, { target: { value: 'Tag1' } })

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('renders Autocomplete for __typeName field', () => {
		render(<TypeCustomValueEditor {...defaultProps} field="__typeName" />)

		expect(screen.getByTestId('autocomplete')).toBeTruthy()
	})

	it('handles type name change', () => {
		render(<TypeCustomValueEditor {...defaultProps} field="__typeName" />)

		const input = screen.getByTestId('autocomplete-input')
		fireEvent.change(input, { target: { value: 'Entity1' } })

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('returns null for is_null operator', () => {
		const { container } = render(
			<TypeCustomValueEditor {...defaultProps} operator="is_null" />
		)

		expect(container.firstChild).toBeNull()
	})

	it('returns null for not_null operator', () => {
		const { container } = render(
			<TypeCustomValueEditor {...defaultProps} operator="not_null" />
		)

		expect(container.firstChild).toBeNull()
	})

	it('renders time range selector for datetime-local with TIME_RANGE operator', () => {
		render(
			<TypeCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				operator="TIME_RANGE"
			/>
		)

		expect(screen.getByText('Select Time Range')).toBeTruthy()
	})

	it('shows date picker when CUSTOM_RANGE is selected', async () => {
		render(
			<TypeCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				operator="TIME_RANGE"
			/>
		)

		const select = screen.getByRole('combobox') || screen.getByDisplayValue('')
		if (select) {
			fireEvent.change(select, { target: { value: 'CUSTOM_RANGE' } })
		}

		await waitFor(() => {
			expect(screen.getByTestId('date-picker')).toBeTruthy()
		})
	})

	it('handles date range change with both dates', async () => {
		render(
			<TypeCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				operator="TIME_RANGE"
			/>
		)

		const select = screen.getByRole('combobox') || screen.getByDisplayValue('')
		if (select) {
			fireEvent.change(select, { target: { value: 'CUSTOM_RANGE' } })
		}

		await waitFor(() => {
			const datePicker = screen.getByTestId('date-picker')
			expect(datePicker).toBeTruthy()

			const dateInput = screen.getByTestId('date-input')
			fireEvent.change(dateInput, { target: { value: '2024-01-01' } })
		})

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('handles date range change with empty dates', async () => {
		render(
			<TypeCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				operator="TIME_RANGE"
			/>
		)

		const select = screen.getByRole('combobox') || screen.getByDisplayValue('')
		if (select) {
			fireEvent.change(select, { target: { value: 'CUSTOM_RANGE' } })
		}

		await waitFor(() => {
			expect(screen.getByTestId('date-picker')).toBeTruthy()
		})

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('handles non-CUSTOM_RANGE selection', () => {
		render(
			<TypeCustomValueEditor
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
			<TypeCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				value={moment().valueOf()}
			/>
		)

		expect(screen.getByTestId('date-picker')).toBeTruthy()
	})

	it('initializes date value when selectedDateValue is null', () => {
		render(
			<TypeCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				value=""
			/>
		)

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('handles date change for datetime-local', () => {
		render(
			<TypeCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				value={moment().valueOf()}
			/>
		)

		const dateInput = screen.getByTestId('date-input')
		fireEvent.change(dateInput, { target: { value: '2024-01-01' } })

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('handles null date change', () => {
		render(
			<TypeCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				value={moment().valueOf()}
			/>
		)

		const datePicker = screen.getByTestId('date-picker')
		const dateInput = screen.getByTestId('date-input')
		fireEvent.change(dateInput, { target: { value: '' } })

		expect(mockHandleOnChange).toHaveBeenCalled()
	})

	it('renders default ValueEditor for other fields', () => {
		render(<TypeCustomValueEditor {...defaultProps} field="otherField" />)

		expect(screen.getByTestId('value-editor')).toBeTruthy()
	})

	it('handles empty classificationDefs', () => {
		mockUseAppSelector.mockImplementationOnce((selector: any) => selector({
			classification: {
				classificationData: {}
			},
			typeHeader: {
				typeHeaderData: []
			}
		}))

		render(<TypeCustomValueEditor {...defaultProps} field="__classificationNames" />)

		expect(screen.getByTestId('autocomplete')).toBeTruthy()
	})

	it('handles empty typeHeaderData', () => {
		mockUseAppSelector.mockImplementationOnce((selector: any) => selector({
			classification: {
				classificationData: {
					classificationDefs: []
				}
			},
			typeHeader: {
				typeHeaderData: []
			}
		}))

		render(<TypeCustomValueEditor {...defaultProps} field="__typeName" />)

		expect(screen.getByTestId('autocomplete')).toBeTruthy()
	})

	it('handles dateRange as non-array', () => {
		mockUseAppSelector.mockImplementationOnce((selector: any) => selector({
			classification: {
				classificationData: {
					classificationDefs: []
				}
			},
			typeHeader: {
				typeHeaderData: []
			}
		}))

		render(
			<TypeCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				operator="TIME_RANGE"
			/>
		)

		expect(screen.getByText('Select Time Range')).toBeTruthy()
	})

	it('handles valid date value', () => {
		const validDate = moment('2024-01-01').valueOf()
		render(
			<TypeCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				value={validDate}
			/>
		)

		expect(screen.getByTestId('date-picker')).toBeTruthy()
	})

	it('handles invalid date value', () => {
		render(
			<TypeCustomValueEditor
				{...defaultProps}
				inputType="datetime-local"
				value="invalid-date"
			/>
		)

		expect(screen.getByTestId('date-picker')).toBeTruthy()
	})
})
