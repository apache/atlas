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

import React from 'react'
import { fireEvent, render, screen, waitFor } from '@utils/test-utils'
import { useForm } from 'react-hook-form'
import EnumCreateUpdate from '../EnumCreateUpdate'

const mockDispatch = jest.fn()
let mockState: any = {}

jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => selector(mockState),
	useAppDispatch: () => mockDispatch
}))

jest.mock('@components/muiComponents', () => ({
	CustomButton: ({ children, onClick, ...rest }: any) => (
		<button type="button" onClick={onClick} {...rest}>
			{children}
		</button>
	)
}))

jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) =>
		val === null ||
		val === undefined ||
		val === '' ||
		(Array.isArray(val) && val.length === 0) ||
		(typeof val === 'object' && Object.keys(val).length === 0),
	customSortBy: (arr: any[]) => arr
}))

jest.mock('@mui/material/Autocomplete', () => {
	const React = require('react')
	return {
		__esModule: true,
		default: ({
			renderInput,
			onChange,
			options = [],
			filterOptions,
			value,
			multiple,
			getOptionLabel,
			...rest
		}: any) => {
			const testId = rest['data-cy'] || 'autocomplete'
			const input = renderInput
				? renderInput({ InputProps: {}, inputProps: {} })
				: null

			if (filterOptions) {
				if (filterOptions.length === 3) {
					filterOptions([{ value: 'Alpha' }], { inputValue: 'a' }, [])
				} else {
					filterOptions(options, { inputValue: 'NewEnum' })
				}
			}

			if (getOptionLabel) {
				getOptionLabel({ value: 'ACTIVE' })
			}

			return (
				<div data-testid={testId}>
					{input}
					<button
						type="button"
						data-testid={`${testId}-change`}
						onClick={() =>
							onChange?.(
								{},
								multiple ? [options[0]] : options[0],
								'selectOption'
							)
						}
					>
						change
					</button>
					<button
						type="button"
						data-testid={`${testId}-clear`}
						onClick={() => onChange?.({}, null, 'clear')}
					>
						clear
					</button>
				<button
					type="button"
					data-testid={`${testId}-empty`}
					onClick={() => onChange?.({}, '', 'selectOption')}
				>
					empty
				</button>
					<button
						type="button"
						data-testid={`${testId}-create`}
						onClick={() =>
							onChange?.(
								{},
								{ value: 'NewEnum', label: 'NewEnum' },
								'selectOption'
							)
						}
					>
						create
					</button>
					<button
						type="button"
						data-testid={`${testId}-dupe`}
						onClick={() =>
							onChange?.(
								{},
								[
									{ value: 'Active' },
									{ value: 'ACTIVE' },
									{ value: 'Inactive' }
								],
								'selectOption'
							)
						}
					>
						dupe
					</button>
				</div>
			)
		},
		createFilterOptions: () => (options: any[]) => options
	}
})

const setupState = (enumDefs: any[] = []) => {
	mockState = {
		enum: {
			enumObj: {
				data: {
					enumDefs
				}
			}
		}
	}
}

const renderComponent = (options: any = {}) => {
	let methods: any = null
	let setValueSpy: any = null
	let resetSpy: any = null

	const Wrapper = () => {
		methods = useForm({
			defaultValues: {
				enumType: '',
				enumValues: []
			}
		})
		setValueSpy = jest.fn((...args) => methods.setValue(...args))
		resetSpy = jest.fn((...args) => methods.reset(...args))

		return (
			<EnumCreateUpdate
				control={methods.control}
				handleSubmit={methods.handleSubmit}
				setValue={setValueSpy}
				watch={methods.watch}
				reset={resetSpy}
				onSubmit={options.onSubmit}
				isSubmitting={options.isSubmitting}
				isDirty={options.isDirty}
			/>
		)
	}

	render(<Wrapper />)

	return { methods, setValueSpy, resetSpy }
}

describe('EnumCreateUpdate - 100% Coverage', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		setupState([
			{
				name: 'StatusEnum',
				elementDefs: [{ value: 'ACTIVE' }, { value: 'INACTIVE' }]
			}
		])
	})

	test('handles enum type changes and clear', () => {
		const { setValueSpy, methods } = renderComponent({
			onSubmit: jest.fn()
		})

		fireEvent.click(screen.getByTestId('autocomplete-change'))
		expect(setValueSpy).toHaveBeenCalledWith('enumValues', [
			{ value: 'ACTIVE' },
			{ value: 'INACTIVE' }
		])

		fireEvent.click(screen.getByTestId('autocomplete-clear'))
		expect(methods.getValues('enumValues')).toEqual([])

		fireEvent.click(screen.getByTestId('autocomplete-create'))
		expect(methods.getValues('enumType')).toBe('NewEnum')
	})

	test('renders enum values when enum type selected', async () => {
		const { methods } = renderComponent({
			onSubmit: jest.fn()
		})

		await waitFor(() => {
			methods.setValue('enumType', 'StatusEnum')
		})

		expect(screen.getByTestId('enumValueSelector')).toBeInTheDocument()
	})

	test('filters duplicate enum values on change', () => {
		const { methods } = renderComponent({
			onSubmit: jest.fn()
		})

		fireEvent.click(screen.getByTestId('autocomplete-change'))
		fireEvent.click(screen.getByTestId('enumValueSelector-dupe'))
		const values = methods.getValues('enumValues')
		expect(values.length).toBe(2)
	})

	test('handles empty selection without clear reason', () => {
		const { setValueSpy } = renderComponent({
			onSubmit: jest.fn()
		})

		fireEvent.click(screen.getByTestId('autocomplete-empty'))
		expect(setValueSpy).toHaveBeenCalledWith('enumValues', [])
	})

	test('handles clear and update buttons', () => {
		const onSubmit = jest.fn()
		const { resetSpy } = renderComponent({
			onSubmit,
			isDirty: true
		})

		const clearButton = screen.getByText('Clear')
		fireEvent.click(clearButton)
		expect(resetSpy).toHaveBeenCalledWith({ enumType: '', enumValues: [] })
	})

	test('disables buttons when submitting or not dirty', () => {
		renderComponent({
			onSubmit: jest.fn(),
			isSubmitting: true,
			isDirty: false
		})

		expect(screen.getByText('Clear')).toBeDisabled()
		expect(screen.getByText('Update')).toBeDisabled()
	})

	test('hides buttons when onSubmit is not provided', () => {
		renderComponent()

		expect(screen.queryByText('Clear')).not.toBeInTheDocument()
		expect(screen.queryByText('Update')).not.toBeInTheDocument()
	})
})
