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

import React, { useEffect } from 'react'
import { fireEvent, render, screen, waitFor } from '@utils/test-utils'
import { useForm } from 'react-hook-form'
import BusinessMetadataAttributeForm, {
	filterAttributeEnumOptions
} from '../BusinessMetadataAtrributeForm'
import { createEnum, updateEnum } from '@api/apiMethods/typeDefApiMethods'
import { fetchEnumData } from '@redux/slice/enumSlice'
import { serverError } from '@utils/Utils'
import { toast } from 'react-toastify'

const mockDispatch = jest.fn()
let mockState: any = {}
let enumFormValues: any = null
let lastCheckboxOnChange: any = null

jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => selector(mockState),
	useAppDispatch: () => mockDispatch
}))

jest.mock('@api/apiMethods/typeDefApiMethods', () => ({
	createEnum: jest.fn(),
	updateEnum: jest.fn()
}))

jest.mock('@redux/slice/enumSlice', () => ({
	fetchEnumData: jest.fn(() => ({ type: 'FETCH_ENUM_DATA' }))
}))

jest.mock('@components/muiComponents', () => ({
	CustomButton: ({ children, onClick, ...rest }: any) => (
		<button type="button" onClick={onClick} {...rest}>
			{children}
		</button>
	),
	LightTooltip: ({ children }: any) => <div>{children}</div>
}))

jest.mock('@components/Modal', () => ({
	__esModule: true,
	default: ({
		open,
		button1Handler,
		button2Handler,
		children
	}: any) =>
		open ? (
			<div data-testid="custom-modal">
				<button type="button" onClick={button1Handler}>
					Cancel
				</button>
				<button type="button" onClick={button2Handler}>
					Update
				</button>
				{children}
			</div>
		) : null
}))

jest.mock('../EnumCreateUpdate', () => {
	const React = require('react')
	return {
		__esModule: true,
		default: (props: any) => {
			React.useEffect(() => {
				if (enumFormValues) {
					props.setValue('enumType', enumFormValues.enumType)
					props.setValue('enumValues', enumFormValues.enumValues)
				}
			}, [props.setValue])
			return <div data-testid="enum-create-update" />
		}
	}
})

jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) =>
		val === null ||
		val === undefined ||
		val === '' ||
		(Array.isArray(val) && val.length === 0) ||
		(typeof val === 'object' && Object.keys(val).length === 0),
	customSortBy: (arr: any[]) => arr,
	serverError: jest.fn()
}))

jest.mock('@utils/Enum', () => ({
	dataTypes: ['string', 'enumeration'],
	searchWeight: [1, 3, 5]
}))

jest.mock('react-toastify', () => ({
	toast: {
		dismiss: jest.fn(),
		success: jest.fn()
	}
}))

jest.mock('@mui/material', () => {
	const actual = jest.requireActual('@mui/material')
	return {
		...actual,
		Autocomplete: ({
			renderInput,
			onChange,
			options = [],
			multiple,
			getOptionLabel,
			isOptionEqualToValue,
			...rest
		}: any) => {
			const input = renderInput
				? renderInput({ InputProps: {}, inputProps: {} })
				: null
			const testId = rest['data-cy'] || 'autocomplete'
			if (getOptionLabel && options[0]) {
				getOptionLabel(options[0])
			}
			if (isOptionEqualToValue && options[0]) {
				isOptionEqualToValue(options[0], options[0])
			}
			return (
				<div data-testid={testId}>
					{input}
					<button
						type="button"
						data-testid={`${testId}-change`}
						onClick={() =>
							onChange?.({}, multiple ? [options[0]] : options[0])
						}
					>
						change
					</button>
					<button
						type="button"
						data-testid={`${testId}-clear`}
						onClick={() => onChange?.({}, null)}
					>
						clear
					</button>
				</div>
			)
		},
		Select: ({ children, value, onChange, ...rest }: any) => (
			<select
				data-testid={rest['data-cy'] || 'select'}
				value={value}
				onChange={onChange}
			>
				{children}
			</select>
		),
		MenuItem: ({ value, children }: any) => (
			<option value={value}>{children}</option>
		),
		Checkbox: ({ checked, onChange }: any) => {
			lastCheckboxOnChange = onChange
			return (
			<div>
				<input type="checkbox" checked={checked} onChange={onChange} />
				<button
					type="button"
					data-testid="checkbox-uncheck"
					onClick={() => onChange?.({ target: { checked: false } })}
				>
					uncheck
				</button>
			</div>
			)
		},
		ToggleButtonGroup: ({ children, onChange }: any) => (
			<div>
				{React.Children.map(children, (child: any) =>
					React.cloneElement(child, {
						onClick: () => onChange?.({}, child.props.value)
					})
				)}
				<button
					type="button"
					data-testid="toggle-null"
					onClick={() => onChange?.({}, null)}
				>
					null
				</button>
			</div>
		),
		ToggleButton: ({ value, children, onClick }: any) => (
			<button type="button" data-testid={`toggle-${value}`} onClick={onClick}>
				{children}
			</button>
		)
	}
})

const setupState = (overrides: any = {}) => {
	mockState = {
		enum: {
			enumObj: {
				data: {
					enumDefs: []
				}
			}
		},
		createBM: {
			editbmAttribute: {}
		},
		...overrides
	}
}

const buildFields = (attributeDefs: any[]) =>
	attributeDefs.map((field, index) => ({
		id: `${index}`,
		...field
	}))

const defaultAttributeDef = {
	name: 'attr1',
	typeName: 'string',
	searchWeight: 1,
	multiValueSelect: false,
	options: {
		maxStrLength: 50,
		applicableEntityTypes: []
	},
	enumType: '',
	enumValues: [],
	cardinalityToggle: 'SET'
}

const renderComponent = (options: any = {}) => {
	const attributeDefs = options.attributeDefs || [defaultAttributeDef]
	const remove = options.remove || jest.fn()
	let formMethods: any = null
	const setValueSpy = jest.fn((...args) =>
		formMethods ? formMethods.setValue(...args) : undefined
	)

	const Wrapper = () => {
		const methods = useForm({
			defaultValues: {
				attributeDefs
			}
		})
		formMethods = methods
		const watched =
			options.watchedOverride ?? methods.watch('attributeDefs' as any)

		useEffect(() => {
			options.onReady?.(methods)
		}, [methods])

		return (
			<BusinessMetadataAttributeForm
				fields={buildFields(attributeDefs)}
				control={methods.control}
				remove={remove}
				watched={watched}
				dataTypeOptions={options.dataTypeOptions || ['DataSet']}
				enumTypes={options.enumTypes || ['StatusEnum']}
				watch={methods.watch}
				setValue={setValueSpy}
			/>
		)
	}

	render(<Wrapper />)

	return { remove, setValueSpy, formMethods }
}

describe('BusinessMetadataAttributeForm - 100% Coverage', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		setupState()
		enumFormValues = null
	})

	test('filterAttributeEnumOptions filters and excludes selected', () => {
		const result = filterAttributeEnumOptions(
			[
				{ value: 'Alpha' },
				{ value: 'Delta' },
				{ value: 'Beta' }
			],
			'al',
			[{ value: 'Alpha' }]
		)

		expect(result).toEqual([])

		const fallbackResult = filterAttributeEnumOptions(
			[{ value: 'Alpha' }, { value: 'Beta' }],
			'a',
			[]
		)
		expect(fallbackResult.length).toBe(2)

		const emptyInputResult = filterAttributeEnumOptions(
			[{ value: 'Alpha' }],
			'',
			[]
		)
		expect(emptyInputResult.length).toBe(1)
	})

	test('removes attribute when remove button clicked', () => {
		const remove = jest.fn()
		renderComponent({ remove })

		const removeButton = screen.getByLabelText('back')
		fireEvent.click(removeButton)

		expect(remove).toHaveBeenCalled()
	})

	test('hides remove button when editing attribute', () => {
		setupState({
			createBM: {
				editbmAttribute: { name: 'edit-attr' }
			}
		})

		renderComponent()
		expect(screen.queryByLabelText('back')).not.toBeInTheDocument()
	})

	test('type change resets enum fields when not enumeration', () => {
		const { setValueSpy } = renderComponent({
			attributeDefs: [
				{
					...defaultAttributeDef,
					typeName: 'enumeration'
				}
			]
		})

		const selects = screen.getAllByTestId('select')
		fireEvent.change(selects[0], { target: { value: 'string' } })

		expect(setValueSpy).toHaveBeenCalledWith(
			'attributeDefs.0.enumType',
			''
		)
	})

	test('handles multivalue checkbox and cardinality toggle', () => {
		const { setValueSpy } = renderComponent({
			attributeDefs: [
				{
					...defaultAttributeDef,
					multiValueSelect: true
				}
			]
		})

		const listToggle = screen.getByTestId('toggle-LIST')
		fireEvent.click(listToggle)

		const nullToggle = screen.getByTestId('toggle-null')
		fireEvent.click(nullToggle)

		lastCheckboxOnChange?.({ target: { checked: false } })

		expect(setValueSpy).toHaveBeenCalledWith(
			'attributeDefs.0.cardinalityToggle',
			'SET'
		)
	})

	test('enum type selection sets enum values', () => {
		setupState({
			enum: {
				enumObj: {
					data: {
						enumDefs: [
							{
								name: 'StatusEnum',
								elementDefs: [{ value: 'ACTIVE' }]
							}
						]
					}
				}
			}
		})

		const { setValueSpy } = renderComponent({
			attributeDefs: [
				{
					...defaultAttributeDef,
					typeName: 'enumeration'
				}
			],
			enumTypes: ['StatusEnum']
		})

		const changeButtons = screen.getAllByTestId('autocomplete-change')
		fireEvent.click(changeButtons[0])

		expect(setValueSpy).toHaveBeenCalledWith(
			'attributeDefs.0.enumValues',
			[{ value: 'ACTIVE' }]
		)
	})

	test('renders enum values when enum type is selected', () => {
		renderComponent({
			attributeDefs: [
				{
					...defaultAttributeDef,
					typeName: 'enumeration',
					enumType: 'StatusEnum',
					enumValues: [{ value: 'ACTIVE' }]
				}
			]
		})

		expect(screen.getByTestId('enumValueSelector')).toBeInTheDocument()
		screen
			.getAllByTestId('autocomplete-change')
			.forEach((button) => fireEvent.click(button))
	})

	test('handles empty enumTypes and dataTypeOptions', () => {
		setupState({
			enum: {
				enumObj: {
					data: {
						enumDefs: []
					}
				}
			}
		})

		renderComponent({
			attributeDefs: [
				{
					...defaultAttributeDef,
					typeName: 'enumeration',
					enumType: 'MissingEnum',
					enumValues: []
				}
			],
			enumTypes: [],
			dataTypeOptions: []
		})

		expect(screen.getByTestId('enumValueSelector')).toBeInTheDocument()
	})

	test('renders enumeration fields in edit mode without enum button', () => {
		setupState({
			createBM: {
				editbmAttribute: { name: 'edit-attr' }
			},
			enum: {
				enumObj: {
					data: {
						enumDefs: [
							{
								name: 'StatusEnum',
								elementDefs: [{ value: 'ACTIVE' }]
							}
						]
					}
				}
			}
		})

		renderComponent({
			attributeDefs: [
				{
					...defaultAttributeDef,
					typeName: 'enumeration',
					enumType: 'StatusEnum',
					enumValues: [{ value: 'ACTIVE' }]
				}
			]
		})

		expect(screen.queryByText('Enum')).not.toBeInTheDocument()
	})

	test('skips watched-dependent sections when watched is undefined', () => {
		renderComponent({
			watchedOverride: undefined,
			attributeDefs: [
				{
					...defaultAttributeDef,
					typeName: 'string'
				}
			]
		})

		expect(screen.queryByTestId('enumValueSelector')).not.toBeInTheDocument()
	})

	test('creates enum and updates attribute fields', async () => {
		enumFormValues = {
			enumType: 'NewEnum',
			enumValues: [{ value: 'ONE' }, { value: 'TWO' }]
		}
		setupState({
			enum: {
				enumObj: {
					data: {
						enumDefs: []
					}
				}
			}
		})

		;(createEnum as jest.Mock).mockResolvedValueOnce({})

		const { setValueSpy } = renderComponent({
			attributeDefs: [
				{
					...defaultAttributeDef,
					typeName: 'enumeration',
					enumType: 'NewEnum'
				}
			]
		})

		fireEvent.click(screen.getByText('Enum'))
		fireEvent.click(screen.getByText('Update'))

		await waitFor(() => {
			expect(createEnum).toHaveBeenCalled()
		})

		expect(setValueSpy).toHaveBeenCalledWith(
			'attributeDefs.0.enumValues',
			[
				{ ordinal: 1, value: 'ONE' },
				{ ordinal: 2, value: 'TWO' }
			]
		)
		expect(toast.success).toHaveBeenCalledWith(
			expect.stringContaining('added')
		)
		expect(mockDispatch).toHaveBeenCalledWith(fetchEnumData())
	})

	test('updates enum when values differ', async () => {
		enumFormValues = {
			enumType: 'StatusEnum',
			enumValues: [{ value: 'ACTIVE' }, { value: 'INACTIVE' }]
		}

		setupState({
			enum: {
				enumObj: {
					data: {
						enumDefs: [
							{
								name: 'StatusEnum',
								elementDefs: [{ value: 'ACTIVE' }]
							}
						]
					}
				}
			}
		})

		;(updateEnum as jest.Mock).mockResolvedValueOnce({})

		renderComponent({
			attributeDefs: [
				{
					...defaultAttributeDef,
					typeName: 'enumeration',
					enumType: 'StatusEnum'
				}
			],
			enumTypes: ['StatusEnum']
		})

		fireEvent.click(screen.getByText('Enum'))
		fireEvent.click(screen.getByText('Update'))

		await waitFor(() => {
			expect(updateEnum).toHaveBeenCalled()
		})
		expect(toast.success).toHaveBeenCalledWith(
			expect.stringContaining('updated')
		)
	})

	test('updates enum when same length has different values', async () => {
		enumFormValues = {
			enumType: 'StatusEnum',
			enumValues: [{ value: 'ACTIVE' }, { value: 'PENDING' }]
		}

		setupState({
			enum: {
				enumObj: {
					data: {
						enumDefs: [
							{
								name: 'StatusEnum',
								elementDefs: [
									{ value: 'ACTIVE' },
									{ value: 'INACTIVE' }
								]
							}
						]
					}
				}
			}
		})

		;(updateEnum as jest.Mock).mockResolvedValueOnce({})

		renderComponent({
			attributeDefs: [
				{
					...defaultAttributeDef,
					typeName: 'enumeration',
					enumType: 'StatusEnum'
				}
			],
			enumTypes: ['StatusEnum']
		})

		fireEvent.click(screen.getByText('Enum'))
		fireEvent.click(screen.getByText('Update'))

		await waitFor(() => {
			expect(updateEnum).toHaveBeenCalled()
		})
	})

	test('shows no update message when values match', async () => {
		enumFormValues = {
			enumType: 'StatusEnum',
			enumValues: [{ value: 'ACTIVE' }]
		}

		setupState({
			enum: {
				enumObj: {
					data: {
						enumDefs: [
							{
								name: 'StatusEnum',
								elementDefs: [{ value: 'ACTIVE' }]
							}
						]
					}
				}
			}
		})

		renderComponent({
			attributeDefs: [
				{
					...defaultAttributeDef,
					typeName: 'enumeration',
					enumType: 'StatusEnum'
				}
			]
		})

		fireEvent.click(screen.getByText('Enum'))
		fireEvent.click(screen.getByText('Update'))

		await waitFor(() => {
			expect(toast.success).toHaveBeenCalledWith('No updated values')
		})
		expect(createEnum).not.toHaveBeenCalled()
		expect(updateEnum).not.toHaveBeenCalled()
	})

	test('handles enum create error', async () => {
		enumFormValues = {
			enumType: 'BrokenEnum',
			enumValues: [{ value: 'FAIL' }]
		}

		;(createEnum as jest.Mock).mockRejectedValueOnce(
			new Error('create enum error')
		)

		renderComponent({
			attributeDefs: [
				{
					...defaultAttributeDef,
					typeName: 'enumeration',
					enumType: 'BrokenEnum'
				}
			]
		})

		fireEvent.click(screen.getByText('Enum'))
		fireEvent.click(screen.getByText('Update'))

		await waitFor(() => {
			expect(serverError).toHaveBeenCalled()
		})
	})
})
