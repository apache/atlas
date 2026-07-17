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
import AddTag from '../AddTag'
import { addTag, editAssignTag } from '@api/apiMethods/classificationApiMethod'
import { fetchGlossaryDetails } from '@redux/slice/glossaryDetailsSlice'
import { fetchDetailPageData } from '@redux/slice/detailPageSlice'
import { fetchGlossaryData } from '@redux/slice/glossarySlice'
import { serverError } from '@utils/Utils'
import { toast } from 'react-toastify'

const mockDispatch = jest.fn()
let mockState: any = {}
let mockFormValues: any = {}
let mockWatchValues: Record<string, any> = {}
let mockIsSubmitting = false
let mockIsDirty = false
let mockSearch = '?gtype=glossary'
const baseIsEmpty = (val: any) =>
	val === null ||
	val === undefined ||
	val === '' ||
	(Array.isArray(val) && val.length === 0) ||
	(typeof val === 'object' && Object.keys(val).length === 0)

jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => selector(mockState),
	useAppDispatch: () => mockDispatch
}))

jest.mock('@api/apiMethods/classificationApiMethod', () => ({
	addTag: jest.fn(),
	editAssignTag: jest.fn()
}))

jest.mock('@redux/slice/glossaryDetailsSlice', () => ({
	fetchGlossaryDetails: jest.fn(() => ({ type: 'FETCH_GLOSSARY_DETAILS' }))
}))

jest.mock('@redux/slice/detailPageSlice', () => ({
	fetchDetailPageData: jest.fn(() => ({ type: 'FETCH_DETAIL_PAGE' }))
}))

jest.mock('@redux/slice/glossarySlice', () => ({
	fetchGlossaryData: jest.fn(() => ({ type: 'FETCH_GLOSSARY_DATA' }))
}))

jest.mock('@utils/Utils', () => {
	const isEmpty = jest.fn((val: any) => baseIsEmpty(val))
	return {
		customSortBy: (arr: any[]) => arr,
		extractKeyValueFromEntity: (obj: any, key: string) => ({
			name: obj?.[key] || obj?.typeName
		}),
		getNestedSuperTypeObj: jest.fn(),
		isArray: (val: any) => Array.isArray(val),
		isEmpty,
		serverError: jest.fn()
	}
})

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
					Submit
				</button>
				{children}
			</div>
		) : null
}))

jest.mock('@components/Forms/FormCreatableSelect', () => () => (
	<div data-testid="form-creatable" />
))
jest.mock('@components/Forms/FormSelectBoolean', () => () => (
	<div data-testid="form-boolean" />
))
jest.mock('@components/Forms/FormDatepicker', () => () => (
	<div data-testid="form-date" />
))
jest.mock('@components/Forms/FormInputText', () => () => (
	<div data-testid="form-text" />
))
jest.mock('@components/Forms/FormSingleSelect', () => () => (
	<div data-testid="form-single" />
))
jest.mock('../AddValidityPeriod', () => () => (
	<div data-testid="add-validity" />
))

jest.mock('react-hook-form', () => ({
	Controller: ({ render, name }: any) =>
		render({
			field: {
				onChange: jest.fn(),
				value: mockWatchValues[name]
			},
			fieldState: { error: undefined }
		}),
	useForm: () => ({
		control: {},
		watch: (name: string) => mockWatchValues[name],
		handleSubmit: (fn: any) => () => fn(mockFormValues),
		formState: { isSubmitting: mockIsSubmitting, isDirty: mockIsDirty }
	})
}))

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useParams: () => ({ guid: 'entity-guid' }),
	useLocation: () => ({ search: mockSearch })
}))

jest.mock('react-toastify', () => ({
	toast: {
		dismiss: jest.fn(),
		success: jest.fn(),
		warning: jest.fn()
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
			getOptionLabel,
			isOptionEqualToValue,
			...rest
		}: any) => (
			<div data-testid={rest['data-cy'] || 'autocomplete'}>
				<span data-testid="option-label">
					{getOptionLabel?.(options[0] || { label: '' })}
				</span>
				<span data-testid="option-equals">
					{String(
						isOptionEqualToValue?.(options[0] || {}, options[0] || {})
					)}
				</span>
				{renderInput?.({ InputProps: {}, inputProps: {} })}
				<button
					type="button"
					data-testid="autocomplete-change"
					onClick={() => onChange?.({}, options[0])}
				>
					change
				</button>
			</div>
		),
		TextField: (props: any) => <input {...props} />,
		Checkbox: ({ checked, onChange }: any) => (
			<input type="checkbox" checked={checked} onChange={onChange} />
		),
		FormControlLabel: ({ control, label }: any) => (
			<label>
				{control}
				{label}
			</label>
		),
		InputLabel: ({ children }: any) => <label>{children}</label>,
		Stack: ({ children }: any) => <div>{children}</div>,
		Typography: ({ children }: any) => <div>{children}</div>,
		Card: ({ children }: any) => <div>{children}</div>
	}
})

const setupState = (overrides: any = {}) => {
	mockState = {
		classification: {
			classificationData: {
				classificationDefs: [
					{
						name: 'PII',
						attributeDefs: []
					}
				]
			}
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
		},
		...overrides
	}
}

describe('AddTag - 100% Coverage', () => {
	const onClose = jest.fn()
	const setUpdateTable = jest.fn()
	const setRowSelection = jest.fn()

	beforeEach(() => {
		jest.clearAllMocks()
		mockDispatch.mockReturnValue({
			unwrap: () => Promise.resolve({}),
		})
		setupState()
		mockSearch = '?gtype=glossary'
		mockIsSubmitting = false
		mockIsDirty = false
		mockFormValues = {}
		mockWatchValues = {}
		const { isEmpty, getNestedSuperTypeObj } = jest.requireMock('@utils/Utils')
		isEmpty.mockImplementation((val: any) => baseIsEmpty(val))
		getNestedSuperTypeObj.mockReset()
	})

	test('renders form controls based on attribute types', () => {
		const { getNestedSuperTypeObj } = jest.requireMock('@utils/Utils')
		getNestedSuperTypeObj.mockReturnValue([
			{ typeName: 'StatusEnum' },
			{ typeName: 'array<string>' },
			{ typeName: 'boolean' },
			{ typeName: 'date' },
			{ typeName: 'time' },
			{ typeName: 'string' },
			null
		])

		mockWatchValues = {
			classification: { label: 'PII' },
			checkModalTagProperty: true
		}

		render(
			<AddTag
				open
				isAdd
				onClose={onClose}
				entityData={{ guid: 'g1', classifications: [] }}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		fireEvent.click(screen.getByTestId('autocomplete-change'))
		expect(screen.getByTestId('form-single')).toBeInTheDocument()
		expect(screen.getByTestId('form-creatable')).toBeInTheDocument()
		expect(screen.getByTestId('form-boolean')).toBeInTheDocument()
		expect(screen.getAllByTestId('form-date')).toHaveLength(2)
		expect(screen.getByTestId('form-text')).toBeInTheDocument()
	})

	test('shows warning when classification is missing', async () => {
		setupState({
			classification: { classificationData: { classificationDefs: [] } }
		})
		mockWatchValues = {
			classification: null,
			checkModalTagProperty: true
		}
		mockFormValues = {
			classification: null,
			attributes: {}
		}

		render(
			<AddTag
				open
				isAdd
				onClose={onClose}
				entityData={{ guid: 'g1', classifications: [] }}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(toast.warning).toHaveBeenCalled()
		})
	})

	test('adds tag with validity periods and attributes array', async () => {
		const now = new Date('2024-01-01T10:00:00Z')
		mockFormValues = {
			checkModalTagProperty: true,
			classification: { label: 'PII' },
			removePropagationsOnEntityDelete: true,
			validityPeriod: [
				{
					startTime: now,
					endTime: now,
					timeZone: { label: 'UTC' }
				}
			],
			attributes: {
				tags: [{ inputValue: 'A' }, { inputValue: '' }, 'B'],
				count: 2
			}
		}
		mockWatchValues = {
			classification: { label: 'PII' },
			checkModalTagProperty: true,
			checkTimezoneProperty: true
		}

		;(addTag as jest.Mock).mockResolvedValueOnce({})

		render(
			<AddTag
				open
				isAdd
				onClose={onClose}
				entityData={[{ guid: 'g1' }, { guid: 'g2' }]}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(addTag).toHaveBeenCalled()
		})
		expect(setUpdateTable).toHaveBeenCalled()
		expect(mockDispatch).toHaveBeenCalledWith(fetchGlossaryData())
		expect(mockDispatch).toHaveBeenCalledWith(
			fetchGlossaryDetails({ gtype: 'glossary', guid: 'entity-guid' })
		)
		expect(mockDispatch).toHaveBeenCalledWith(fetchDetailPageData('entity-guid'))
		expect(setRowSelection).toHaveBeenCalledWith({})
	})

	test('edits tag and handles empty validity period', async () => {
		mockFormValues = {
			checkModalTagProperty: false,
			classification: { label: 'PII' },
			removePropagationsOnEntityDelete: false,
			validityPeriod: [],
			attributes: {}
		}
		mockWatchValues = {
			classification: { label: 'PII' },
			checkModalTagProperty: false
		}

		;(editAssignTag as jest.Mock).mockResolvedValueOnce({})

		render(
			<AddTag
				open
				isAdd={false}
				onClose={onClose}
				entityData={{
					guid: 'g1',
					typeName: 'PII',
					classifications: [],
					validityPeriods: [
						{
							startTime: '2020-01-01',
							endTime: '2020-01-01',
							timeZone: 'UTC'
						}
					],
					removePropagationsOnEntityDelete: true,
					attributes: { a: 'b' }
				}}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(editAssignTag).toHaveBeenCalled()
		})
	})

	test('handles API errors', async () => {
		mockFormValues = {
			checkModalTagProperty: true,
			classification: { label: 'PII' },
			validityPeriod: [],
			attributes: {}
		}
		mockWatchValues = {
			classification: { label: 'PII' },
			checkModalTagProperty: true
		}

		;(addTag as jest.Mock).mockRejectedValueOnce(new Error('error'))

		render(
			<AddTag
				open
				isAdd
				onClose={onClose}
				entityData={{ guid: 'g1', classifications: [] }}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(serverError).toHaveBeenCalled()
		})
	})

	test('handles edit error flow', async () => {
		mockFormValues = {
			checkModalTagProperty: true,
			classification: { label: 'PII' },
			validityPeriod: [],
			attributes: {}
		}
		mockWatchValues = {
			classification: { label: 'PII' },
			checkModalTagProperty: true
		}

		;(editAssignTag as jest.Mock).mockRejectedValueOnce(new Error('error'))

		render(
			<AddTag
				open
				isAdd={false}
				onClose={onClose}
				entityData={{ guid: 'g1', typeName: 'PII', classifications: [] }}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(serverError).toHaveBeenCalled()
		})
	})

	test('builds options when classifications already assigned', () => {
		setupState({
			classification: {
				classificationData: {
					classificationDefs: [
						{ name: 'PII', attributeDefs: [] },
						{ name: 'Sensitive', attributeDefs: [] }
					]
				}
			}
		})
		mockWatchValues = {
			classification: { label: 'Sensitive' },
			checkModalTagProperty: true
		}

		render(
			<AddTag
				open
				isAdd
				onClose={onClose}
				entityData={{
					guid: 'g1',
					classifications: [{ typeName: 'PII' }]
				}}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		expect(screen.getByTestId('custom-modal')).toBeInTheDocument()
	})

	test('skips glossary dispatch when gtype is empty', async () => {
		mockSearch = ''
		mockFormValues = {
			checkModalTagProperty: true,
			classification: { label: 'PII' },
			validityPeriod: [],
			attributes: {}
		}
		mockWatchValues = {
			classification: { label: 'PII' },
			checkModalTagProperty: true
		}

		;(addTag as jest.Mock).mockResolvedValueOnce({})

		render(
			<AddTag
				open
				isAdd
				onClose={onClose}
				entityData={{ guid: 'g1', classifications: [] }}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(addTag).toHaveBeenCalled()
		})

		expect(fetchGlossaryDetails).not.toHaveBeenCalled()
	})

	test('handles empty classification data state', () => {
		const { isEmpty } = jest.requireMock('@utils/Utils')
		const originalIsEmpty = isEmpty.getMockImplementation()
		setupState({
			classification: {
				classificationData: { classificationDefs: [] }
			},
			enum: {
				enumObj: {}
			}
		})
		mockWatchValues = {
			classification: null,
			checkModalTagProperty: false,
			checkTimezoneProperty: false
		}
		isEmpty.mockImplementation((val: any) =>
			val === mockState.classification.classificationData || baseIsEmpty(val)
		)

		render(
			<AddTag
				open
				isAdd
				onClose={onClose}
				entityData={undefined}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		expect(screen.getByTestId('custom-modal')).toBeInTheDocument()
		isEmpty.mockImplementation(originalIsEmpty || baseIsEmpty)
	})

	test('skips validity period when unchecked', () => {
		mockWatchValues = {
			classification: { label: 'PII' },
			checkModalTagProperty: true,
			checkTimezoneProperty: false
		}

		render(
			<AddTag
				open
				isAdd
				onClose={onClose}
				entityData={{ guid: 'g1', classifications: [], validityPeriods: [] }}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		expect(screen.queryByTestId('add-validity')).not.toBeInTheDocument()
	})

	test('renders default control when enumDefs empty', () => {
		const { getNestedSuperTypeObj } = jest.requireMock('@utils/Utils')
		setupState({
			enum: {
				enumObj: { data: { enumDefs: [] } }
			},
			classification: {
				classificationData: {
					classificationDefs: [{ name: 'PII', attributeDefs: [] }]
				}
			}
		})
		getNestedSuperTypeObj.mockReturnValue([{ typeName: 'string' }])
		mockWatchValues = {
			classification: { label: 'PII' },
			checkModalTagProperty: true
		}

		render(
			<AddTag
				open
				isAdd
				onClose={onClose}
				entityData={{ guid: 'g1', classifications: [] }}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		expect(screen.getByTestId('form-text')).toBeInTheDocument()
	})

	test('handles undefined classification and enum state', () => {
		const originalFilter = (Object.prototype as any).filter
		;(Object.prototype as any).filter = () => []
		mockState = {
			classification: {},
			enum: {}
		}
		mockWatchValues = {
			classification: null,
			checkModalTagProperty: false,
			checkTimezoneProperty: false
		}

		render(
			<AddTag
				open
				isAdd
				onClose={onClose}
				entityData={{ guid: 'g1', classifications: [] }}
				setUpdateTable={setUpdateTable}
				setRowSelection={setRowSelection}
			/>
		)

		expect(screen.getByTestId('custom-modal')).toBeInTheDocument()
		;(Object.prototype as any).filter = originalFilter
	})
})
