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
import ClassificationForm from '../ClassificationForm'
import { createOrUpdateTag } from '@api/apiMethods/typeDefApiMethods'
import { serverError, sanitizeHtmlContent } from '@utils/Utils'

const mockDispatch = jest.fn()
let mockState: any = {}
let mockFormValues: any = {}
let mockWatchValues: Record<string, any> = {}
let mockIsSubmitting = false
let mockIsDirty = true
let mockFields = [{ id: '1', attributeName: 'attr' }]
let lastUseFormArgs: any = null
const appendSpy = jest.fn()
const removeSpy = jest.fn()
const setValueSpy = jest.fn()
let mockTagName = 'PII'
let mockToggleValue = true

jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => selector(mockState),
	useAppDispatch: () => mockDispatch
}))

jest.mock('@api/apiMethods/typeDefApiMethods', () => ({
	createOrUpdateTag: jest.fn()
}))

jest.mock('@redux/slice/typeDefSlices/typedefClassificationSlice', () => ({
	fetchClassificationData: jest.fn(() => ({ type: 'FETCH_CLASSIFICATION' }))
}))

jest.mock('@utils/Enum', () => ({
	defaultDataType: ['string', 'int']
}))

const baseIsEmpty = (val: any) =>
	val === null ||
	val === undefined ||
	val === '' ||
	(Array.isArray(val) && val.length === 0) ||
	(typeof val === 'object' && Object.keys(val).length === 0)

jest.mock('@utils/Utils', () => {
	const empty = (val: any) =>
		val === null ||
		val === undefined ||
		val === '' ||
		(Array.isArray(val) && val.length === 0) ||
		(typeof val === 'object' && Object.keys(val).length === 0)
	return {
		getBaseUrl: jest.fn(() => ''),
		isEmpty: jest.fn((val: any) => empty(val)),
		sanitizeHtmlContent: jest.fn((val: string) => val),
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

jest.mock('@components/muiComponents', () => ({
	CustomButton: ({ children, onClick }: any) => (
		<button type="button" onClick={onClick}>
			{children}
		</button>
	),
	LightTooltip: ({ children }: any) => <div>{children}</div>
}))

jest.mock('@utils/Muiutils', () => ({
	AntSwitch: ({ checked, onChange }: any) => (
		<input
			type="checkbox"
			checked={checked}
			onChange={(e) => onChange?.(e)}
			data-testid="ant-switch"
		/>
	)
}))

jest.mock('react-quill-new', () => ({
	__esModule: true,
	default: ({ onChange }: any) => (
		<div>
			<button type="button" onClick={() => onChange('formatted-text')}>
				ChangeQuill
			</button>
		</div>
	)
}))

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useParams: () => ({ tagName: mockTagName })
}))

jest.mock('react-hook-form', () => ({
	Controller: ({ render, name }: any) =>
		render({
			field: {
				onChange: jest.fn(),
				value: name.includes('toggleDuplicates')
					? mockToggleValue
					: mockWatchValues[name]
			},
			fieldState: { error: undefined }
		}),
	useFieldArray: () => ({
		fields: mockFields,
		append: appendSpy,
		remove: removeSpy
	}),
	useForm: (args: any) => {
		lastUseFormArgs = args
		return {
			control: {},
			handleSubmit: (fn: any) => async () => await fn(mockFormValues),
			watch: (name: string, defaultValue?: any) =>
				mockWatchValues[name] ?? defaultValue,
			reset: jest.fn(),
			setValue: setValueSpy,
			register: jest.fn(),
			isDirty: mockIsDirty,
			formState: { isSubmitting: mockIsSubmitting }
		}
	}
}))

jest.mock('@mui/material', () => {
	const actual = jest.requireActual('@mui/material')
	return {
		...actual,
		Stack: ({ children }: any) => <div>{children}</div>,
		TextField: (props: any) => (
			<input {...props} />
		),
		InputLabel: ({ children }: any) => <label>{children}</label>,
		ToggleButtonGroup: ({ children, onChange }: any) => (
			<div>
				<button type="button" onClick={(e) => onChange?.(e, 'plain')}>
					TogglePlain
				</button>
				<button type="button" onClick={() => onChange?.(undefined, 'formatted')}>
					ToggleUndefined
				</button>
				{children}
			</div>
		),
		ToggleButton: ({ children, onClick }: any) => (
			<button type="button" onClick={onClick}>
				{children}
			</button>
		),
		Select: ({ children, ...rest }: any) => (
			<select {...rest}>{children}</select>
		),
		MenuItem: ({ children, value }: any) => (
			<option value={value}>{children}</option>
		),
		IconButton: ({ children, onClick }: any) => (
			<button type="button" data-testid="remove-attr" onClick={onClick}>
				{children}
			</button>
		),
		Autocomplete: ({
			renderInput,
			onChange,
			options,
			getOptionLabel,
			isOptionEqualToValue
		}: any) => (
			<div>
				<span>{getOptionLabel?.(options?.[0] || { label: '' })}</span>
				<span>
					{String(
						isOptionEqualToValue?.(options?.[0] || {}, options?.[0] || {})
					)}
				</span>
				{renderInput?.({ InputProps: {}, inputProps: {} })}
				<button
					type="button"
					onClick={(e) => onChange?.(e, options?.[0])}
				>
					SelectClassification
				</button>
			</div>
		),
		Typography: ({ children }: any) => <div>{children}</div>
	}
})

describe('ClassificationForm - 100% Coverage', () => {
	const onClose = jest.fn()
	const setTagModal = jest.fn()

	beforeEach(() => {
		jest.clearAllMocks()
		appendSpy.mockClear()
		removeSpy.mockClear()
		setValueSpy.mockClear()
		mockTagName = 'PII'
		mockToggleValue = true
		const { isEmpty } = jest.requireMock('@utils/Utils')
		isEmpty.mockImplementation((val: any) => baseIsEmpty(val))
		mockState = {
			classification: {
				classificationData: {
					classificationDefs: [
						{ name: 'PII', description: 'desc', attributeDefs: [] }
					]
				}
			},
			enum: {
				enumObj: { data: { enumDefs: [{ name: 'EnumType', guid: '1' }] } }
			}
		}
		mockFormValues = {
			name: 'NewTag',
			description: 'Desc',
			classifications: [{ label: 'PII' }],
			attributes: [
				{ attributeName: 'a1', typeName: '', toggleDuplicates: true },
				{ attributeName: 'a2', typeName: 'array<string>', toggleDuplicates: false },
				{ attributeName: 'a3', typeName: 'string' }
			]
		}
		mockWatchValues = {
			name: 'Name',
			description: '<p>Desc</p>',
			attributes: [{ typeName: 'array<string>' }]
		}
		mockIsSubmitting = false
		mockIsDirty = true
		mockFields = [{ id: '1', attributeName: 'attr' }]
		lastUseFormArgs = null
	})

	test('creates classification and toggles description modes', async () => {
		;(createOrUpdateTag as jest.Mock).mockResolvedValueOnce({})

		render(
			<ClassificationForm
				open
				onClose={onClose}
				setTagModal={setTagModal}
				isAdd
				subAdd={false}
				node={{ text: 'PII' }}
			/>
		)

		fireEvent.click(screen.getByText('ChangeQuill'))
		fireEvent.click(screen.getByText('TogglePlain'))
		fireEvent.change(screen.getByPlaceholderText('Name required'), {
			target: { value: 'New Name' }
		})
		fireEvent.change(screen.getByPlaceholderText('Long Description'), {
			target: { value: 'Plain text' }
		})
		fireEvent.click(screen.getByText('SelectClassification'))
		fireEvent.click(screen.getByText('Add New Attributes'))
		fireEvent.click(screen.getByTestId('remove-attr'))
		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(createOrUpdateTag).toHaveBeenCalled()
		})
		expect(sanitizeHtmlContent).toHaveBeenCalled()
		expect(appendSpy).toHaveBeenCalled()
		expect(removeSpy).toHaveBeenCalled()
		expect(setValueSpy).toHaveBeenCalled()
		expect(setValueSpy).toHaveBeenCalledWith('description', 'New Name')
		expect(lastUseFormArgs.defaultValues.classifications).toHaveLength(1)
	})

	test('updates classification when editing', async () => {
		mockFormValues = {
			name: 'PII',
			description: 'Updated',
			attributes: []
		}
		mockWatchValues = {
			name: 'PII',
			description: 'Updated'
		}
		;(createOrUpdateTag as jest.Mock).mockResolvedValueOnce({})

		render(
			<ClassificationForm
				open
				onClose={onClose}
				setTagModal={setTagModal}
				isAdd={false}
				subAdd={false}
				node={{ text: 'PII' }}
			/>
		)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(createOrUpdateTag).toHaveBeenCalled()
		})
	})

	test('handles submit error', async () => {
		mockFormValues = {
			name: 'NewTag',
			description: 'Desc',
			classifications: [],
			attributes: undefined
		}
		;(createOrUpdateTag as jest.Mock).mockRejectedValueOnce(
			new Error('error')
		)

		render(
			<ClassificationForm
				open
				onClose={onClose}
				setTagModal={setTagModal}
				isAdd
				subAdd={false}
				node={{ text: 'PII' }}
			/>
		)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(serverError).toHaveBeenCalled()
		})
	})

	test('uses tagName when node is missing', () => {
		mockTagName = 'PII'
		mockState = {
			classification: {
				classificationData: {
					classificationDefs: [
						{ name: 'PII', description: 'desc', attributeDefs: [] }
					]
				}
			},
			enum: {
				enumObj: { data: { enumDefs: [] } }
			}
		}

		render(
			<ClassificationForm
				open
				onClose={onClose}
				setTagModal={setTagModal}
				isAdd
				subAdd={false}
				node={undefined}
			/>
		)

		expect(screen.getByText('SelectClassification')).toBeInTheDocument()
	})

	test('handles empty tagName and empty enums', () => {
		mockTagName = ''
		mockState = {
			classification: {
				classificationData: {}
			},
			enum: {
				enumObj: { data: { enumDefs: [] } }
			}
		}

		render(
			<ClassificationForm
				open
				onClose={onClose}
				setTagModal={setTagModal}
				isAdd
				subAdd={false}
				node={undefined}
			/>
		)

		expect(screen.getByText('Add New Attributes')).toBeInTheDocument()
	})

	test('renders array attribute toggle with false value', () => {
		mockToggleValue = false
		mockWatchValues = {
			name: 'Name',
			description: '<p>Desc</p>',
			attributes: [{ typeName: 'array<string>' }]
		}

		render(
			<ClassificationForm
				open
				onClose={onClose}
				setTagModal={setTagModal}
				isAdd
				subAdd={false}
				node={{ text: 'PII' }}
			/>
		)

		expect(screen.getByTestId('remove-attr')).toBeInTheDocument()
	})

	test('handles update error flow', async () => {
		mockFormValues = {
			name: 'PII',
			description: 'Updated',
			attributes: []
		}
		mockWatchValues = {
			name: 'PII',
			description: 'Updated'
		}
		;(createOrUpdateTag as jest.Mock).mockRejectedValueOnce(
			new Error('error')
		)

		render(
			<ClassificationForm
				open
				onClose={onClose}
				setTagModal={setTagModal}
				isAdd={false}
				subAdd={false}
				node={{ text: 'PII' }}
			/>
		)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(serverError).toHaveBeenCalled()
		})
	})

	test('covers defaultAddValues empty branch', () => {
		const { isEmpty } = jest.requireMock('@utils/Utils')
		const node = { text: 'PII' }
		let nodeCallCount = 0
		isEmpty.mockImplementation((val: any) => {
			if (val === node) {
				nodeCallCount += 1
				return nodeCallCount === 1 ? false : true
			}
			return baseIsEmpty(val)
		})

		render(
			<ClassificationForm
				open
				onClose={onClose}
				setTagModal={setTagModal}
				isAdd
				subAdd={false}
				node={node}
			/>
		)

		expect(lastUseFormArgs.defaultValues.classifications).toEqual([])
	})

	test('handles undefined classifications with isEmpty false', async () => {
		const { isEmpty } = jest.requireMock('@utils/Utils')
		isEmpty.mockImplementation((val: any) =>
			val === undefined ? false : baseIsEmpty(val)
		)
		mockFormValues = {
			name: 'NewTag',
			description: 'Desc',
			classifications: undefined,
			attributes: []
		}
		;(createOrUpdateTag as jest.Mock).mockResolvedValueOnce({})

		render(
			<ClassificationForm
				open
				onClose={onClose}
				setTagModal={setTagModal}
				isAdd
				subAdd={false}
				node={{ text: 'PII' }}
			/>
		)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(createOrUpdateTag).toHaveBeenCalled()
		})
	})

	test('skips attribute toggle when watched is null', () => {
		mockWatchValues = {
			name: 'Name',
			description: '<p>Desc</p>',
			attributes: null
		}

		render(
			<ClassificationForm
				open
				onClose={onClose}
				setTagModal={setTagModal}
				isAdd
				subAdd={false}
				node={{ text: 'PII' }}
			/>
		)

		expect(screen.getByText('Add New Attributes')).toBeInTheDocument()
	})

	test('skips attribute toggle when watched is empty array', () => {
		mockWatchValues = {
			name: 'Name',
			description: '<p>Desc</p>',
			attributes: []
		}

		render(
			<ClassificationForm
				open
				onClose={onClose}
				setTagModal={setTagModal}
				isAdd
				subAdd={false}
				node={{ text: 'PII' }}
			/>
		)

		expect(screen.getByText('Add New Attributes')).toBeInTheDocument()
	})
})
