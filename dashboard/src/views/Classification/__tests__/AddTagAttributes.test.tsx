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
import AddTagAttributes from '../AddTagAttributes'
import { createOrUpdateTag } from '@api/apiMethods/typeDefApiMethods'
import { serverError } from '@utils/Utils'

const mockDispatch = jest.fn()
let mockState: any = {}
let mockFormValues: any = {}
let mockIsSubmitting = false
let mockFields = [{ id: '1' }]
const appendSpy = jest.fn()
const removeSpy = jest.fn()
let mockTagName = 'PII'
let mockToggleValue = true

jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => selector(mockState),
	useAppDispatch: () => mockDispatch
}))

jest.mock('@api/apiMethods/typeDefApiMethods', () => ({
	createOrUpdateTag: jest.fn()
}))

jest.mock('@utils/Utils', () => ({
	isEmpty: (val: any) =>
		val === null ||
		val === undefined ||
		val === '' ||
		(Array.isArray(val) && val.length === 0) ||
		(typeof val === 'object' && Object.keys(val).length === 0),
	serverError: jest.fn()
}))

jest.mock('@components/Modal', () => ({
	__esModule: true,
	default: ({ open, button1Handler, button2Handler, children }: any) =>
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

jest.mock('@utils/Enum', () => ({
	defaultDataType: ['string', 'int']
}))

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useParams: () => ({ tagName: mockTagName })
}))

jest.mock('@redux/slice/typeDefSlices/typedefClassificationSlice', () => ({
	fetchClassificationData: jest.fn(() => ({ type: 'FETCH_CLASSIFICATION' }))
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

jest.mock('react-hook-form', () => ({
	Controller: ({ render, name }: any) =>
		render({
			field: {
				value: name.includes('toggleDuplicates') ? mockToggleValue : undefined,
				onChange: jest.fn()
			}
		}),
	useFieldArray: () => ({
		fields: mockFields,
		append: appendSpy,
		remove: removeSpy
	}),
	useForm: () => ({
		watch: () => mockFormValues.attributes,
		control: {},
		handleSubmit: (fn: any) => () => fn(mockFormValues),
		register: jest.fn(),
		formState: { isSubmitting: mockIsSubmitting }
	})
}))

jest.mock('@mui/material', () => {
	const actual = jest.requireActual('@mui/material')
	return {
		...actual,
		Stack: ({ children }: any) => <div>{children}</div>,
		TextField: (props: any) => <input {...props} />,
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
		)
	}
})

describe('AddTagAttributes - 100% Coverage', () => {
	const onClose = jest.fn()

	beforeEach(() => {
		jest.clearAllMocks()
		appendSpy.mockClear()
		removeSpy.mockClear()
		mockTagName = 'PII'
		mockToggleValue = true
		mockState = {
			enum: {
				enumObj: {
					data: {
						enumDefs: [{ name: 'EnumType', guid: '1' }]
					}
				}
			},
			classification: {
				classificationData: {
					classificationDefs: [{ name: 'PII', attributeDefs: [] }]
				}
			}
		}
		mockFormValues = {
			attributes: [
				{ attributeName: 'a1', typeName: '', toggleDuplicates: true },
				{ attributeName: 'a2', typeName: 'array<string>', toggleDuplicates: false },
				{ attributeName: 'a3', typeName: 'int' }
			]
		}
		mockIsSubmitting = false
		mockFields = [{ id: '1' }]
	})

	test('adds attributes and handles toggleDuplicates branches', async () => {
		;(createOrUpdateTag as jest.Mock).mockResolvedValueOnce({})

		render(<AddTagAttributes open onClose={onClose} />)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(createOrUpdateTag).toHaveBeenCalled()
		})
	})

	test('handles submit error', async () => {
		;(createOrUpdateTag as jest.Mock).mockRejectedValueOnce(
			new Error('error')
		)

		render(<AddTagAttributes open onClose={onClose} />)

		fireEvent.click(screen.getByText('Submit'))

		await waitFor(() => {
			expect(serverError).toHaveBeenCalled()
		})
	})

	test('renders toggle switch when array type', () => {
		mockFormValues = {
			attributes: [{ typeName: 'array<string>' }]
		}
		render(<AddTagAttributes open onClose={onClose} />)
		expect(screen.getByTestId('ant-switch')).toBeInTheDocument()
	})

	test('handles empty enum list and false toggle label', () => {
		mockState = {
			enum: {
				enumObj: { data: { enumDefs: [] } }
			},
			classification: {
				classificationData: { classificationDefs: [] }
			}
		}
		mockTagName = ''
		mockToggleValue = false
		mockFormValues = {
			attributes: [{ attributeName: 'a1', typeName: 'array<string>' }]
		}

		render(<AddTagAttributes open onClose={onClose} />)

		expect(screen.getByTestId('ant-switch')).toBeInTheDocument()
	})

	test('handles add and remove attribute actions', () => {
		render(<AddTagAttributes open onClose={onClose} />)

		fireEvent.click(screen.getByText('Add New Attributes'))
		fireEvent.click(screen.getByTestId('remove-attr'))

		expect(appendSpy).toHaveBeenCalled()
		expect(removeSpy).toHaveBeenCalled()
	})

	test('does not render toggle when attributes are missing', () => {
		mockFormValues = {}
		render(<AddTagAttributes open onClose={onClose} />)
		expect(screen.queryByTestId('ant-switch')).not.toBeInTheDocument()
	})

	test('does not render toggle for non-array type', () => {
		mockFormValues = {
			attributes: [{ typeName: 'string' }]
		}
		render(<AddTagAttributes open onClose={onClose} />)
		expect(screen.queryByTestId('ant-switch')).not.toBeInTheDocument()
	})

	test('does not render toggle when attributes null', () => {
		mockFormValues = {
			attributes: null
		}
		render(<AddTagAttributes open onClose={onClose} />)
		expect(screen.queryByTestId('ant-switch')).not.toBeInTheDocument()
	})

	test('does not render toggle for empty attributes array', () => {
		mockFormValues = {
			attributes: []
		}
		render(<AddTagAttributes open onClose={onClose} />)
		expect(screen.queryByTestId('ant-switch')).not.toBeInTheDocument()
	})
})
