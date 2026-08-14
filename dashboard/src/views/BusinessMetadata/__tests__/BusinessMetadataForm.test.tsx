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
import BusinessMetadataForm from '../BusinessMetadataForm'
import { createEditBusinessMetadata } from '@api/apiMethods/typeDefApiMethods'
import { fetchBusinessMetaData } from '@redux/slice/typeDefSlices/typedefBusinessMetadataSlice'
import { setEditBMAttribute } from '@redux/slice/createBMSlice'
import { serverError } from '@utils/Utils'
import { toast } from 'react-toastify'
import { getTypeName } from '@utils/CommonViewFunction'

const mockDispatch = jest.fn()
let mockState: any = {}
let mockFormValues: any = {}
let mockIsSubmitting = false
const mockAppend = jest.fn()
const mockRemove = jest.fn()
const mockReset = jest.fn()
const mockSetValue = jest.fn()
let mockFields: any[] = []
let mockWatchedValue: any = []

jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => selector(mockState),
	useAppDispatch: () => mockDispatch
}))

jest.mock('@api/apiMethods/typeDefApiMethods', () => ({
	createEditBusinessMetadata: jest.fn()
}))

jest.mock('@redux/slice/typeDefSlices/typedefBusinessMetadataSlice', () => ({
	fetchBusinessMetaData: jest.fn(() => ({ type: 'FETCH_BM' }))
}))

jest.mock('@redux/slice/createBMSlice', () => ({
	setEditBMAttribute: jest.fn(() => ({ type: 'SET_EDIT_BM' }))
}))

jest.mock('@utils/Helper', () => ({
	cloneDeep: (obj: any) => JSON.parse(JSON.stringify(obj))
}))

jest.mock('@utils/CommonViewFunction', () => ({
	getTypeName: jest.fn(() => 'string')
}))

jest.mock('@utils/Enum', () => ({
	defaultType: ['string', 'int', 'boolean']
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

jest.mock('react-quill-new', () => (props: any) => (
	<div
		data-testid="react-quill"
		onClick={() => props.onChange?.('formatted text')}
	>
		{props.value}
	</div>
))

jest.mock('@components/muiComponents', () => ({
	CustomButton: ({ children, onClick, ...rest }: any) => (
		<button type="button" onClick={onClick} {...rest}>
			{children}
		</button>
	),
	LightTooltip: ({ children }: any) => <div>{children}</div>
}))

jest.mock('../BusinessMetadataAtrributeForm', () => () => (
	<div data-testid="bm-attribute-form" />
))

jest.mock('react-hook-form', () => ({
	Controller: ({ render }: any) =>
		render({
			field: {
				onChange: jest.fn(),
				value: ''
			},
			fieldState: { error: undefined }
		}),
	useForm: () => ({
		control: {},
		handleSubmit: (fn: any) => (e: any) => {
			e?.preventDefault?.()
			return fn(mockFormValues)
		},
		setValue: mockSetValue,
		watch: jest.fn(() => mockWatchedValue),
		reset: mockReset,
		formState: { isSubmitting: mockIsSubmitting }
	}),
	useFieldArray: () => ({
		fields: mockFields,
		append: mockAppend,
		remove: mockRemove
	})
}))

jest.mock('react-toastify', () => ({
	toast: {
		info: jest.fn(),
		success: jest.fn()
	}
}))

const setupState = (overrides: any = {}) => {
	mockState = {
		createBM: {
			editbmAttribute: {}
		},
		typeHeader: {
			typeHeaderData: [
				{ category: 'ENTITY', name: 'DataSet' },
				{ category: 'CLASSIFICATION', name: 'PII' }
			]
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

describe('BusinessMetadataForm - 100% Coverage', () => {
	const setForm = jest.fn()
	const setBMAttribute = jest.fn()

	beforeEach(() => {
		jest.clearAllMocks()
		setupState()
		mockIsSubmitting = false
		mockFields = []
		mockWatchedValue = []
	})

	test('renders create form and toggles description mode', () => {
		mockFormValues = {
			name: '',
			description: '',
			attributeDefs: []
		}

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		expect(screen.getByText('Create Business Metadata')).toBeInTheDocument()
		expect(screen.getByText('Create')).toBeInTheDocument()
		expect(screen.getByTestId('react-quill')).toBeInTheDocument()

		fireEvent.change(screen.getByPlaceholderText('Name required'), {
			target: { value: 'BM2' }
		})
		fireEvent.click(screen.getByTestId('react-quill'))
		expect(mockSetValue).toHaveBeenCalledWith(
			'description',
			'formatted text'
		)

		fireEvent.click(screen.getByText('Plain text'))
		const textarea = screen.getByPlaceholderText('Long Description')
		fireEvent.change(textarea, { target: { value: 'plain text' } })
		expect(mockSetValue).toHaveBeenCalledWith('description', 'plain text')
	})

	test('shows toast when name is missing on create', async () => {
		mockFormValues = {
			name: '',
			description: '',
			attributeDefs: []
		}

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		fireEvent.click(screen.getByText('Create'))

		await waitFor(() => {
			expect(toast.info).toHaveBeenCalledWith(
				'Please enter the Enumeration name'
			)
		})
		expect(createEditBusinessMetadata).not.toHaveBeenCalled()
	})

	test('submits create request with all attribute branches', async () => {
		mockFormValues = {
			name: 'BM1',
			description: 'desc',
			attributeDefs: [
				{
					name: 'attr1',
					typeName: 'string',
					multiValueSelect: false,
					cardinality: 'SINGLE',
					cardinalityToggle: 'SET',
					enumType: '',
					enumValues: [],
					options: {
						applicableEntityTypes: ['DataSet'],
						maxStrLength: 50
					}
				},
				{
					name: 'attr2',
					typeName: 'string',
					multiValueSelect: true,
					cardinality: 'LIST',
					cardinalityToggle: 'LIST',
					enumType: 'StatusEnum',
					enumValues: [{ value: 'ACTIVE' }],
					options: {
						applicableEntityTypes: ['DataSet'],
						maxStrLength: 100
					}
				},
				{
					name: 'attr3',
					typeName: 'string',
					multiValueSelect: true,
					cardinality: 'SINGLE',
					cardinalityToggle: 'SET',
					enumType: 'EmptyEnum',
					enumValues: [],
					options: {
						applicableEntityTypes: [],
						maxStrLength: 25
					}
				}
			]
		}

		;(createEditBusinessMetadata as jest.Mock).mockResolvedValueOnce({
			data: {
				businessMetadataDefs: [{ name: 'BM1' }]
			}
		})

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		fireEvent.click(screen.getByText('Create'))

		await waitFor(() => {
			expect(createEditBusinessMetadata).toHaveBeenCalledWith(
				'business_metadata',
				'POST',
				expect.any(Object)
			)
		})
		expect(getTypeName).toHaveBeenCalled()
		expect(toast.success).toHaveBeenCalledWith(
			'Business Metadata BM1 was created successfully'
		)
		expect(mockDispatch).toHaveBeenCalledWith(fetchBusinessMetaData())
		expect(setBMAttribute).toHaveBeenCalledWith({})
		expect(setForm).toHaveBeenCalledWith(false)
	})

	test('adds attributes to existing business metadata', async () => {
		mockFormValues = {
			name: 'BM1',
			description: '',
			attributeDefs: [
				{
					name: 'newAttr',
					typeName: 'string',
					multiValueSelect: false,
					cardinality: 'SINGLE',
					cardinalityToggle: 'SET',
					options: {
						applicableEntityTypes: [],
						maxStrLength: 10
					}
				}
			]
		}

		const bmAttribute = {
			name: 'BM1',
			attributeDefs: [{ name: 'existingAttr', typeName: 'string' }]
		}

		;(createEditBusinessMetadata as jest.Mock).mockResolvedValueOnce({
			data: {
				businessMetadataDefs: [{ name: 'BM1' }]
			}
		})

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={bmAttribute}
			/>
		)

		fireEvent.click(screen.getByText('Save'))

		await waitFor(() => {
			expect(createEditBusinessMetadata).toHaveBeenCalledWith(
				'business_metadata',
				'PUT',
				expect.any(Object)
			)
		})
		expect(toast.success).toHaveBeenCalledWith(
			'One or more Business Metadata attributes were updated successfully'
		)
	})

	test('updates matching attribute when editing', async () => {
		setupState({
			createBM: {
				editbmAttribute: {
					name: 'existingAttr',
					typeName: 'string',
					cardinality: 'SINGLE',
					options: {}
				}
			}
		})

		mockFormValues = {
			name: 'BM1',
			description: '',
			attributeDefs: [
				{
					name: 'existingAttr',
					typeName: 'string',
					multiValueSelect: false,
					cardinality: 'SINGLE',
					cardinalityToggle: 'SET',
					options: {
						applicableEntityTypes: [],
						maxStrLength: 10
					}
				}
			]
		}

		const bmAttribute = {
			name: 'BM1',
			attributeDefs: [
				{ name: 'existingAttr', typeName: 'string' },
				{ name: 'otherAttr', typeName: 'string' }
			]
		}

		;(createEditBusinessMetadata as jest.Mock).mockResolvedValueOnce({
			data: {
				businessMetadataDefs: [{ name: 'BM1' }]
			}
		})

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={bmAttribute}
			/>
		)

		fireEvent.click(screen.getByText('Save'))

		await waitFor(() => {
			expect(createEditBusinessMetadata).toHaveBeenCalled()
		})
	})

	test('handles API error on submit', async () => {
		mockFormValues = {
			name: 'BM1',
			description: '',
			attributeDefs: []
		}

		;(createEditBusinessMetadata as jest.Mock).mockRejectedValueOnce(
			new Error('api error')
		)

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		fireEvent.click(screen.getByText('Create'))

		await waitFor(() => {
			expect(serverError).toHaveBeenCalled()
		})
	})

	test('cancel resets form state', () => {
		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		fireEvent.click(screen.getByText('Cancel'))

		expect(setForm).toHaveBeenCalledWith(false)
		expect(setBMAttribute).toHaveBeenCalledWith({})
		expect(mockDispatch).toHaveBeenCalledWith(setEditBMAttribute({}))
	})

	test('shows update title when editing attribute', () => {
		setupState({
			createBM: {
				editbmAttribute: {
					name: 'existingAttr',
					typeName: 'string',
					cardinality: 'SINGLE',
					options: {}
				}
			}
		})

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		expect(screen.getByText('Update Attribute of: existingAttr')).toBeInTheDocument()
	})

	test('disables submit button when submitting', () => {
		mockIsSubmitting = true
		mockFormValues = {
			name: 'BM1',
			description: '',
			attributeDefs: []
		}

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		expect(screen.getByText('Create')).toBeDisabled()
	})

	test('parses applicableEntityTypes JSON for edit attributes', () => {
		setupState({
			createBM: {
				editbmAttribute: {
					name: 'editAttr',
					typeName: 'string',
					options: {
						applicableEntityTypes: '["DataSet"]'
					}
				}
			}
		})

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		expect(screen.getByText('Update Attribute of: editAttr')).toBeInTheDocument()
	})

	test('handles invalid applicableEntityTypes JSON', () => {
		setupState({
			createBM: {
				editbmAttribute: {
					name: 'editAttr',
					typeName: 'string',
					options: {
						applicableEntityTypes: 'invalid-json'
					}
				}
			}
		})

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		expect(screen.getByText('Update Attribute of: editAttr')).toBeInTheDocument()
	})

	test('adds a new attribute when clicking add button', () => {
		mockFormValues = {
			name: '',
			description: '',
			attributeDefs: []
		}

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		fireEvent.click(screen.getByText('Add Business Metadata Attribute'))
		expect(mockAppend).toHaveBeenCalled()
	})

	test('covers typeName parsing branches and empty enum data', () => {
		mockFormValues = {
			name: '',
			description: '',
			attributeDefs: []
		}

		setupState({
			typeHeader: {
				typeHeaderData: []
			},
			enum: {
				enumObj: {}
			},
			createBM: {
				editbmAttribute: {
					name: 'arrayDefault',
					typeName: 'array<string>',
					cardinality: 'SET',
					options: {
						applicableEntityTypes: '[]'
					}
				}
			}
		})

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		setupState({
			enum: {
				enumObj: {
					data: {
						enumDefs: [
							{
								name: 'CustomEnum',
								elementDefs: []
							}
						]
					}
				}
			},
			createBM: {
				editbmAttribute: {
					name: 'arrayEnum',
					typeName: 'array<CustomEnum>',
					cardinality: 'LIST',
					options: {
						applicableEntityTypes: '[]'
					}
				}
			}
		})

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)

		setupState({
			enum: {
				enumObj: {
					data: {
						enumDefs: [
							{
								name: 'CustomEnum',
								elementDefs: []
							}
						]
					}
				}
			},
			createBM: {
				editbmAttribute: {
					name: 'plainEnum',
					typeName: 'CustomEnum',
					cardinality: 'SINGLE',
					options: {
						applicableEntityTypes: '[]'
					}
				}
			}
		})

		render(
			<BusinessMetadataForm
				setForm={setForm}
				setBMAttribute={setBMAttribute}
				bmAttribute={{}}
			/>
		)
	})
})
