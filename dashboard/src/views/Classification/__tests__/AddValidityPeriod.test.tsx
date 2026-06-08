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
import { fireEvent, render, screen } from '@utils/test-utils'
import AddValidityPeriod from '../AddValidityPeriod'

let mockState: any = {}
let mockFields = [{ id: '1' }]
let mockControllerValues: Record<string, any> = {}
let mockControllerErrorNames: Record<string, boolean> = {}

jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => selector(mockState)
}))

jest.mock('@components/muiComponents', () => ({
	CustomButton: ({ children, onClick }: any) => (
		<button type="button" onClick={onClick}>
			{children}
		</button>
	),
	LightTooltip: ({ children }: any) => <div>{children}</div>
}))

jest.mock('@components/DatePicker/CustomDatePicker', () => ({
	__esModule: true,
	default: ({ onChange }: any) => (
		<div>
			<button type="button" onClick={() => onChange?.({ toISOString: () => 'iso' })}>
				ChangeDate
			</button>
			<button type="button" onClick={() => onChange?.(null)}>
				ClearDate
			</button>
		</div>
	)
}))

jest.mock('react-hook-form', () => ({
	Controller: ({ render, name }: any) =>
		render({
			field: {
				onChange: jest.fn(),
				value: mockControllerValues[name],
				ref: jest.fn()
			},
			fieldState: {
				error: mockControllerErrorNames[name] ? { message: 'error' } : undefined
			}
		}),
	useFieldArray: () => ({
		fields: mockFields,
		append: jest.fn(),
		remove: jest.fn()
	})
}))

jest.mock('@mui/material', () => {
	const actual = jest.requireActual('@mui/material')
	return {
		...actual,
		Stack: ({ children }: any) => <div>{children}</div>,
		InputLabel: ({ children }: any) => <label>{children}</label>,
		TextField: (props: any) => <input {...props} />,
		Autocomplete: ({
			renderInput,
			onChange,
			options,
			getOptionLabel,
			isOptionEqualToValue
		}: any) => {
			const firstOption = options?.[0]
			getOptionLabel?.(firstOption)
			getOptionLabel?.(undefined)
			if (firstOption) {
				isOptionEqualToValue?.(firstOption, firstOption)
			}
			return (
				<div>
					{renderInput?.({ InputProps: {}, inputProps: {} })}
					<button
						type="button"
						onClick={() => onChange?.({}, options?.[0])}
					>
						Select
					</button>
				</div>
			)
		},
		IconButton: ({ children, onClick }: any) => (
			<button type="button" data-testid="remove-period" onClick={onClick}>
				{children}
			</button>
		),
		Card: ({ children }: any) => <div>{children}</div>,
		CardContent: ({ children }: any) => <div>{children}</div>
	}
})

describe('AddValidityPeriod - 100% Coverage', () => {
	beforeEach(() => {
		mockState = {
			session: {
				sessionObj: {
					data: {
						timezones: ['UTC', 'EST']
					}
				}
			}
		}
		mockFields = [{ id: '1' }]
		mockControllerValues = {
			'validityPeriod.0.startTime': '2020-01-01T00:00:00Z',
			'validityPeriod.0.endTime': 'invalid',
			'validityPeriod.0.timeZone': { label: 'UTC', value: 'UTC' }
		}
		mockControllerErrorNames = {
			'validityPeriod.0.timeZone': true
		}
	})

	test('renders validity period rows and handles interactions', () => {
		render(<AddValidityPeriod control={{}} />)

		fireEvent.click(screen.getAllByText('ChangeDate')[0])
		fireEvent.click(screen.getAllByText('ClearDate')[0])
		fireEvent.click(screen.getAllByText('ChangeDate')[1])
		fireEvent.click(screen.getAllByText('ClearDate')[1])
		fireEvent.click(screen.getByText('Select'))
		fireEvent.click(screen.getByText('Add Validity Period'))
		fireEvent.click(screen.getByTestId('remove-period'))
	})

	test('renders with empty timezones and no errors', () => {
		mockState = {
			session: {
				sessionObj: {
					data: {
						timezones: []
					}
				}
			}
		}
		mockControllerValues = {
			'validityPeriod.0.startTime': 'invalid',
			'validityPeriod.0.endTime': '2020-01-01T00:00:00Z',
			'validityPeriod.0.timeZone': { label: 'UTC', value: 'UTC' }
		}
		mockControllerErrorNames = {}

		render(<AddValidityPeriod control={{}} />)

		expect(screen.getByText('Add Validity Period')).toBeInTheDocument()
	})

	test('handles missing session data', () => {
		mockState = {
			session: {
				sessionObj: {}
			}
		}
		mockControllerValues = {
			'validityPeriod.0.startTime': '2020-01-01T00:00:00Z',
			'validityPeriod.0.endTime': '2020-01-01T00:00:00Z',
			'validityPeriod.0.timeZone': { label: 'UTC', value: 'UTC' }
		}
		mockControllerErrorNames = {}

		render(<AddValidityPeriod control={{}} />)

		expect(screen.getByText('Add Validity Period')).toBeInTheDocument()
	})
})
