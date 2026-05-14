import React from 'react'
import { fireEvent, render, screen } from '@utils/test-utils'
import TagAttributes from '../TagAttributes'

let mockState: any = {}
let mockFields = [{ id: '1' }]

jest.mock('@hooks/reducerHook', () => ({
	useAppSelector: (selector: any) => selector(mockState)
}))

jest.mock('@utils/Enum', () => ({
	defaultDataType: ['string', 'int']
}))

jest.mock('@components/muiComponents', () => ({
	CustomButton: ({ children, onClick }: any) => (
		<button type="button" onClick={onClick}>
			{children}
		</button>
	),
	LightTooltip: ({ children }: any) => <div>{children}</div>
}))

jest.mock('react-hook-form', () => ({
	Controller: ({ render, name }: any) =>
		render({
			field: { onChange: jest.fn(), value: name.includes('typeName') ? 'string' : '' }
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
		TextField: (props: any) => (
			<input {...props} />
		),
		Select: ({ children, onChange }: any) => (
			<select onChange={onChange}>{children}</select>
		),
		MenuItem: ({ children, value }: any) => (
			<option value={value}>{children}</option>
		),
		IconButton: ({ children, onClick }: any) => (
			<button type="button" onClick={onClick}>
				{children}
			</button>
		)
	}
})

describe('TagAttributes - 100% Coverage', () => {
	beforeEach(() => {
		mockState = {
			enum: {
				enumObj: {
					data: {
						enumDefs: [{ name: 'EnumType', guid: '1' }]
					}
				}
			}
		}
		mockFields = [{ id: '1' }]
	})

	test('renders fields and handles interactions', () => {
		render(<TagAttributes control={{}} />)

		fireEvent.click(screen.getByText('Add New Attributes'))
		fireEvent.change(screen.getByPlaceholderText('Attribute Name'), {
			target: { value: 'a1' }
		})
		fireEvent.change(screen.getByRole('combobox'), {
			target: { value: 'int' }
		})
		fireEvent.click(screen.getAllByRole('button')[1])
	})

	test('handles empty enum definitions', () => {
		mockState = {
			enum: {
				enumObj: {
					data: {
						enumDefs: []
					}
				}
			}
		}

		render(<TagAttributes control={{}} />)

		expect(screen.getByText('Add New Attributes')).toBeInTheDocument()
	})
})
