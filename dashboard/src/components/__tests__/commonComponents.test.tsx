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
 * Unit tests for commonComponents exports
 */

import React from 'react'
import { render, screen } from '@testing-library/react'
let mockReduxState = { typeHeader: { typeHeaderData: [] } }

jest.mock('react-redux', () => ({
	useSelector: (fn: any) => fn(mockReduxState)
}))

jest.mock('react-router-dom', () => ({
	Link: ({ children, to }: any) => (
		<a href={to?.pathname || ''}>{children}</a>
	),
	useLocation: () => ({ search: '' }),
	useNavigate: () => jest.fn()
}))

jest.mock('../../api/apiMethods/detailpageApiMethod', () => ({
	getDetailPageData: jest.fn(() => Promise.resolve({ data: {} }))
}))

jest.mock('../muiComponents', () => ({
	IconButton: ({ children }: any) => <button>{children}</button>
}))

jest.mock('../../utils/CommonViewFunction', () => ({
	JSONPrettyPrint: () => '<span>json</span>',
	getValue: (val: any) => String(val)
}))

jest.mock('../../utils/Enum', () => ({
	entityStateReadOnly: { ACTIVE: true },
	filterQueryValue: {},
	queryBuilderDateRangeUIValueToAPI: {},
	systemAttributes: {}
}))

jest.mock('../../utils/Utils', () => ({
	dateFormat: 'YYYY',
	extractKeyValueFromEntity: (
		input: any,
		_p1?: any,
		_p2?: any,
		getGuid?: (guid: string) => void
	) => {
		if (getGuid && input?.guid) {
			getGuid(input.guid)
		}
		return { name: input?.name || 'Entity' }
	},
	formatedDate: () => 'formatted-date',
	escapeHtml: (s: string) => String(s),
	isArray: (val: any) => Array.isArray(val),
	isBoolean: (val: any) => typeof val === 'boolean',
	isEmpty: (val: any) =>
		val == null ||
		(Array.isArray(val) ? val.length === 0 :
			typeof val === 'object' ? Object.keys(val).length === 0 :
			val === ''),
	isFunction: (val: any) => typeof val === 'function',
	isNumber: (val: any) => typeof val === 'number',
	isObject: (val: any) =>
		val !== null && typeof val === 'object' && !Array.isArray(val),
	isString: (val: any) => typeof val === 'string'
}))

jest.mock('moment', () => {
	const moment: any = () => ({ isValid: () => true })
	return moment
})


const {
	EllipsisText,
	ExtractObject,
	GetArrayValue,
	getValues,
	GetNumberSuffix
} = require('../commonComponents')

describe('commonComponents', () => {
	it('renders EllipsisText children', () => {
		render(
			<EllipsisText>
				<span>Child</span>
			</EllipsisText>
		)

		expect(screen.getByText('Child')).toBeTruthy()
	})

	it('renders ExtractObject with primitive values', () => {
		const { container } = render(
			<ExtractObject keyValue={['abc', true, 1]} properties="props" />
		)

		const pre = container.querySelector('pre')
		expect(pre).toBeTruthy()
	})



	it('renders struct attributes when category is STRUCT', () => {
		mockReduxState = {
			typeHeader: {
				typeHeaderData: [
					{ name: 'StructType', category: 'STRUCT' }
				]
			}
		}

		render(
			<ExtractObject
				keyValue={[{
					attributes: { foo: 'bar' },
					typeName: 'StructType',
					name: 'Struct',
					status: 'ACTIVE'
				}]}
			/>
		)

		expect(screen.getByText('json')).toBeTruthy()
	})

	it('renders ExtractObject link and delete icon for read-only', () => {
		render(
			<ExtractObject
				keyValue={[{
					guid: 'g1',
					name: 'EntityName',
					status: 'ACTIVE',
					typeName: 'DataSet'
				}]}
			/>
		)

		expect(screen.getByText('EntityName')).toBeTruthy()
	})

	it('renders glossary term link without delete icon', () => {
		const { container } = render(
			<ExtractObject
				keyValue={[{
					guid: 't1',
					name: 'Term1',
					status: 'ACTIVE',
					typeName: 'AtlasGlossaryTerm'
				}]}
			/>
		)

		expect(screen.getByText('Term1')).toBeTruthy()
		expect(container.querySelector('.delete-icon')).toBeNull()
	})

	it('renders JSON pretty for struct object', () => {
		const { container } = render(
			<ExtractObject
				keyValue={[{
					attributes: { a: 1 },
					typeName: 'StructType',
					name: 'Struct',
					status: 'ACTIVE'
				}]}
			/>
		)

		expect(container.innerHTML).toContain('json')
	})



	it('renders N/A for skipped $ values', () => {
	render(<ExtractObject keyValue={['$skip']} />)
	expect(screen.getByText('N/A')).toBeTruthy()
	})

	it('returns N/A when no value and no link', () => {
		render(<ExtractObject keyValue={[]} />)
		expect(screen.getByText('N/A')).toBeTruthy()
	})

	it('renders GetArrayValue with objects and strings', () => {
		render(
			<GetArrayValue
				values={[{ guid: 'g1', name: 'Obj' }, 'plain']}
				properties="props"
			/>
		)

		expect(screen.getByText('Obj')).toBeTruthy()
		expect(screen.getByText('plain')).toBeTruthy()
	})

	it('returns NA when GetArrayValue is empty', () => {
		render(<GetArrayValue values={[]} />)
		expect(screen.getByText('NA')).toBeTruthy()
	})

	it('getValues handles profileData', () => {
		const result = getValues(
			{
				row: { original: { attributes: { attr: 'profileData' } } },
				getValue: () => 'profileData'
			},
			{},
			{ name: 'attr' }
		)
		expect(result).toBeUndefined()
	})

	it('getValues renders date for entity type', () => {
		const result = getValues(
			{
				row: { original: { attributes: { attr: 1690000000000 } } },
				getValue: () => 1690000000000
			},
			{ typeName: 'date' },
			{ name: 'attr', typeName: 'date' }
		)

		const { container } = render(<>{result}</>)
		expect(container.textContent).toContain('formatted-date')
	})

	it('getValues renders object with ExtractObject', () => {
		const result = getValues(
			{
				row: { original: { attributes: { attr: { a: 1 } } } },
				getValue: () => ({ a: 1 })
			},
			{ typeName: 'string' },
			{ name: 'attr', typeName: 'string' }
		)

		const { container } = render(<>{result}</>)
		expect(container.textContent).toContain('json')
	})

	it('getValues renders array with GetArrayValue', () => {
		const result = getValues(
			{ getValue: () => ['a', 'b'] },
			{ typeName: 'string' },
			{ name: 'attr', typeName: 'string' }
		)

		const { container } = render(<>{result}</>)
		expect(container.textContent).toContain('a')
	})

	it('getValues renders boolean', () => {
		const result = getValues(
			true,
			{},
			{ name: 'attr' },
			undefined,
			'props'
		)
		const { container } = render(<>{result}</>)
		expect(container.textContent).toContain('true')
	})

	it('getValues renders string', () => {
		const result = getValues(
			{ getValue: () => 'text' },
			{},
			{ name: 'attr' }
		)
		const { container } = render(<>{result}</>)
		expect(container.textContent).toContain('text')
	})

	it('getValues renders number date', () => {
		const result = getValues(
			123,
			{},
			{ name: 'attr', attributeDefs: [{ name: 'attr', typeName: 'date' }] },
			undefined,
			'props',
			undefined,
			undefined,
			'attr'
		)
		const { container } = render(<>{result}</>)
		expect(container.textContent).toContain('formatted-date')
	})

	it('getValues renders fallback for empty', () => {
		const result = getValues(
			{ getValue: () => '' },
			{},
			{ name: 'attr' }
		)
		const { container } = render(<>{result}</>)
		expect(container.textContent).toContain('N/A')
	})

	it('renders GetNumberSuffix with sup and without', () => {
		const { container } = render(
			<>
				<GetNumberSuffix number={1} sup={true} />
				<GetNumberSuffix number={2} />
			</>
		)

		expect(container.textContent).toContain('1')
		expect(container.textContent).toContain('2nd')
	})
})
