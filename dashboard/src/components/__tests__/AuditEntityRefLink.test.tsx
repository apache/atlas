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

import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import AuditEntityRefLink from '../AuditEntityRefLink';

const mockGetDetailPageData = jest.fn();

jest.mock('react-redux', () => ({
	useSelector: (fn: any) => fn({ typeHeader: { typeHeaderData: [] } })
}));

jest.mock('react-router-dom', () => ({
	Link: ({ children, className }: any) => (
		<a className={className}>{children}</a>
	),
	useLocation: () => ({ search: '' })
}));

jest.mock('../../api/apiMethods/detailpageApiMethod', () => ({
	getDetailPageData: (...args: any[]) => mockGetDetailPageData(...args)
}));

jest.mock('../muiComponents', () => ({
	IconButton: ({ children, 'aria-label': ariaLabel }: any) => (
		<button aria-label={ariaLabel}>{children}</button>
	)
}));

jest.mock('../../utils/CommonViewFunction', () => ({
	JSONPrettyPrint: () => '<span>json</span>',
	getValue: (val: any) => String(val)
}));

jest.mock('../../utils/Enum', () => ({
	entityStateReadOnly: {
		ACTIVE: false,
		DELETED: true,
		STATUS_ACTIVE: false,
		STATUS_DELETED: true
	}
}));

jest.mock('../../utils/Utils', () => ({
	extractKeyValueFromEntity: (input: any) => ({
		name: input?.name || 'Entity'
	}),
	escapeHtml: (s: string) => String(s),
	isArray: (val: any) => Array.isArray(val),
	isBoolean: (val: any) => typeof val === 'boolean',
	isEmpty: (val: any) =>
		val == null ||
		(Array.isArray(val) ? val.length === 0 :
			typeof val === 'object' ? Object.keys(val).length === 0 :
			val === ''),
	isNumber: (val: any) => typeof val === 'number',
	isObject: (val: any) =>
		val !== null && typeof val === 'object' && !Array.isArray(val),
	isString: (val: any) => typeof val === 'string'
}));

describe('AuditEntityRefLink', () => {
	beforeEach(() => {
		jest.clearAllMocks();
	});

	it('shows delete icon after header API returns DELETED status', async () => {
		mockGetDetailPageData.mockResolvedValue({
			data: {
				guid: 'cf2-guid',
				name: 'cf2',
				status: 'DELETED',
				entityStatus: 'DELETED'
			}
		});

		const { container } = render(
			<AuditEntityRefLink
				entityRef={{
					guid: 'cf2-guid',
					typeName: 'hbase_column_family'
				}}
				referredEntities={{
					'cf2-guid': {
						guid: 'cf2-guid',
						name: 'cf2'
					}
				}}
			/>
		);

		expect(container.querySelector('.delete-icon')).toBeNull();

		await waitFor(() => {
			expect(container.querySelector('.delete-icon')).toBeTruthy();
		});

		expect(screen.getByText('cf2')).toBeTruthy();
		expect(container.querySelector('.entity-name-deleted')).toBeTruthy();
		expect(mockGetDetailPageData).toHaveBeenCalledWith(
			'cf2-guid',
			{},
			'headers'
		);
	});

	it('keeps snapshot rendering when header API returns empty data', async () => {
		mockGetDetailPageData.mockResolvedValue({ data: null });

		const { container } = render(
			<AuditEntityRefLink
				entityRef={{
					guid: 'cf3-guid',
					typeName: 'hbase_column_family'
				}}
				referredEntities={{
					'cf3-guid': {
						guid: 'cf3-guid',
						name: 'cf3'
					}
				}}
			/>
		);

		await waitFor(() => {
			expect(mockGetDetailPageData).toHaveBeenCalled();
		});

		expect(screen.getByText('cf3')).toBeTruthy();
		expect(container.querySelector('.delete-icon')).toBeNull();
		expect(container.querySelector('.entity-name-deleted')).toBeNull();
		expect(container.querySelector('.text-blue')).toBeTruthy();
	});

	it('shows delete icon when header returns only entityStatus DELETED', async () => {
		mockGetDetailPageData.mockResolvedValue({
			data: {
				guid: 'cf4-guid',
				name: 'cf4',
				entityStatus: 'DELETED'
			}
		});

		const { container } = render(
			<AuditEntityRefLink
				entityRef={{
					guid: 'cf4-guid',
					typeName: 'hbase_column_family'
				}}
				referredEntities={{
					'cf4-guid': {
						guid: 'cf4-guid',
						name: 'cf4'
					}
				}}
			/>
		);

		await waitFor(() => {
			expect(container.querySelector('.delete-icon')).toBeTruthy();
		});

		expect(screen.getByText('cf4')).toBeTruthy();
		expect(container.querySelector('.entity-name-deleted')).toBeTruthy();
	});

	it('shows delete icon when header returns only status DELETED', async () => {
		mockGetDetailPageData.mockResolvedValue({
			data: {
				guid: 'cf5-guid',
				name: 'cf5',
				status: 'DELETED'
			}
		});

		const { container } = render(
			<AuditEntityRefLink
				entityRef={{
					guid: 'cf5-guid',
					typeName: 'hbase_column_family'
				}}
				referredEntities={{
					'cf5-guid': {
						guid: 'cf5-guid',
						name: 'cf5'
					}
				}}
			/>
		);

		await waitFor(() => {
			expect(container.querySelector('.delete-icon')).toBeTruthy();
		});

		expect(screen.getByText('cf5')).toBeTruthy();
		expect(container.querySelector('.entity-name-deleted')).toBeTruthy();
	});

	it('keeps active link styling when header API rejects', async () => {
		mockGetDetailPageData.mockRejectedValue(new Error('Network error'));

		const { container } = render(
			<AuditEntityRefLink
				entityRef={{
					guid: 'cf1-guid',
					typeName: 'hbase_column_family'
				}}
				referredEntities={{
					'cf1-guid': {
						guid: 'cf1-guid',
						name: 'cf1'
					}
				}}
			/>
		);

		await waitFor(() => {
			expect(mockGetDetailPageData).toHaveBeenCalled();
		});

		expect(screen.getByText('cf1')).toBeTruthy();
		expect(container.querySelector('.delete-icon')).toBeNull();
		expect(container.querySelector('.entity-name-deleted')).toBeNull();
		expect(container.querySelector('.text-blue')).toBeTruthy();
	});
});
