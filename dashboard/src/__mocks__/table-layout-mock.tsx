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

interface ColumnDefinition {
	id?: string
	accessorKey?: string
	accessorFn?: (row: any) => any
	header?: React.ReactNode | ((context: any) => React.ReactNode)
	cell?: (context: any) => React.ReactNode
}

interface TableLayoutMockProps {
	fetchData?: (args: { pagination: { pageIndex: number; pageSize: number } }) => void
	data?: any[]
	columns?: ColumnDefinition[]
	defaultColumnVisibility?: Record<string, boolean>
	emptyText?: string
	tableFilters?: boolean
	showPagination?: boolean
	pageCount?: number
	totalCount?: number
}

const getColumnKey = (column: ColumnDefinition, index: number) => {
	const baseKey = column.id || column.accessorKey || 'col'
	return `${baseKey}-${index}`
}

const getColumnValue = (row: any, column: ColumnDefinition) => {
	if (typeof column.accessorFn === 'function') {
		return column.accessorFn(row)
	}
	if (column.accessorKey) {
		return row[column.accessorKey]
	}
	return undefined
}

const applyColumnVisibility = (
	columns: ColumnDefinition[],
	defaultColumnVisibility?: Record<string, boolean>,
) => {
	if (!defaultColumnVisibility) return columns
	return columns.filter((column) => {
		const key = column.id || column.accessorKey
		if (!key) return true
		const isVisible = defaultColumnVisibility[key]
		if (isVisible === false) return false
		return true
	})
}

const renderHeader = (column: ColumnDefinition) => {
	if (typeof column.header === 'function') {
		return column.header({})
	}
	return column.header
}

const renderCell = (row: any, column: ColumnDefinition) => {
	const cellValue = getColumnValue(row, column)
	if (typeof column.cell === 'function') {
		return column.cell({
			row: { original: row },
			getValue: () => cellValue,
		})
	}
	if (Array.isArray(cellValue)) {
		return cellValue.join(', ')
	}
	if (cellValue && typeof cellValue === 'object') {
		return JSON.stringify(cellValue)
	}
	return cellValue ?? ''
}

function TableLayout({
	fetchData,
	data = [],
	columns = [],
	defaultColumnVisibility,
	emptyText = 'No Records found!',
	tableFilters,
	showPagination,
	pageCount,
	totalCount,
}: TableLayoutMockProps) {
	const captureColumns = (globalThis as any).__tableLayoutCaptureColumns
	if (typeof captureColumns === 'function') {
		captureColumns(columns)
	}

	useEffect(() => {
		if (typeof fetchData === 'function') {
			fetchData({
				pagination: {
					pageIndex: 0,
					pageSize: 10,
				},
			})
		}
	}, [fetchData])

	const visibleColumns = applyColumnVisibility(columns, defaultColumnVisibility)

	return (
		<div data-testid='table-layout'>
			{tableFilters && (
				<div data-testid='table-filters'>
					TableFilters
					<button
						type='button'
						data-testid='refresh-table-btn'
						onClick={() => {
							if (typeof fetchData === 'function') {
								fetchData({
									pagination: {
										pageIndex: 0,
										pageSize: 10,
									},
								})
							}
						}}
					>
						Refresh
					</button>
				</div>
			)}
			<table role='table' className='table'>
				<thead>
					<tr>
						{visibleColumns.map((column, index) => (
							<th key={getColumnKey(column, index)}>
								{renderHeader(column)}
							</th>
						))}
					</tr>
				</thead>
				<tbody>
					{data.length === 0 ? (
						<tr>
							<td colSpan={Math.max(1, visibleColumns.length)}>
								{emptyText}
							</td>
						</tr>
					) : (
						data.map((row, rowIndex) => (
							<tr key={row.guid || rowIndex}>
								{visibleColumns.map((column, columnIndex) => (
									<td key={`${rowIndex}-${getColumnKey(column, columnIndex)}`}>
										{renderCell(row, column)}
									</td>
								))}
							</tr>
						))
					)}
				</tbody>
			</table>
			{showPagination && (
				<div data-testid='table-pagination'>
					<div data-testid='page-count'>{pageCount || 0}</div>
					<div data-testid='total-count'>{totalCount || 0}</div>
				</div>
			)}
		</div>
	)
}

const IndeterminateCheckbox = (props: any) => (
	<input type='checkbox' {...props} />
)

export { TableLayout, IndeterminateCheckbox }
