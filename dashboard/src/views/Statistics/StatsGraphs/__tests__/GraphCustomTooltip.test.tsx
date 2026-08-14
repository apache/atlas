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
 * Unit tests for GraphCustomTooltip.tsx
 * 
 * Coverage Target: 100%
 * - Statements: 100% (10/10)
 * - Branches: 100% (7/7)
 * - Functions: 100% (3/3)
 * - Lines: 100% (10/10)
 */

import React from 'react'
import { render, screen } from '@testing-library/react'
import GraphCustomTooltip from '../GraphCustomTooltip'
import moment from 'moment'

// Mock MUI components
jest.mock('@mui/material', () => ({
	Stack: ({ children, sx, className, ...props }: any) => (
		<div data-testid="stack" style={sx} className={className} {...props}>
			{children}
		</div>
	),
	Typography: ({ children, variant, className }: any) => (
		<div data-testid="typography" data-variant={variant} className={className}>
			{children}
		</div>
	),
	Table: ({ children, size }: any) => (
		<table data-testid="table" data-size={size}>
			{children}
		</table>
	),
	TableBody: ({ children }: any) => <tbody data-testid="table-body">{children}</tbody>,
	TableRow: ({ children }: any) => <tr data-testid="table-row">{children}</tr>,
	TableCell: ({ children, className, sx }: any) => (
		<td data-testid="table-cell" className={className} style={sx}>
			{children}
		</td>
	)
}))

describe('GraphCustomTooltip', () => {
	const mockCoordinate = { x: 100, y: 200 }
	const mockLabel = '2024-01-15T10:30:00Z'
	const mockPayload = [
		{
			name: 'Active',
			value: 50.5,
			color: '#FF0000'
		},
		{
			name: 'Deleted',
			value: 25.75,
			color: '#00FF00'
		},
		{
			name: 'Updated',
			value: 10.25,
			color: '#0000FF'
		}
	]

	describe('Rendering Conditions', () => {
		it('should return null when active is false', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={false}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)
			expect(container.firstChild).toBeNull()
		})

		it('should return null when payload is null', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={true}
					payload={null}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)
			expect(container.firstChild).toBeNull()
		})

		it('should return null when payload is undefined', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={true}
					payload={undefined}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)
			expect(container.firstChild).toBeNull()
		})

		it('should return null when payload is empty array', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={true}
					payload={[]}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)
			expect(container.firstChild).toBeNull()
		})
	})

	describe('Tooltip Rendering', () => {
		it('should render tooltip when active is true and payload has entries', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			const mainStack = container.querySelector('.graph-custom-tooltip')
			expect(mainStack).toBeInTheDocument()
			expect(mainStack).toHaveClass('graph-custom-tooltip')
			expect(mainStack).toHaveAttribute('data-cy', 'graph-custom-tooltip')
		})

		it('should apply correct transform style based on coordinate', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			const stack = container.querySelector('.graph-custom-tooltip')
			expect(stack).toHaveStyle({ transform: 'translate(100px, 200px)' })
		})

		it('should render formatted date label', () => {
			render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			const typography = screen.getByTestId('typography')
			expect(typography).toHaveClass('graph-custom-tooltip-title')
			expect(typography).toHaveAttribute('data-variant', 'subtitle2')
			expect(typography.textContent).toBe(moment(mockLabel).format('MM/DD/YYYY'))
		})

		it('should render table with correct size', () => {
			render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			const table = screen.getByTestId('table')
			expect(table).toHaveAttribute('data-size', 'small')
		})

		it('should render all payload entries as table rows', () => {
			render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			const rows = screen.getAllByTestId('table-row')
			// 3 payload entries + 1 total row = 4 rows
			expect(rows).toHaveLength(4)
		})

		it('should render entry details correctly', () => {
			render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			// Check first entry
			expect(screen.getByText('Active')).toBeInTheDocument()
			expect(screen.getByText('50.50')).toBeInTheDocument()

			// Check second entry
			expect(screen.getByText('Deleted')).toBeInTheDocument()
			expect(screen.getByText('25.75')).toBeInTheDocument()

			// Check third entry
			expect(screen.getByText('Updated')).toBeInTheDocument()
			expect(screen.getByText('10.25')).toBeInTheDocument()
		})

		it('should render color indicator for each entry', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			const colorIndicators = container.querySelectorAll('.graph-custom-tooltip-icon')
			expect(colorIndicators).toHaveLength(3)
		})

		it('should calculate and display total correctly', () => {
			render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			// Total should be 50.5 + 25.75 + 10.25 = 86.5
			expect(screen.getByText('TOTAL')).toBeInTheDocument()
			expect(screen.getByText('86.50')).toBeInTheDocument()
		})

		it('should render total row with correct classes', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			const totalCell = container.querySelector('.graph-custom-tooltip-total')
			const totalValueCell = container.querySelector('.graph-custom-tooltip-total-value')

			expect(totalCell).toBeInTheDocument()
			expect(totalValueCell).toBeInTheDocument()
		})
	})

	describe('Edge Cases', () => {
		it('should handle single payload entry', () => {
			const singlePayload = [
				{
					name: 'Single',
					value: 100.123,
					color: '#FF00FF'
				}
			]

			render(
				<GraphCustomTooltip
					active={true}
					payload={singlePayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			expect(screen.getByText('Single')).toBeInTheDocument()
			// Value appears twice - once in entry row and once in total row
			const valueElements = screen.getAllByText('100.12')
			expect(valueElements.length).toBeGreaterThanOrEqual(1)
			expect(screen.getByText('TOTAL')).toBeInTheDocument()
		})

		it('should handle zero values in payload', () => {
			const zeroPayload = [
				{
					name: 'Zero',
					value: 0,
					color: '#000000'
				}
			]

			render(
				<GraphCustomTooltip
					active={true}
					payload={zeroPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			expect(screen.getByText('Zero')).toBeInTheDocument()
			// Value appears twice - once in entry row and once in total row
			const valueElements = screen.getAllByText('0.00')
			expect(valueElements.length).toBeGreaterThanOrEqual(1)
			expect(screen.getByText('TOTAL')).toBeInTheDocument()
		})

		it('should handle negative values in payload', () => {
			const negativePayload = [
				{
					name: 'Negative',
					value: -10.5,
					color: '#FF0000'
				}
			]

			render(
				<GraphCustomTooltip
					active={true}
					payload={negativePayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			expect(screen.getByText('Negative')).toBeInTheDocument()
			// Value appears twice - once in entry row and once in total row
			const valueElements = screen.getAllByText('-10.50')
			expect(valueElements.length).toBeGreaterThanOrEqual(1)
			expect(screen.getByText('TOTAL')).toBeInTheDocument()
		})

		it('should handle decimal values correctly', () => {
			const decimalPayload = [
				{
					name: 'Decimal',
					value: 99.999,
					color: '#00FF00'
				}
			]

			render(
				<GraphCustomTooltip
					active={true}
					payload={decimalPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)

			expect(screen.getByText('Decimal')).toBeInTheDocument()
			// Value appears twice - once in entry row and once in total row
			// toFixed(2) rounds 99.999 to 100.00
			const valueElements = screen.getAllByText('100.00')
			expect(valueElements.length).toBeGreaterThanOrEqual(1)
			expect(screen.getByText('TOTAL')).toBeInTheDocument()
		})

		it('should handle different coordinate values', () => {
			const customCoordinate = { x: 0, y: 0 }

			const { container } = render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={mockLabel}
					coordinate={customCoordinate}
				/>
			)

			const stack = container.querySelector('.graph-custom-tooltip')
			expect(stack).toHaveStyle({ transform: 'translate(0px, 0px)' })
		})

		it('should handle different date formats', () => {
			const differentDate = '2023-12-25T00:00:00Z'

			render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={differentDate}
					coordinate={mockCoordinate}
				/>
			)

			const typography = screen.getByTestId('typography')
			expect(typography.textContent).toBe(moment(differentDate).format('MM/DD/YYYY'))
		})
	})

	describe('Branch Coverage', () => {
		it('should handle active=false && payload exists', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={false}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)
			expect(container.firstChild).toBeNull()
		})

		it('should handle active=true && payload is null', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={true}
					payload={null}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)
			expect(container.firstChild).toBeNull()
		})

		it('should handle active=true && payload.length is 0', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={true}
					payload={[]}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)
			expect(container.firstChild).toBeNull()
		})

		it('should handle active=true && payload.length > 0', () => {
			const { container } = render(
				<GraphCustomTooltip
					active={true}
					payload={mockPayload}
					label={mockLabel}
					coordinate={mockCoordinate}
				/>
			)
			const stack = container.querySelector('.graph-custom-tooltip')
			expect(stack).toBeInTheDocument()
		})
	})
})
