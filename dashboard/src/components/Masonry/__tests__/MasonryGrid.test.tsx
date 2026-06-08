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
 * Unit tests for MasonryGrid component
 * 
 * Coverage Target: 100%
 */

import React from 'react'
import { render, screen } from '@testing-library/react'
import MasonryGrid from '../MasonryGrid'

describe('MasonryGrid', () => {
	const defaultProps = {
		children: <div>Grid Content</div>
	}

	it('renders grid with children', () => {
		render(<MasonryGrid {...defaultProps} />)
		
		expect(screen.getByText('Grid Content')).toBeTruthy()
	})

	it('renders with default props', () => {
		const { container } = render(<MasonryGrid {...defaultProps} />)
		
		const grid = container.querySelector('.masonry-grid') as HTMLElement
		expect(grid).toBeTruthy()
		expect(grid?.style.display).toBe('grid')
		expect(grid?.style.gridTemplateColumns).toContain('280px')
		expect(grid?.style.gridAutoRows).toBe('8px')
		expect(grid?.getAttribute('data-row-height')).toBe('8')
		expect(grid?.getAttribute('data-row-gap')).toBe('16')
	})

	it('renders with custom minColumnWidth', () => {
		const { container } = render(
			<MasonryGrid {...defaultProps} minColumnWidth={400} />
		)
		
		const grid = container.querySelector('.masonry-grid') as HTMLElement
		expect(grid?.style.gridTemplateColumns).toContain('400px')
	})

	it('renders with custom columnGap', () => {
		const { container } = render(
			<MasonryGrid {...defaultProps} columnGap={24} />
		)
		
		const grid = container.querySelector('.masonry-grid') as HTMLElement
		expect(grid?.style.columnGap).toBe('24px')
	})

	it('renders with custom rowGap', () => {
		const { container } = render(
			<MasonryGrid {...defaultProps} rowGap={20} />
		)
		
		const grid = container.querySelector('.masonry-grid') as HTMLElement
		expect(grid?.style.rowGap).toBe('20px')
		expect(grid?.getAttribute('data-row-gap')).toBe('20')
	})

	it('renders with custom rowHeight', () => {
		const { container } = render(
			<MasonryGrid {...defaultProps} rowHeight={10} />
		)
		
		const grid = container.querySelector('.masonry-grid') as HTMLElement
		expect(grid?.style.gridAutoRows).toBe('10px')
		expect(grid?.getAttribute('data-row-height')).toBe('10')
	})

	it('renders with custom className', () => {
		const { container } = render(
			<MasonryGrid {...defaultProps} className="custom-grid" />
		)
		
		const grid = container.querySelector('.masonry-grid.custom-grid')
		expect(grid).toBeTruthy()
	})

	it('renders with custom style', () => {
		const customStyle = { backgroundColor: 'blue', padding: '20px' }
		const { container } = render(
			<MasonryGrid {...defaultProps} style={customStyle} />
		)
		
		const grid = container.querySelector('.masonry-grid') as HTMLElement
		expect(grid?.style.backgroundColor).toBe('blue')
		expect(grid?.style.padding).toBe('20px')
	})

	it('merges custom style with default grid styles', () => {
		const customStyle = { backgroundColor: 'red' }
		const { container } = render(
			<MasonryGrid {...defaultProps} style={customStyle} />
		)
		
		const grid = container.querySelector('.masonry-grid') as HTMLElement
		expect(grid?.style.display).toBe('grid')
		expect(grid?.style.backgroundColor).toBe('red')
	})

	it('sets gridAutoFlow to dense', () => {
		const { container } = render(<MasonryGrid {...defaultProps} />)
		
		const grid = container.querySelector('.masonry-grid') as HTMLElement
		expect(grid?.style.gridAutoFlow).toBe('dense')
	})

	it('renders multiple children', () => {
		render(
			<MasonryGrid>
				<div>Child 1</div>
				<div>Child 2</div>
				<div>Child 3</div>
			</MasonryGrid>
		)
		
		expect(screen.getByText('Child 1')).toBeTruthy()
		expect(screen.getByText('Child 2')).toBeTruthy()
		expect(screen.getByText('Child 3')).toBeTruthy()
	})
})
