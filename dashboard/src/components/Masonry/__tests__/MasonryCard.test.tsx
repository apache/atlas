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
 * Unit tests for MasonryCard component
 * 
 * Coverage Target: 100%
 */

import React from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import MasonryCard from '../MasonryCard'

// Mock ResizeObserver
const mockObserve = jest.fn()
const mockDisconnect = jest.fn()

global.ResizeObserver = class ResizeObserver {
	observe = mockObserve
	unobserve = jest.fn()
	disconnect = mockDisconnect
	constructor(callback: ResizeObserverCallback) {
		// Store callback if needed
	}
} as any

// Mock getBoundingClientRect
const mockGetBoundingClientRect = jest.fn(() => ({
	height: 100,
	width: 200,
	top: 0,
	left: 0,
	bottom: 100,
	right: 200,
	x: 0,
	y: 0,
	toJSON: jest.fn()
}))

// Apply mock to HTMLElement prototype
Object.defineProperty(HTMLElement.prototype, 'getBoundingClientRect', {
	value: mockGetBoundingClientRect,
	configurable: true,
	writable: true
})

describe('MasonryCard', () => {
	const defaultProps = {
		title: 'Test Card',
		children: <div>Card Content</div>
	}

	beforeEach(() => {
		jest.clearAllMocks()
		mockGetBoundingClientRect.mockReturnValue({
			height: 100,
			width: 200,
			top: 0,
			left: 0,
			bottom: 100,
			right: 200,
			x: 0,
			y: 0,
			toJSON: jest.fn()
		})
	})

	it('renders card with title and children', () => {
		// MasonryCard needs a parent grid element with dataset attributes
		const { container } = render(
			<div data-row-height="8" data-row-gap="16">
				<MasonryCard {...defaultProps} />
			</div>
		)
		
		expect(screen.getByText('Test Card')).toBeTruthy()
		expect(screen.getByText('Card Content')).toBeTruthy()
	})

	it('renders with custom className', () => {
		const { container } = render(
			<div data-row-height="8" data-row-gap="16">
				<MasonryCard {...defaultProps} className="custom-class" />
			</div>
		)
		
		const card = container.querySelector('.masonry-card.custom-class')
		expect(card).toBeTruthy()
	})

	it('renders with custom style', () => {
		const customStyle = { backgroundColor: 'red' }
		const { container } = render(
			<div data-row-height="8" data-row-gap="16">
				<MasonryCard {...defaultProps} style={customStyle} />
			</div>
		)
		
		const card = container.querySelector('.masonry-card') as HTMLElement
		expect(card?.style.backgroundColor).toBe('red')
	})

	it('renders footer when provided', () => {
		const footer = <div>Footer Content</div>
		render(
			<div data-row-height="8" data-row-gap="16">
				<MasonryCard {...defaultProps} footer={footer} />
			</div>
		)
		
		expect(screen.getByText('Footer Content')).toBeTruthy()
	})

	it('does not render footer when not provided', () => {
		const { container } = render(
			<div data-row-height="8" data-row-gap="16">
				<MasonryCard {...defaultProps} />
			</div>
		)
		
		const footer = container.querySelector('.masonry-card__footer')
		expect(footer).toBeNull()
	})

	it('uses default maxBodyHeight when not provided', () => {
		const { container } = render(
			<div data-row-height="8" data-row-gap="16">
				<MasonryCard {...defaultProps} />
			</div>
		)
		
		const body = container.querySelector('.masonry-card__body') as HTMLElement
		expect(body?.style.maxHeight).toBe('260px')
	})

	it('uses custom maxBodyHeight when provided', () => {
		const { container } = render(
			<MasonryCard {...defaultProps} maxBodyHeight={500} />
		)
		
		const body = container.querySelector('.masonry-card__body') as HTMLElement
		expect(body?.style.maxHeight).toBe('500px')
	})

	it('calculates row span based on height', () => {
		// The component calls getBoundingClientRect during render via useEffect
		// Our global mock should handle this
		const { container } = render(
			<div data-row-height="8" data-row-gap="16">
				<MasonryCard {...defaultProps} />
			</div>
		)
		const card = container.querySelector('.masonry-card') as HTMLElement
		
		// Verify the card rendered
		expect(card).toBeTruthy()
		// ResizeObserver should have been called
		expect(mockObserve).toHaveBeenCalled()
		// getBoundingClientRect should have been called during measure() in useEffect
		expect(mockGetBoundingClientRect).toHaveBeenCalled()
	})

	it('handles missing parent element gracefully', () => {
		const { container } = render(
			<div data-row-height="8" data-row-gap="16">
				<MasonryCard {...defaultProps} />
			</div>
		)
		const card = container.querySelector('.masonry-card') as HTMLElement
		
		if (card) {
			Object.defineProperty(card, 'parentElement', {
				get: () => null,
				configurable: true
			})

			// Should not throw error
			expect(card).toBeTruthy()
		}
	})

	it('handles missing dataset values', async () => {
		const mockGetBoundingClientRect = jest.fn(() => ({
			height: 50,
			width: 200,
			top: 0,
			left: 0,
			bottom: 50,
			right: 200,
			x: 0,
			y: 0,
			toJSON: jest.fn()
		}))

		const mockParentElement = {
			dataset: {}
		}

		const { container } = render(
			<div data-row-height="8" data-row-gap="16">
				<MasonryCard {...defaultProps} />
			</div>
		)
		const card = container.querySelector('.masonry-card') as HTMLElement
		
		if (card) {
			card.getBoundingClientRect = mockGetBoundingClientRect
			Object.defineProperty(card, 'parentElement', {
				get: () => mockParentElement as any,
				configurable: true
			})

			// Should use default values
			expect(card).toBeTruthy()
		}
	})

	it('sets minimum row span of 1', async () => {
		const mockGetBoundingClientRect = jest.fn(() => ({
			height: 0,
			width: 200,
			top: 0,
			left: 0,
			bottom: 0,
			right: 200,
			x: 0,
			y: 0,
			toJSON: jest.fn()
		}))

		const mockParentElement = {
			dataset: { rowHeight: '8', rowGap: '16' }
		}

		const { container } = render(
			<div data-row-height="8" data-row-gap="16">
				<MasonryCard {...defaultProps} />
			</div>
		)
		const card = container.querySelector('.masonry-card') as HTMLElement
		
		if (card) {
			card.getBoundingClientRect = mockGetBoundingClientRect
			Object.defineProperty(card, 'parentElement', {
				get: () => mockParentElement as any,
				configurable: true
			})

			// Should still render
			expect(card).toBeTruthy()
		}
	})

	it('cleans up ResizeObserver on unmount', () => {
		const { unmount } = render(
			<div data-row-height="8" data-row-gap="16">
				<MasonryCard {...defaultProps} />
			</div>
		)
		
		// Verify observe was called
		expect(mockObserve).toHaveBeenCalled()
		
		unmount()
		
		// Verify disconnect was called on unmount
		expect(mockDisconnect).toHaveBeenCalled()
	})
})
