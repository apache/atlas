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
 * Unit tests for ShowMoreText component
 * 
 * Coverage Target: 100%
 */

import React from 'react'
import { render, screen, fireEvent } from '@testing-library/react'
import ShowMoreText from '../ShowMoreText'

jest.mock('@utils/Utils', () => ({
	isEmpty: jest.fn((val: any) => !val || val === ''),
	sanitizeHtmlContent: jest.fn((html: string) => html)
}))

const { isEmpty } = require('@utils/Utils')

describe('ShowMoreText', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		isEmpty.mockImplementation((val: any) => !val || val === '')
	})

	it('renders NA when value is empty', () => {
		render(<ShowMoreText value="" maxLength={160} />)
		
		expect(screen.getByText('NA')).toBeTruthy()
	})

	it('renders full text when length is less than maxLength', () => {
		const shortText = 'Short text'
		render(<ShowMoreText value={shortText} maxLength={160} />)
		
		expect(screen.getByText(shortText)).toBeTruthy()
		expect(screen.queryByText(/show more/i)).toBeNull()
	})

	it('truncates text when length exceeds maxLength', () => {
		const longText = 'A'.repeat(200)
		render(<ShowMoreText value={longText} maxLength={160} />)
		
		const truncated = longText.substring(0, 160) + '...'
		expect(screen.getByText(truncated)).toBeTruthy()
		expect(screen.getByText(/show more/i)).toBeTruthy()
	})

	it('shows "show more" link when text is truncated', () => {
		const longText = 'A'.repeat(200)
		render(<ShowMoreText value={longText} maxLength={160} />)
		
		expect(screen.getByText(/show more/i)).toBeTruthy()
	})

	it('expands text when "show more" is clicked', () => {
		const longText = 'A'.repeat(200)
		render(<ShowMoreText value={longText} maxLength={160} />)
		
		const showMoreLink = screen.getByText(/show more/i)
		fireEvent.click(showMoreLink)
		
		expect(screen.getByText(longText)).toBeTruthy()
		expect(screen.getByText(/show less/i)).toBeTruthy()
	})

	it('collapses text when "show less" is clicked', () => {
		const longText = 'A'.repeat(200)
		render(<ShowMoreText value={longText} maxLength={160} />)
		
		const showMoreLink = screen.getByText(/show more/i)
		fireEvent.click(showMoreLink)
		
		const showLessLink = screen.getByText(/show less/i)
		fireEvent.click(showLessLink)
		
		const truncated = longText.substring(0, 160) + '...'
		expect(screen.getByText(truncated)).toBeTruthy()
		expect(screen.getByText(/show more/i)).toBeTruthy()
	})

	it('uses custom "more" text', () => {
		const longText = 'A'.repeat(200)
		render(
			<ShowMoreText
				value={longText}
				maxLength={160}
				more="Read more..."
			/>
		)
		
		expect(screen.getByText('Read more...')).toBeTruthy()
	})

	it('uses custom "less" text', () => {
		const longText = 'A'.repeat(200)
		render(
			<ShowMoreText
				value={longText}
				maxLength={160}
				less="Read less..."
			/>
		)
		
		const showMoreLink = screen.getByText(/show more/i)
		fireEvent.click(showMoreLink)
		
		// After clicking, it should show the custom "less" text that was passed
		expect(screen.getByText('Read less...')).toBeTruthy()
	})

	it('renders HTML content when isHtml is true', () => {
		const htmlText = '<p>HTML content</p>'
		const { container } = render(
			<ShowMoreText value={htmlText} maxLength={160} isHtml={true} />
		)
		
		const htmlDiv = container.querySelector('.long-descriptions')
		expect(htmlDiv).toBeTruthy()
		expect(htmlDiv?.innerHTML).toBe(htmlText)
	})

	it('renders plain text when isHtml is false', () => {
		const text = 'Plain text content'
		render(<ShowMoreText value={text} maxLength={160} isHtml={false} />)
		
		expect(screen.getByText(text)).toBeTruthy()
	})

	it('handles text exactly at maxLength', () => {
		const exactText = 'A'.repeat(160)
		render(<ShowMoreText value={exactText} maxLength={160} />)
		
		expect(screen.getByText(exactText)).toBeTruthy()
		expect(screen.queryByText(/show more/i)).toBeNull()
	})

	it('handles text one character longer than maxLength', () => {
		const text = 'A'.repeat(161)
		render(<ShowMoreText value={text} maxLength={160} />)
		
		const truncated = 'A'.repeat(160) + '...'
		expect(screen.getByText(truncated)).toBeTruthy()
		expect(screen.getByText(/show more/i)).toBeTruthy()
	})

	it('toggles between truncated and full text multiple times', () => {
		const longText = 'A'.repeat(200)
		render(<ShowMoreText value={longText} maxLength={160} />)
		
		const showMoreLink = screen.getByText(/show more/i)
		fireEvent.click(showMoreLink)
		expect(screen.getByText(longText)).toBeTruthy()
		
		const showLessLink = screen.getByText(/show less/i)
		fireEvent.click(showLessLink)
		const truncated = longText.substring(0, 160) + '...'
		expect(screen.getByText(truncated)).toBeTruthy()
		
		fireEvent.click(showMoreLink)
		expect(screen.getByText(longText)).toBeTruthy()
	})
})
