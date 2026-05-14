/**
 * Unit tests for HtmlRenderer component
 */

import React from 'react'
import { render } from '@testing-library/react'
import HtmlRenderer from '../HtmlRenderer'

describe('HtmlRenderer', () => {
	it('renders sanitized html string content', () => {
		const htmlString = '<p>Hello</p>'
		const { container } = render(<HtmlRenderer htmlString={htmlString} />)

		const root = container.querySelector('.html-content')
		const paragraph = root?.querySelector('p')
		expect(paragraph).toBeTruthy()
		expect(paragraph?.textContent).toBe('Hello')
	})

	it('updates innerHTML when htmlString prop changes', () => {
		const { container, rerender } = render(
			<HtmlRenderer htmlString="<p>Old</p>" />
		)

		expect(
			container.querySelector('.html-content p')?.textContent
		).toBe('Old')

		rerender(<HtmlRenderer htmlString="<p><strong>Content</strong></p>" />)

		const strong = container.querySelector('.html-content strong')
		expect(strong).toBeTruthy()
		expect(strong?.textContent).toBe('Content')
	})
})
