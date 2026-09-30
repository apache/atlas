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
 * Comprehensive unit tests for About component
 * 
 * Coverage Target: 100% (Statements, Branches, Functions, Lines)
 */

import React from 'react'
import { render, screen, waitFor } from '@utils/test-utils'
import About from '../About'

// Mock API methods
const mockGetVersion = jest.fn()
jest.mock('@api/apiMethods/headerApiMethods', () => ({
	getVersion: (...args: any[]) => mockGetVersion(...args)
}))

// Mock SkeletonLoader component
jest.mock('@components/SkeletonLoader', () => ({
	__esModule: true,
	default: ({ count, animation, variant, width, sx }: any) => (
		<div 
			data-testid="skeleton-loader"
			data-count={count}
			data-animation={animation}
			data-variant={variant}
			data-width={width}
			data-sx={JSON.stringify(sx)}
		>
			Loading...
		</div>
	)
}))

// Mock MUI components
jest.mock('@mui/material', () => ({
	Stack: ({ children, spacing, direction, ...props }: any) => (
		<div 
			data-testid={direction === 'column' ? 'stack-column' : 'stack'}
			data-spacing={spacing}
			data-direction={direction}
			{...props}
		>
			{children}
		</div>
	),
	Typography: ({ children, variant, color, ...props }: any) => (
		<div 
			data-testid={`typography-${variant}`}
			data-variant={variant}
			data-color={color}
			{...props}
		>
			{children}
		</div>
	),
	List: ({ children, dense, ...props }: any) => (
		<div data-testid="list" data-dense={dense} {...props}>
			{children}
		</div>
	),
	ListItem: ({ children, button, component, href, target, ...props }: any) => (
		<div 
			data-testid="list-item"
			data-button={button}
			data-component={component}
			data-href={href}
			data-target={target}
			{...props}
		>
			{children}
		</div>
	),
	ListItemText: ({ primary, ...props }: any) => (
		<div data-testid="list-item-text" data-primary={primary} {...props}>
			{primary}
		</div>
	)
}))

// Mock Utils
const mockServerError = jest.fn()
jest.mock('@utils/Utils', () => ({
	serverError: (...args: any[]) => mockServerError(...args)
}))

// Mock console.error to avoid noise in test output
const originalConsoleError = console.error
beforeAll(() => {
	console.error = jest.fn()
})

afterAll(() => {
	console.error = originalConsoleError
})

describe('About', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetVersion.mockClear()
		mockServerError.mockClear()
	})

	it('should render skeleton loader when loading', async () => {
		mockGetVersion.mockImplementation(() => new Promise(() => {})) // Never resolves

		render(<About />)

		// Should show skeleton loader initially
		const skeletonLoader = screen.getByTestId('skeleton-loader')
		expect(skeletonLoader).toBeInTheDocument()
		expect(skeletonLoader).toHaveAttribute('data-count', '3')
		expect(skeletonLoader).toHaveAttribute('data-animation', 'wave')
		expect(skeletonLoader).toHaveAttribute('data-variant', 'text')
		expect(skeletonLoader).toHaveAttribute('data-width', '100%')
	})

	it('should render version data when API call succeeds', async () => {
		const mockVersionData = {
			Version: '3.0.0-SNAPSHOT',
			Description: 'Metadata Management Platform',
			Revision: 'abc123'
		}

		mockGetVersion.mockResolvedValue({
			data: mockVersionData
		})

		render(<About />)

		// Wait for loading to finish
		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		// Check version is displayed
		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
		expect(screen.getByText('3.0.0-SNAPSHOT')).toBeInTheDocument()

		// Check "Get involved!" text
		expect(screen.getByText('Get involved!')).toBeInTheDocument()

		// Check license link
		const licenseLink = screen.getByText('Licensed under the Apache License Version 2.0')
		expect(licenseLink).toBeInTheDocument()

		// Check ListItem has correct attributes
		const listItem = screen.getByTestId('list-item')
		expect(listItem).toHaveAttribute('data-button', 'true')
		expect(listItem).toHaveAttribute('data-component', 'a')
		expect(listItem).toHaveAttribute('data-href', 'http://apache.org/licenses/LICENSE-2.0')
		expect(listItem).toHaveAttribute('data-target', '_blank')
	})

	it('should render empty version when API returns empty data object', async () => {
		mockGetVersion.mockResolvedValue({
			data: {}
		})

		render(<About />)

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		// Version should be displayed but empty
		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
		const versionTypography = screen.getByTestId('typography-body1')
		expect(versionTypography).toBeInTheDocument()
		expect(versionTypography.textContent).toContain('Version:')
	})

	it('should render empty version when API returns undefined data', async () => {
		mockGetVersion.mockResolvedValue({
			data: undefined
		})

		render(<About />)

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
	})

	it('should render empty version when API returns null data', async () => {
		mockGetVersion.mockResolvedValue({
			data: null
		})

		render(<About />)

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
	})

	it('should handle API call error and call serverError', async () => {
		const mockError = new Error('Network error')
		mockGetVersion.mockRejectedValue(mockError)

		render(<About />)

		await waitFor(() => {
			expect(mockServerError).toHaveBeenCalledWith(mockError, expect.any(Object))
		})

		// Should still show content (not skeleton) after error
		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
	})

	it('should handle API call error with response data', async () => {
		const mockError = {
			response: {
				data: {
					errorMessage: 'Server error occurred'
				}
			}
		}
		mockGetVersion.mockRejectedValue(mockError)

		render(<About />)

		await waitFor(() => {
			expect(mockServerError).toHaveBeenCalledWith(mockError, expect.any(Object))
		})

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})
	})

	it('should call getVersion on component mount', async () => {
		mockGetVersion.mockResolvedValue({
			data: { Version: '1.0.0' }
		})

		render(<About />)

		expect(mockGetVersion).toHaveBeenCalledTimes(1)
		expect(mockGetVersion).toHaveBeenCalledWith()

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})
	})

	it('should render correct Typography variants and colors', async () => {
		mockGetVersion.mockResolvedValue({
			data: { Version: '2.0.0' }
		})

		render(<About />)

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		// Check Typography variants
		const body1Typography = screen.getByTestId('typography-body1')
		expect(body1Typography).toBeInTheDocument()

		const body2Typographies = screen.getAllByTestId('typography-body2')
		expect(body2Typographies.length).toBeGreaterThan(0)

		// Check color prop for "Get involved!" text
		const getInvolvedTypography = body2Typographies.find(
			(el) => el.textContent === 'Get involved!'
		)
		expect(getInvolvedTypography).toHaveAttribute('data-color', 'info.main')
	})

	it('should render Stack components with correct props', async () => {
		mockGetVersion.mockResolvedValue({
			data: { Version: '1.0.0' }
		})

		render(<About />)

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		// Check main Stack
		const mainStack = screen.getByTestId('stack')
		expect(mainStack).toHaveAttribute('data-spacing', '2')

		// Check column Stack
		const columnStack = screen.getByTestId('stack-column')
		expect(columnStack).toHaveAttribute('data-spacing', '1')
		expect(columnStack).toHaveAttribute('data-direction', 'column')
	})

	it('should render List with dense prop', async () => {
		mockGetVersion.mockResolvedValue({
			data: { Version: '1.0.0' }
		})

		render(<About />)

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		const list = screen.getByTestId('list')
		expect(list).toHaveAttribute('data-dense', 'true')
	})

	it('should render ListItemText with correct primary text', async () => {
		mockGetVersion.mockResolvedValue({
			data: { Version: '1.0.0' }
		})

		render(<About />)

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		const listItemText = screen.getByTestId('list-item-text')
		expect(listItemText).toHaveAttribute(
			'data-primary',
			'Licensed under the Apache License Version 2.0'
		)
	})

	it('should handle versionData with undefined Version property', async () => {
		mockGetVersion.mockResolvedValue({
			data: { Description: 'Some description' }
		})

		render(<About />)

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
		const versionTypography = screen.getByTestId('typography-body1')
		expect(versionTypography.textContent).toContain('Version:')
		expect(versionTypography.textContent).not.toContain('undefined')
	})

	it('should set loader to false after successful API call', async () => {
		mockGetVersion.mockResolvedValue({
			data: { Version: '1.0.0' }
		})

		render(<About />)

		// Initially should show loader
		expect(screen.getByTestId('skeleton-loader')).toBeInTheDocument()

		// After API resolves, loader should be hidden
		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})
	})

	it('should set loader to false after failed API call', async () => {
		mockGetVersion.mockRejectedValue(new Error('API Error'))

		render(<About />)

		// Initially should show loader
		expect(screen.getByTestId('skeleton-loader')).toBeInTheDocument()

		// After API rejects, loader should be hidden
		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})
	})

	it('should handle API response with null response object', async () => {
		mockGetVersion.mockResolvedValue(null)

		render(<About />)

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
	})

	it('should handle API response with undefined response object', async () => {
		mockGetVersion.mockResolvedValue(undefined)

		render(<About />)

		await waitFor(() => {
			expect(screen.queryByTestId('skeleton-loader')).not.toBeInTheDocument()
		})

		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
	})
})
