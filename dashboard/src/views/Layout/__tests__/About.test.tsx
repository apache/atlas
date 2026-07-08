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
import { render, screen } from '@utils/test-utils'
import '@testing-library/jest-dom'
import About from '../About'
import * as reducerHook from '@hooks/reducerHook'

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

describe('About', () => {
	let useAppSelectorSpy: jest.SpyInstance

	beforeEach(() => {
		jest.clearAllMocks()
		useAppSelectorSpy = jest.spyOn(reducerHook, 'useAppSelector')
	})

	it('should render skeleton loader when loading', () => {
		useAppSelectorSpy.mockReturnValue({ data: {}, loading: true })

		render(<About />)

		// Should show skeleton loader initially
		const skeletonLoader = screen.getByTestId('skeleton-loader')
		expect(skeletonLoader).toBeInTheDocument()
		expect(skeletonLoader).toHaveAttribute('data-count', '3')
		expect(skeletonLoader).toHaveAttribute('data-animation', 'wave')
		expect(skeletonLoader).toHaveAttribute('data-variant', 'text')
		expect(skeletonLoader).toHaveAttribute('data-width', '100%')
	})

	it('should render version data when loading is false and data is available', () => {
		const mockVersionData = {
			Version: '3.0.0-SNAPSHOT',
			Description: 'Metadata Management Platform',
			Revision: 'abc123'
		}

		useAppSelectorSpy.mockReturnValue({ data: mockVersionData, loading: false })

		render(<About />)

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

	it('should render empty version when data object is empty', () => {
		useAppSelectorSpy.mockReturnValue({ data: {}, loading: false })

		render(<About />)

		// Version should be displayed but empty
		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
		const versionTypography = screen.getByTestId('typography-body1')
		expect(versionTypography).toBeInTheDocument()
		expect(versionTypography.textContent).toContain('Version:')
	})

	it('should render empty version when data is undefined', () => {
		useAppSelectorSpy.mockReturnValue({ data: undefined, loading: false })

		render(<About />)

		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
	})

	it('should render empty version when data is null', () => {
		useAppSelectorSpy.mockReturnValue({ data: null, loading: false })

		render(<About />)

		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
	})

	it('should handle versionData with undefined Version property', () => {
		useAppSelectorSpy.mockReturnValue({ data: { Description: 'Some description' }, loading: false })

		render(<About />)

		expect(screen.getByText(/Version:/i)).toBeInTheDocument()
		const versionTypography = screen.getByTestId('typography-body1')
		expect(versionTypography.textContent).toContain('Version:')
		expect(versionTypography.textContent).not.toContain('undefined')
	})
})
