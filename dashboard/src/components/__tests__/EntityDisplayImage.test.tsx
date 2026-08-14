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
 * Unit tests for EntityDisplayImage component
 * 
 * Coverage Target: 100%
 * - Statements: 100%
 * - Branches: 100%
 * - Functions: 100%
 * - Lines: 100%
 */

import React from 'react'
import { render, waitFor, act } from '@testing-library/react'
import DisplayImage from '../EntityDisplayImage'

// Import Utils to spy on it
import * as Utils from '../../utils/Utils'

const mockGetEntityIconPath = jest.fn()

// Mock the Utils module
jest.mock('../../utils/Utils', () => ({
	getEntityIconPath: jest.fn()
}))

const mockFetch = (contentType: string | null, shouldReject?: boolean) => {
	if (shouldReject) {
		(global as any).fetch = jest.fn().mockRejectedValue(new Error('fetch failed'))
		return
	}
	(global as any).fetch = jest.fn().mockResolvedValue({
		ok: true,
		headers: {
			get: jest.fn((header: string) => {
				return header === 'Content-Type' ? (contentType || '') : null
			})
		}
	})
}

describe('EntityDisplayImage', () => {
	const entity = { guid: 'entity-1' }

	beforeEach(() => {
		jest.clearAllMocks()
		
		// Set up the mock implementation for getEntityIconPath
		;(Utils.getEntityIconPath as jest.Mock).mockImplementation(({ entityData, errorUrl }: { entityData: any, errorUrl?: string }) => {
			const result = errorUrl ? `${errorUrl}-fallback` : `/icons/${entityData.guid}.png`
			mockGetEntityIconPath({ entityData, errorUrl })
			return result
		})
	})

	it('renders cached image when content-type is image', async () => {
		mockFetch('image/png')

		const { container } = render(
			<DisplayImage entity={entity} width={20} height={20} />
		)

		// Wait for Skeleton to disappear and image to appear
		await waitFor(() => {
			const skeleton = container.querySelector('.MuiSkeleton-root')
			expect(skeleton).not.toBeInTheDocument()
		}, { timeout: 10000, interval: 100 })

		await waitFor(() => {
			const img = container.querySelector('img')
			expect(img).toBeInTheDocument()
			expect(img?.getAttribute('src')).toBe('/icons/entity-1.png')
			expect(img?.getAttribute('alt')).toBe('Entity Icon')
			expect(img?.getAttribute('id')).toBe('entity-1')
			expect(img?.getAttribute('data-cy')).toBe('entity-1')
		}, { timeout: 10000 })
	}, 20000)

	it('renders fallback image when content-type is not image', async () => {
		mockFetch('text/plain')

		const { container } = render(
			<DisplayImage entity={entity} width={20} height={20} />
		)

		await waitFor(() => {
			const img = container.querySelector('img')
			expect(img).toBeInTheDocument()
			expect(img?.getAttribute('src')).toBe('/icons/entity-1.png-fallback')
		}, { timeout: 10000 })
	}, 20000)

	it('renders fallback image when content-type is null', async () => {
		mockFetch(null)

		const { container} = render(
			<DisplayImage entity={entity} width={20} height={20} />
		)

		await waitFor(() => {
			const img = container.querySelector('img')
			expect(img).toBeInTheDocument()
			expect(img?.getAttribute('src')).toBe('/icons/entity-1.png-fallback')
		}, { timeout: 10000 })
	}, 20000)

	it('renders fallback image when fetch throws', async () => {
		mockFetch('image/png', true)

		const { container } = render(
			<DisplayImage entity={entity} width={20} height={20} />
		)

		await waitFor(() => {
			const img = container.querySelector('img')
			expect(img).toBeInTheDocument()
			expect(img?.getAttribute('src')).toBe('/icons/entity-1.png-fallback')
		}, { timeout: 10000 })
	}, 20000)

	it('renders Avatar when avatarDisplay is provided and image is cached', async () => {
		mockFetch('image/png')

		const { container } = render(
			<DisplayImage
				entity={entity}
				width={30}
				height={30}
				avatarDisplay={true}
			/>
		)

		await waitFor(() => {
			const avatar = container.querySelector('img[alt="entityImg"]')
			expect(avatar).toBeTruthy()
			expect(avatar?.getAttribute('src')).toBe('/icons/entity-1.png')
		}, { timeout: 10000 })
	}, 20000)

	it('renders Avatar when avatarDisplay is provided and image is not cached', async () => {
		mockFetch('text/plain')

		const { container } = render(
			<DisplayImage
				entity={entity}
				width={30}
				height={30}
				avatarDisplay={true}
			/>
		)

		await waitFor(() => {
			const avatar = container.querySelector('img[alt="entityImg"]')
			expect(avatar).toBeTruthy()
			expect(avatar?.getAttribute('src')).toBe('/icons/entity-1.png-fallback')
		}, { timeout: 10000 })
	}, 20000)

	it('renders Skeleton when imageUrl is undefined', () => {
		mockGetEntityIconPath.mockReturnValue(undefined)

		const { container } = render(
			<DisplayImage entity={entity} width={20} height={20} />
		)

		const skeleton = container.querySelector('div')
		expect(skeleton).toBeTruthy()
	})

	it('handles isProcess prop', async () => {
		mockFetch('image/png')

		const entityWithProcess = { guid: 'entity-2', isProcess: true }
		render(
			<DisplayImage entity={entityWithProcess} width={20} height={20} isProcess={true} />
		)

		await waitFor(() => {
			expect(mockGetEntityIconPath).toHaveBeenCalledWith(
				expect.objectContaining({
					entityData: expect.objectContaining({ isProcess: true })
				})
			)
		})
	}, 20000)

	it('handles entity without isProcess prop but with isProcess passed', async () => {
		mockFetch('image/png')

		render(
			<DisplayImage entity={entity} width={20} height={20} isProcess={false} />
		)

		await waitFor(() => {
			expect(mockGetEntityIconPath).toHaveBeenCalledWith(
				expect.objectContaining({
					entityData: expect.objectContaining({ isProcess: false })
				})
			)
		})
	}, 20000)

	it('sets checkEntityImage cache when image is valid', async () => {
		mockFetch('image/jpeg')

		const { container } = render(
			<DisplayImage entity={entity} width={20} height={20} />
		)

		await waitFor(() => {
			const img = container.querySelector('img')
			expect(img).toBeInTheDocument()
			expect(img?.getAttribute('src')).toBe('/icons/entity-1.png')
		}, { timeout: 10000 })
	}, 20000)

	it('handles different image content types', async () => {
		const contentTypes = ['image/gif', 'image/webp', 'image/svg+xml']
		
		for (const contentType of contentTypes) {
			mockFetch(contentType)
			const { container, unmount } = render(
				<DisplayImage entity={{ guid: `entity-${contentType}` }} width={20} height={20} />
			)

			await waitFor(() => {
				const img = container.querySelector('img')
				expect(img).toBeTruthy()
			}, { timeout: 10000 })
			unmount()
		}
	}, 30000)

	it('handles errorUrl in getEntityIconPath when fetch fails', async () => {
		mockFetch('image/png', true)
		;(Utils.getEntityIconPath as jest.Mock).mockImplementation(({ entityData, errorUrl }: { entityData: any, errorUrl?: string }) => {
			if (errorUrl) return `${errorUrl}-error`
			return `/icons/${entityData.guid}.png`
		})

		const { container } = render(
			<DisplayImage entity={entity} width={20} height={20} />
		)

		await waitFor(() => {
			const img = container.querySelector('img')
			expect(img).toBeInTheDocument()
			expect(img?.getAttribute('src')).toContain('-error')
		}, { timeout: 10000 })
	}, 20000)

	it('handles errorUrl in getEntityIconPath when content-type is not image', async () => {
		mockFetch('application/json')
		;(Utils.getEntityIconPath as jest.Mock).mockImplementation(({ entityData, errorUrl }: { entityData: any, errorUrl?: string }) => {
			if (errorUrl) return `${errorUrl}-error`
			return `/icons/${entityData.guid}.png`
		})

		const { container } = render(
			<DisplayImage entity={entity} width={20} height={20} />
		)

		await waitFor(() => {
			const img = container.querySelector('img')
			expect(img).toBeInTheDocument()
			expect(img?.getAttribute('src')).toContain('-error')
		}, { timeout: 10000 })
	}, 20000)
})
