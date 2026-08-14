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

import React from 'react'
import { render, screen, waitFor } from '@utils/test-utils'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import DashboardOverview from '../DashboardOverview'
import { metricsReducer } from '@redux/slice/metricsSlice'
import { typeHeaderReducer } from '@redux/slice/typeDefSlices/typeDefHeaderSlice'
import { dashboardRefreshReducer } from '@redux/slice/dashboardRefreshSlice'

jest.mock('@api/apiMethods/searchApiMethod', () => ({
	getLatestEntities: jest.fn()
}))

const { getLatestEntities } = jest.requireMock('@api/apiMethods/searchApiMethod') as {
	getLatestEntities: jest.Mock
}

const buildStore = (metricsLoading: boolean) =>
	configureStore({
		reducer: {
			metrics: metricsReducer,
			typeHeader: typeHeaderReducer,
			dashboardRefresh: dashboardRefreshReducer
		},
		preloadedState: {
			metrics: {
				loading: metricsLoading,
				metricsData: metricsLoading
					? null
					: {
							data: {
								general: {
									entityCount: 12,
									tagCount: 3,
									stats: {}
								},
								entity: {},
								tag: {}
							}
					  },
				error: null
			},
			typeHeader: {
				loading: false,
				typeHeaderData: [],
				error: null
			},
			dashboardRefresh: { version: 0 }
		} as any
	})

const renderOverview = (metricsLoading: boolean) => {
	const store = buildStore(metricsLoading)
	return render(
		<Provider store={store}>
			<DashboardOverview />
		</Provider>
	)
}

describe('DashboardOverview', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		getLatestEntities.mockResolvedValue({
			data: { entities: [] }
		})
	})

	it('shows skeleton loaders for metric cards and charts while metrics load', async () => {
		const { container } = renderOverview(true)

		expect(container.querySelectorAll('.MuiSkeleton-root').length).toBeGreaterThan(0)

		await waitFor(() => {
			expect(getLatestEntities).toHaveBeenCalled()
		})
	})

	it('renders overview card and chart regions when metrics are loaded', async () => {
		renderOverview(false)

		await waitFor(() => {
			expect(screen.getByText('Overview')).toBeInTheDocument()
		})

		expect(screen.getByText('12')).toBeInTheDocument()
		expect(screen.getByText('Latest Entities Created')).toBeInTheDocument()
	})

	it('keeps latest-entities skeleton visible until getLatestEntities resolves', async () => {
		let resolveLatest: (v: unknown) => void = () => {}
		getLatestEntities.mockImplementation(
			() =>
				new Promise((res) => {
					resolveLatest = res
				})
		)

		renderOverview(false)

		await waitFor(() => {
			expect(screen.getByText('Overview')).toBeInTheDocument()
		})

		expect(document.querySelectorAll('.MuiSkeleton-root').length).toBeGreaterThan(0)

		resolveLatest({ data: { entities: [] } })

		await waitFor(() => {
			expect(screen.getByText('Latest Entities Created')).toBeInTheDocument()
		})
	})
})
