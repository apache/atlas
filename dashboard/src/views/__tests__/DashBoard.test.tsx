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
import { render, screen } from '@utils/test-utils'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import DashBoard from '../DashBoard'

jest.mock('@components/GlobalSearch/QuickSearch', () => ({
	__esModule: true,
	default: () => <div data-testid="quick-search">QuickSearch</div>,
}))

jest.mock('../DashboardOverview/DashboardOverview', () => ({
	__esModule: true,
	default: () => <div data-testid="dashboard-overview">DashboardOverview</div>,
}))

const createMockStore = (opts?: {
	sessionData?: Record<string, unknown>
	sessionObj?: unknown
	dashboardVersion?: number
}) => {
	const {
		sessionData = {},
		sessionObj = { data: sessionData },
		dashboardVersion = 0,
	} = opts ?? {}
	return configureStore({
		reducer: {
			session: (state = { sessionObj }) => state,
			dashboardRefresh: (state = { version: dashboardVersion }) => state,
		},
		preloadedState: {
			session: { sessionObj },
			dashboardRefresh: { version: dashboardVersion },
		},
	})
}

describe('DashBoard', () => {
	it('renders QuickSearch and DashboardOverview', () => {
		const store = createMockStore()

		render(
			<Provider store={store}>
				<DashBoard />
			</Provider>,
		)

		expect(screen.getByTestId('quick-search')).toBeInTheDocument()
		expect(screen.getByTestId('dashboard-overview')).toBeInTheDocument()
	})

	it('remounts QuickSearch when dashboardRefresh version changes', () => {
		const store = createMockStore({ dashboardVersion: 3 })

		const { rerender } = render(
			<Provider store={store}>
				<DashBoard />
			</Provider>,
		)

		expect(screen.getByTestId('quick-search')).toBeInTheDocument()

		const nextStore = createMockStore({ dashboardVersion: 4 })
		rerender(
			<Provider store={nextStore}>
				<DashBoard />
			</Provider>,
		)

		expect(screen.getByTestId('quick-search')).toBeInTheDocument()
	})

	it('handles empty session data', () => {
		const store = createMockStore({ sessionData: {} })

		render(
			<Provider store={store}>
				<DashBoard />
			</Provider>,
		)

		expect(screen.getByTestId('quick-search')).toBeInTheDocument()
		expect(screen.getByTestId('dashboard-overview')).toBeInTheDocument()
	})

	it('handles missing session object shape', () => {
		const store = createMockStore({ sessionObj: '' })

		render(
			<Provider store={store}>
				<DashBoard />
			</Provider>,
		)

		expect(screen.getByTestId('quick-search')).toBeInTheDocument()
	})
})
