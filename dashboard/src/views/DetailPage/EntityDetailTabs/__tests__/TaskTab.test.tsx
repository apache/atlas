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

import { render, screen, waitFor } from '@testing-library/react'
import TaskTab from '../TaskTab'

const mockGetPendingTasks = jest.fn()

jest.mock('@api/apiMethods/adminTasksApiMethod', () => ({
	getPendingTasks: (...args: unknown[]) => mockGetPendingTasks(...args)
}))

jest.mock('@utils/Utils', () => ({
	...jest.requireActual('@utils/Utils'),
	serverError: jest.fn()
}))

describe('TaskTab', () => {
	beforeEach(() => {
		jest.clearAllMocks()
		mockGetPendingTasks.mockResolvedValue({ data: [] })
	})

	it('should render pending tasks table with column headers', async () => {
		render(<TaskTab />)

		await waitFor(() => {
			expect(mockGetPendingTasks).toHaveBeenCalled()
		})

		expect(screen.getByRole('button', { name: /refresh tasks/i })).toBeInTheDocument()
		expect(screen.getByText('Type')).toBeInTheDocument()
		expect(screen.getByText('Guid')).toBeInTheDocument()
		expect(screen.getByText('Status')).toBeInTheDocument()
	})

	it('should show empty state when API returns no tasks', async () => {
		mockGetPendingTasks.mockResolvedValue({ data: [] })
		render(<TaskTab />)

		await waitFor(() => {
			expect(screen.getByText('No records found!')).toBeInTheDocument()
		})
	})

	it('should render task rows when API returns tasks', async () => {
		mockGetPendingTasks.mockResolvedValue({
			data: [
				{
					guid: 'task-guid-1',
					type: 'ENTITY_DELETE',
					status: 'PENDING',
					createdTime: 1_700_000_000_000,
					updatedTime: 1_700_000_100_000
				}
			]
		})

		render(<TaskTab />)

		await waitFor(() => {
			expect(screen.getByText('task-guid-1')).toBeInTheDocument()
		})
		expect(screen.queryByText('No records found!')).not.toBeInTheDocument()
	})
})
