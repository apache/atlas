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
import { fireEvent, render, screen, waitFor } from '@utils/test-utils'
import DeleteTag from '../DeleteTag'
import { deleteClassification } from '@api/apiMethods/classificationApiMethod'
import { serverError } from '@utils/Utils'

const mockDispatch = jest.fn()
const mockNavigate = jest.fn()

jest.mock('@api/apiMethods/classificationApiMethod', () => ({
	deleteClassification: jest.fn()
}))

jest.mock('@hooks/reducerHook', () => ({
	useAppDispatch: () => mockDispatch
}))

jest.mock('@redux/slice/typeDefSlices/typedefClassificationSlice', () => ({
	fetchClassificationData: jest.fn(() => ({ type: 'FETCH_CLASSIFICATION' }))
}))

jest.mock('@utils/Utils', () => ({
	serverError: jest.fn()
}))

jest.mock('react-router-dom', () => ({
	...jest.requireActual('react-router-dom'),
	useNavigate: () => mockNavigate
}))

jest.mock('@components/Modal', () => ({
	__esModule: true,
	default: ({
		open,
		button1Handler,
		button2Handler,
		children
	}: any) =>
		open ? (
			<div data-testid="custom-modal">
				<button type="button" onClick={button1Handler}>
					Cancel
				</button>
				<button type="button" onClick={button2Handler}>
					Ok
				</button>
				{children}
			</div>
		) : null
}))

jest.mock('@mui/material', () => ({
	Typography: ({ children }: any) => <div>{children}</div>
}))

describe('DeleteTag - 100% Coverage', () => {
	const onClose = jest.fn()
	const setExpandNode = jest.fn()
	const updatedData = jest.fn()

	beforeEach(() => {
		jest.clearAllMocks()
	})

	test('removes classification and navigates', async () => {
		;(deleteClassification as jest.Mock).mockResolvedValueOnce({})

		render(
			<DeleteTag
				open
				onClose={onClose}
				setExpandNode={setExpandNode}
				node={{ text: 'PII', id: '1' }}
				updatedData={updatedData}
			/>
		)

		fireEvent.click(screen.getByText('Ok'))

		await waitFor(() => {
			expect(deleteClassification).toHaveBeenCalled()
		})

		expect(updatedData).toHaveBeenCalled()
		expect(mockNavigate).toHaveBeenCalled()
		expect(setExpandNode).toHaveBeenCalledWith(null)
	})

	test('handles delete error', async () => {
		;(deleteClassification as jest.Mock).mockRejectedValueOnce(
			new Error('error')
		)

		render(
			<DeleteTag
				open
				onClose={onClose}
				setExpandNode={setExpandNode}
				node={{ text: 'PII', id: '1' }}
				updatedData={updatedData}
			/>
		)

		fireEvent.click(screen.getByText('Ok'))

		await waitFor(() => {
			expect(serverError).toHaveBeenCalled()
		})
	})
})
