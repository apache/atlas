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
 * Unit tests for ImportDialog component
 */

import React from 'react'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import ImportDialog from '../ImportDialog'

const toastSuccess = jest.fn()
const toastError = jest.fn()
const toastDismiss = jest.fn()

jest.mock('react-toastify', () => ({
	toast: {
		success: (...args: any[]) => toastSuccess(...args),
		error: (...args: any[]) => toastError(...args),
		dismiss: (...args: any[]) => toastDismiss(...args)
	}
}))

jest.mock('../muiComponents', () => ({
	Dialog: ({ open, children }: any) => (open ? <div>{children}</div> : null),
	DialogTitle: ({ children }: any) => <div>{children}</div>,
	DialogContent: ({ children }: any) => <div>{children}</div>,
	DialogActions: ({ children }: any) => <div>{children}</div>,
	IconButton: ({ children, onClick, 'aria-label': ariaLabel }: any) => (
		<button aria-label={ariaLabel} onClick={onClick}>{children}</button>
	),
	CustomButton: ({ children, onClick }: any) => (
		<button onClick={onClick}>{children}</button>
	),
	LightTooltip: ({ children }: any) => <span>{children}</span>,
	Typography: ({ children }: any) => <span>{children}</span>,
	CloseIcon: () => <span>close</span>
}))

const uploadMock = jest.fn()
const glossaryMock = jest.fn()

jest.mock('../../api/apiMethods/entitiesApiMethods', () => ({
	getBusinessMetadataImport: (...args: any[]) => uploadMock(...args)
}))

jest.mock('../../api/apiMethods/glossaryApiMethod', () => ({
	getGlossaryImport: (...args: any[]) => glossaryMock(...args)
}))

jest.mock('../../views/SideBar/Import/ImportLayout', () => ({
	__esModule: true,
	default: ({ setFileData, setProgress }: any) => (
		<button
			onClick={() => {
				setFileData({ name: 'file.csv' })
				setProgress(50)
			}}
		>
			Select File
		</button>
	)
}))

describe('ImportDialog', () => {
	beforeEach(() => {
		jest.clearAllMocks()
	})

	it('uploads business metadata file successfully', async () => {
		uploadMock.mockResolvedValue({ data: {} })
		const onClose = jest.fn()

		render(
			<ImportDialog open={true} onClose={onClose} title="Import Business Metadata" />
		)

		fireEvent.click(screen.getByText('Select File'))
		fireEvent.click(screen.getByText('Upload'))

		await waitFor(() => {
			expect(uploadMock).toHaveBeenCalled()
			expect(onClose).toHaveBeenCalled()
			expect(toastSuccess).toHaveBeenCalled()
		})
	})

	it('shows error details when import returns failed info', async () => {
		glossaryMock.mockResolvedValue({
			data: {
				failedImportInfoList: [{ index: 1, remarks: 'Bad row' }]
			}
		})

		render(<ImportDialog open={true} onClose={jest.fn()} title="Import Glossary" />)

		fireEvent.click(screen.getByText('Select File'))
		fireEvent.click(screen.getByText('Upload'))

		await waitFor(() => {
			expect(glossaryMock).toHaveBeenCalled()
			expect(toastError).toHaveBeenCalledWith('Bad row')
		})

		expect(screen.getByText('Error Details')).toBeTruthy()
		expect(screen.getByText('1. Bad row')).toBeTruthy()
	})

	it('handles upload errors', async () => {
		uploadMock.mockRejectedValue(new Error('fail'))

		render(
			<ImportDialog open={true} onClose={jest.fn()} title="Import Business Metadata" />
		)

		fireEvent.click(screen.getByText('Select File'))
		fireEvent.click(screen.getByText('Upload'))

		await waitFor(() => {
			expect(toastError).toHaveBeenCalledWith('Invalid JSON response from server')
		})
	})

	it('closes dialog when Cancel is clicked', () => {
		const onClose = jest.fn()
		render(
			<ImportDialog open={true} onClose={onClose} title="Import Glossary" />
		)

		fireEvent.click(screen.getByText('Cancel'))
		expect(onClose).toHaveBeenCalled()
	})

	it('returns to upload view when back is clicked', async () => {
		glossaryMock.mockResolvedValue({
			data: {
				failedImportInfoList: [{ index: 1, remarks: 'Bad row' }]
			}
		})

		render(<ImportDialog open={true} onClose={jest.fn()} title="Import Glossary" />)

		fireEvent.click(screen.getByText('Select File'))
		fireEvent.click(screen.getByText('Upload'))

		await waitFor(() => {
			expect(screen.getByText('Error Details')).toBeTruthy()
		})

		fireEvent.click(screen.getByLabelText('back'))
		expect(screen.getByText('Upload')).toBeTruthy()
	})
})
