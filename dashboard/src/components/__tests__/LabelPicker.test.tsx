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
 * Unit tests for LabelPicker component
 */

import React from 'react'
import { render, screen, fireEvent } from '@testing-library/react'
import LabelPicker from '../LabelPicker'

jest.mock('@mui/material/ClickAwayListener', () => ({
	__esModule: true,
	default: ({ children }: any) => <div>{children}</div>
}))

jest.mock('@mui/material/Autocomplete', () => ({
	__esModule: true,
	default: (props: any) => {
		const {
			onClose,
			onChange,
			renderInput,
			options
		} = props
		return (
			<div>
				<button onClick={() => onClose({}, 'escape')}>close</button>
				<button
					onClick={() =>
						onChange({ type: 'keydown', key: 'a' }, ['A'], 'selectOption')
					}
				>
					select
				</button>
				<button
					onClick={() =>
						onChange(
							{ type: 'keydown', key: 'Backspace' },
							['A'],
							'removeOption'
						)
					}
				>
					remove
				</button>
				<div data-testid="options-count">{options.length}</div>
				{renderInput({ InputProps: { ref: null }, inputProps: {} })}
			</div>
		)
	}
}))

jest.mock('@mui/material/Popper', () => ({
	__esModule: true,
	default: ({ children }: any) => <div>{children}</div>
}))

describe('LabelPicker', () => {
	it('calls handleClickLabelPicker when icon is clicked', () => {
		const handleClickLabelPicker = jest.fn()
		render(
			<LabelPicker
				anchorEl={{}}
				Label={<span>Label</span>}
				handleCloseLabelPicker={jest.fn()}
				value={[]}
				id="label-picker"
				pendingValue={[]}
				setPendingValue={jest.fn()}
				handleClickLabelPicker={handleClickLabelPicker}
				optionList={['A', 'B']}
			/>
		)

		fireEvent.click(screen.getByLabelText('Filters'))
		expect(handleClickLabelPicker).toHaveBeenCalled()
	})

	it('closes on escape', () => {
		const handleCloseLabelPicker = jest.fn()
		render(
			<LabelPicker
				anchorEl={{}}
				Label={<span>Label</span>}
				handleCloseLabelPicker={handleCloseLabelPicker}
				value={[]}
				id="label-picker"
				pendingValue={[]}
				setPendingValue={jest.fn()}
				handleClickLabelPicker={jest.fn()}
				optionList={['A', 'B']}
			/>
		)

		fireEvent.click(screen.getByText('close'))
		expect(handleCloseLabelPicker).toHaveBeenCalled()
	})

	it('updates pending value on selection', () => {
		const setPendingValue = jest.fn()
		render(
			<LabelPicker
				anchorEl={{}}
				Label={<span>Label</span>}
				handleCloseLabelPicker={jest.fn()}
				value={[]}
				id="label-picker"
				pendingValue={[]}
				setPendingValue={setPendingValue}
				handleClickLabelPicker={jest.fn()}
				optionList={['A', 'B']}
			/>
		)

		fireEvent.click(screen.getByText('select'))
		expect(setPendingValue).toHaveBeenCalledWith(['A'])
	})

	it('does not update pending value on remove with Backspace', () => {
		const setPendingValue = jest.fn()
		render(
			<LabelPicker
				anchorEl={{}}
				Label={<span>Label</span>}
				handleCloseLabelPicker={jest.fn()}
				value={['A']}
				id="label-picker"
				pendingValue={['A']}
				setPendingValue={setPendingValue}
				handleClickLabelPicker={jest.fn()}
				optionList={['A', 'B']}
			/>
		)

		fireEvent.click(screen.getByText('remove'))
		expect(setPendingValue).not.toHaveBeenCalled()
	})
})
