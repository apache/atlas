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
 * Unit tests for TreeIcons component
 */

import React from 'react'
import { render, screen } from '@testing-library/react'
import TreeIcons from '../Treeicons'

describe('TreeIcons', () => {
	it('renders folder icon for Entities with children', () => {
		render(
			<TreeIcons
				node={{ children: [{ id: 'child' }] }}
				treeName="Entities"
				isEmptyServicetype={false}
			/>
		)

		expect(screen.getByTestId('FolderOutlinedIcon')).toBeTruthy()
	})

	it('renders file icon for Entities without children', () => {
		render(
			<TreeIcons
				node={{}}
				treeName="Entities"
				isEmptyServicetype={false}
			/>
		)

		expect(screen.getByTestId('InsertDriveFileOutlinedIcon')).toBeTruthy()
	})

	it('renders glossary folder icon for parent', () => {
		render(
			<TreeIcons
				node={{ types: 'parent' }}
				treeName="Glossary"
				isEmptyServicetype={false}
			/>
		)

		expect(screen.getByTestId('FolderOutlinedIcon')).toBeTruthy()
	})

	it('renders glossary file icon for child', () => {
		render(
			<TreeIcons
				node={{ types: 'child' }}
				treeName="Glossary"
				isEmptyServicetype={false}
			/>
		)

		expect(screen.getByTestId('InsertDriveFileOutlinedIcon')).toBeTruthy()
	})

	it('renders classification icon for Classifications', () => {
		render(
			<TreeIcons
				node={{}}
				treeName="Classifications"
				isEmptyServicetype={false}
			/>
		)

		expect(screen.getByTestId('SellOutlinedIcon')).toBeTruthy()
	})

	it('renders relationship icon for Relationships', () => {
		render(
			<TreeIcons
				node={{}}
				treeName="Relationships"
				isEmptyServicetype={false}
			/>
		)

		expect(screen.getByTestId('LinkOutlinedIcon')).toBeTruthy()
	})

	it('renders folder icon for CustomFilters parent when empty', () => {
		render(
			<TreeIcons
				node={{ types: 'parent', parent: 'BASIC' }}
				treeName="CustomFilters"
				isEmptyServicetype={true}
			/>
		)

		expect(screen.getByTestId('FolderOutlinedIcon')).toBeTruthy()
	})

	it('renders avatar icon for CustomFilters child', () => {
		render(
			<TreeIcons
				node={{ types: 'child', parent: 'SomeParent' }}
				treeName="CustomFilters"
				isEmptyServicetype={false}
			/>
		)

		expect(screen.getByText('S')).toBeTruthy()
	})

	it('renders "R" for BASIC_RELATIONSHIP', () => {
		render(
			<TreeIcons
				node={{ types: 'child', parent: 'BASIC_RELATIONSHIP' }}
				treeName="CustomFilters"
				isEmptyServicetype={false}
			/>
		)

		expect(screen.getByText('R')).toBeTruthy()
	})
})
