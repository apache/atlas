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
