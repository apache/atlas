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

import React from 'react';
import { render, screen } from '@testing-library/react';
import SidebarTreePortal from '../SidebarTreePortal';
import type { Props } from '@models/treeStructureType';

const MockTree = ({
	sideBarOpen,
	searchTerm,
	isPopover,
}: Props) => (
	<div data-testid="mock-tree">
		{String(sideBarOpen)}|{searchTerm}|{String(isPopover)}
	</div>
);

describe('SidebarTreePortal', () => {
	const setSidebarRef = jest.fn(() => jest.fn());

	it('portals tree into sidebar host when drawer is open', () => {
		const host = document.createElement('div');
		document.body.appendChild(host);

		render(
			<SidebarTreePortal
				moduleId="entities"
				dataCy="r_entityTreeRender"
				Tree={MockTree}
				searchTerm="hive"
				drawerOpen={true}
				activePopover={null}
				popoverContainer={null}
				sidebarHost={host}
				setSidebarRef={setSidebarRef}
			/>,
		);

		expect(screen.getByTestId('mock-tree')).toHaveTextContent('true|hive|false');
		expect(host.contains(screen.getByTestId('mock-tree'))).toBe(true);
		expect(document.querySelector('[data-cy="r_entityTreeRender"]')).toBeTruthy();
	});

	it('uses popover target and isPopover when collapsed module is active', () => {
		const sidebarHost = document.createElement('div');
		const popoverHost = document.createElement('div');
		document.body.appendChild(sidebarHost);
		document.body.appendChild(popoverHost);

		render(
			<SidebarTreePortal
				moduleId="classification"
				dataCy="r_classificationTreeRender"
				Tree={MockTree}
				searchTerm=""
				drawerOpen={false}
				activePopover="classification"
				popoverContainer={popoverHost}
				sidebarHost={sidebarHost}
				setSidebarRef={setSidebarRef}
			/>,
		);

		expect(screen.getByTestId('mock-tree')).toHaveTextContent('true||true');
		expect(popoverHost.contains(screen.getByTestId('mock-tree'))).toBe(true);
	});

	it('does not render tree until sidebar host ref is set', () => {
		const { container } = render(
			<SidebarTreePortal
				moduleId="glossary"
				dataCy="r_glossaryTreeRender"
				Tree={MockTree}
				searchTerm=""
				drawerOpen={true}
				activePopover={null}
				popoverContainer={null}
				sidebarHost={null}
				setSidebarRef={setSidebarRef}
			/>,
		);

		expect(container.querySelector('[data-testid="mock-tree"]')).toBeNull();
	});
});
