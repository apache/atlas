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

import React from "react";
import { render, screen, fireEvent, waitFor, act } from "@utils/test-utils";
import "@testing-library/jest-dom";
import BMAttributes from "../BMAttributes";
import { ThemeProvider, createTheme } from "@mui/material/styles";

const theme = createTheme();
const mockEnumDefs = [
	{
		name: "adls_gen2_replication",
		elementDefs: [{ value: "LRS" }, { value: "ZRS" }, { value: "GRS" }]
	}
];

const mockDispatch = jest.fn();

const buildEnumIntegrationState = () => ({
	entity: {
		entityData: {
			entityDefs: [
				{
					name: "DataSet",
					businessAttributeDefs: {
						Group1: [
							{ name: "replication", typeName: "adls_gen2_replication" },
							{
								name: "replication_list",
								typeName: "array<adls_gen2_replication>"
							}
						]
					}
				}
			]
		}
	},
	businessMetaData: {
		businessMetaData: {
			businessMetadataDefs: [
				{
					name: "Group1",
					attributeDefs: [
						{ name: "replication", typeName: "adls_gen2_replication" },
						{
							name: "replication_list",
							typeName: "array<adls_gen2_replication>"
						}
					]
				}
			]
		}
	},
	enum: {
		enumObj: {
			loading: false,
			data: { enumDefs: mockEnumDefs },
			error: null
		}
	}
});

jest.mock("@hooks/reducerHook", () => ({
	useAppDispatch: () => mockDispatch,
	useAppSelector: jest.fn((selector) => selector(buildEnumIntegrationState()))
}));

jest.mock("react-router-dom", () => ({
	...jest.requireActual("react-router-dom"),
	useParams: () => ({ guid: "test-guid-123" })
}));

const mockGetEntityBusinessMetadata = jest.fn();
jest.mock("@api/apiMethods/detailpageApiMethod", () => ({
	getEntityBusinessMetadata: (...args: unknown[]) =>
		mockGetEntityBusinessMetadata(...args)
}));

jest.mock("react-toastify", () => ({
	toast: {
		dismiss: jest.fn(),
		success: jest.fn(() => "toast-id"),
		error: jest.fn(() => "toast-id")
	}
}));

jest.mock("@utils/Utils", () => ({
	...jest.requireActual("@utils/Utils"),
	serverError: jest.fn()
}));

jest.mock("@redux/slice/detailPageSlice", () => ({
	fetchDetailPageData: jest.fn((guid: string) => ({
		type: "fetchDetailPageData",
		payload: guid
	}))
}));

jest.mock("@redux/slice/enumSlice", () => ({
	fetchEnumData: jest.fn(() => ({ type: "FETCH_ENUM_DATA" }))
}));

jest.mock("@components/DatePicker/CustomDatepicker", () => ({
	__esModule: true,
	default: () => <input data-testid="date-picker" />
}));

jest.mock("react-quill-new", () => ({
	__esModule: true,
	default: ({ placeholder }: { placeholder: string }) => (
		<div data-testid="react-quill">{placeholder}</div>
	)
}));

const TestWrapper: React.FC<React.PropsWithChildren<{}>> = ({ children }) => (
	<ThemeProvider theme={theme}>{children}</ThemeProvider>
);

describe("BMAttributes enum integration (no BMAttributesFields mock)", () => {
	const enumProps = {
		loading: false,
		bmAttributes: {
			Group1: {
				replication: "LRS",
				replication_list: ["LRS", "ZRS"]
			}
		},
		entity: { guid: "test-guid-123", status: "ACTIVE", typeName: "DataSet" }
	};

	beforeEach(() => {
		jest.clearAllMocks();
		mockGetEntityBusinessMetadata.mockResolvedValue({ data: {} });
	});

	it("renders real enum Select dropdown in edit mode (positive)", () => {
		render(
			<TestWrapper>
				<BMAttributes {...enumProps} />
			</TestWrapper>
		);

		fireEvent.click(screen.getByText("Edit").closest("button")!);

		expect(
			screen.getByLabelText(/Select replication enum value/i)
		).toBeInTheDocument();
		expect(screen.getAllByText("LRS").length).toBeGreaterThan(0);
		expect(screen.getByLabelText(/Select replication enum value/i)).toHaveTextContent(
			"LRS"
		);
	});

	it("saves single enum value end-to-end after changing dropdown (positive)", async () => {
		render(
			<TestWrapper>
				<BMAttributes {...enumProps} />
			</TestWrapper>
		);

		fireEvent.click(screen.getByText("Edit").closest("button")!);

		const select = screen.getByLabelText(/Select replication enum value/i);
		fireEvent.mouseDown(select);
		fireEvent.click(screen.getByRole("option", { name: "GRS" }));

		const saveBtn = screen.getByText("Save").closest("button")!;
		await act(async () => {
			fireEvent.click(saveBtn);
		});

		await waitFor(() => {
			expect(mockGetEntityBusinessMetadata).toHaveBeenCalledWith(
				"test-guid-123",
				expect.objectContaining({
					Group1: expect.objectContaining({ replication: "GRS" })
				})
			);
		});
	});

	it("serializes array enum values on save via serializeMultiEnumValue (positive)", async () => {
		render(
			<TestWrapper>
				<BMAttributes {...enumProps} />
			</TestWrapper>
		);

		fireEvent.click(screen.getByText("Edit").closest("button")!);

		const saveBtn = screen.getByText("Save").closest("button")!;
		await act(async () => {
			fireEvent.click(saveBtn);
		});

		await waitFor(() => {
			expect(mockGetEntityBusinessMetadata).toHaveBeenCalledWith(
				"test-guid-123",
				{
					Group1: {
						replication: "LRS",
						replication_list: ["LRS", "ZRS"]
					}
				}
			);
		});
	});

	it("does not call save API when enum field row has no key selected (negative)", async () => {
		render(
			<TestWrapper>
				<BMAttributes
					loading={false}
					bmAttributes={{}}
					entity={{ guid: "test-guid-123", status: "ACTIVE", typeName: "DataSet" }}
				/>
			</TestWrapper>
		);

		fireEvent.click(screen.getByText("Add").closest("button")!);

		const saveBtn = screen.getByText("Save").closest("button")!;
		await act(async () => {
			fireEvent.click(saveBtn);
		});

		expect(mockGetEntityBusinessMetadata).not.toHaveBeenCalled();
	});
});
