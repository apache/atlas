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
import { useForm, useWatch } from "react-hook-form";
import { ThemeProvider, createTheme } from "@mui/material/styles";
import BMAttributesFields from "../BMAttributesFields";

const mockEnumDefs = [
	{
		name: "adls_gen2_replication",
		elementDefs: [
			{ value: "LRS" },
			{ value: "ZRS" },
			{ value: "GRS" }
		]
	}
];

const mockUseAppSelector = jest.fn((selector) =>
	selector({
		enum: {
			enumObj: {
				loading: false,
				data: {
					enumDefs: mockEnumDefs
				},
				error: null
			}
		}
	})
);

jest.mock("@hooks/reducerHook", () => ({
	useAppSelector: (selector: unknown) => mockUseAppSelector(selector)
}));

const SINGLE_DATE_TIMESTAMP = 1717200000000;
const MULTI_DATE_TIMESTAMPS = [1717200000000, 1717286400000];

jest.mock("@components/DatePicker/CustomDatePicker", () => ({
	__esModule: true,
	default: ({
		onChange,
		selectsMultiple,
		selected,
		selectedDates
	}: {
		onChange: (value: unknown) => void;
		selectsMultiple?: boolean;
		selected?: Date;
		selectedDates?: Date[];
	}) => (
		<input
			data-testid={selectsMultiple ? "date-picker-multi" : "date-picker"}
			data-selected={selected ? String(selected.getTime()) : ""}
			data-selected-dates={
				selectedDates?.length
					? selectedDates.map((date) => date.getTime()).join(",")
					: ""
			}
			onClick={() => {
				if (selectsMultiple) {
					onChange(
						MULTI_DATE_TIMESTAMPS.map((timestamp) => new Date(timestamp))
					);
					return;
				}
				onChange({ getTime: () => SINGLE_DATE_TIMESTAMP });
			}}
		/>
	)
}));

jest.mock("react-quill-new", () => ({
	__esModule: true,
	default: ({
		placeholder,
		onChange,
		value
	}: {
		placeholder: string;
		onChange: (value: string) => void;
		value: string;
	}) => (
		<input
			data-testid="react-quill"
			placeholder={placeholder}
			value={value}
			onChange={(event) => onChange(event.target.value)}
		/>
	)
}));

const theme = createTheme();

const TestHarness = ({
	typeName,
	name = "testAttr",
	defaultValue,
	showFormValue = false
}: {
	typeName: string;
	name?: string;
	defaultValue?: unknown;
	showFormValue?: boolean;
}) => {
	const resolvedDefault =
		defaultValue !== undefined
			? defaultValue
			: typeName.startsWith("array<")
				? []
				: "";

	const { control } = useForm({
		defaultValues: {
			businessMetadata: [
				{ key: { obj: { name, typeName } }, value: resolvedDefault }
			]
		}
	});

	const currentValue = useWatch({
		control,
		name: "businessMetadata.0.value"
	});

	return (
		<ThemeProvider theme={theme}>
			<BMAttributesFields
				obj={{ name, typeName }}
				control={control}
				index={0}
			/>
			{showFormValue && (
				<span data-testid="form-value">{JSON.stringify(currentValue)}</span>
			)}
		</ThemeProvider>
	);
};

describe("BMAttributesFields enum rendering", () => {
	beforeEach(() => {
		mockUseAppSelector.mockImplementation((selector) =>
			selector({
				enum: {
					enumObj: {
						loading: false,
						data: {
							enumDefs: mockEnumDefs
						},
						error: null
					}
				}
			})
		);
	});

	it("renders MUI Select dropdown for single-value enum (positive)", () => {
		render(<TestHarness typeName="adls_gen2_replication" defaultValue="LRS" />);

		expect(
			screen.getByLabelText(/Select testAttr enum value/i)
		).toBeInTheDocument();
		const select = screen.getByRole("combobox");
		expect(select).toBeInTheDocument();
		expect(screen.getByText("LRS")).toBeInTheDocument();

		fireEvent.mouseDown(select);
		expect(screen.getByRole("option", { name: "ZRS" })).toBeInTheDocument();
		expect(screen.getByRole("option", { name: "GRS" })).toBeInTheDocument();
	});

	it("updates single enum value on selection change (positive)", () => {
		render(<TestHarness typeName="adls_gen2_replication" defaultValue="" />);

		const select = screen.getByRole("combobox");
		fireEvent.mouseDown(select);
		fireEvent.click(screen.getByRole("option", { name: "GRS" }));
		expect(select).toHaveTextContent("GRS");
	});

	it("shows placeholder option for new single-value enum (positive)", () => {
		render(<TestHarness typeName="adls_gen2_replication" defaultValue="" />);

		expect(screen.getByText("--Select Value--")).toBeInTheDocument();
	});

	it("renders free-text Autocomplete for array<double> (positive)", () => {
		render(<TestHarness typeName="array<double>" defaultValue={["1.5"]} />);

		expect(screen.getByRole("combobox")).toBeInTheDocument();
		expect(
			screen.getByPlaceholderText("Select a array<double> from the dropdown list")
		).toBeInTheDocument();
	});

	it("renders constrained multi-select for array enum (positive)", () => {
		render(
			<TestHarness
				typeName="array<adls_gen2_replication>"
				defaultValue={["LRS", "ZRS"]}
			/>
		);

		const autocomplete = screen.getByRole("combobox");
		expect(autocomplete).toBeInTheDocument();
		expect(screen.getByText("LRS")).toBeInTheDocument();
		expect(screen.getByText("ZRS")).toBeInTheDocument();
	});

	it("keeps free-text searchable Autocomplete for array<string> (negative)", () => {
		render(<TestHarness typeName="array<string>" defaultValue={["alpha"]} />);

		const autocomplete = screen.getByRole("combobox");
		expect(autocomplete).toBeInTheDocument();
		expect(
			screen.getByPlaceholderText("Select a array<string> from the dropdown list")
		).toBeInTheDocument();
	});

	it("shows loading state while enum definitions are fetching (positive)", () => {
		mockUseAppSelector.mockImplementationOnce((selector) =>
			selector({
				enum: {
					enumObj: {
						loading: true,
						data: null,
						error: null
					}
				}
			})
		);

		render(<TestHarness typeName="adls_gen2_replication" defaultValue="" />);

		expect(screen.getByLabelText("Loading enum definitions")).toBeInTheDocument();
		expect(screen.getByText("Loading enum definitions...")).toBeInTheDocument();
		expect(screen.queryByRole("combobox")).not.toBeInTheDocument();
	});

	it("shows readable message when no enum definitions are present (negative)", () => {
		mockUseAppSelector.mockImplementationOnce((selector) =>
			selector({
				enum: {
					enumObj: {
						loading: false,
						data: {
							enumDefs: []
						},
						error: null
					}
				}
			})
		);

		render(<TestHarness typeName="adls_gen2_replication" defaultValue="" />);

		expect(screen.getByText("No enum definitions present.")).toBeInTheDocument();
		expect(screen.queryByRole("combobox")).not.toBeInTheDocument();
	});

	it("falls back to TextField for unknown enum when enum definitions exist (negative)", () => {
		render(<TestHarness typeName="array<unknown_enum>" defaultValue="" />);

		expect(screen.getByPlaceholderText("testAttr")).toBeInTheDocument();
		expect(screen.queryByText("No enum definitions present.")).not.toBeInTheDocument();
	});
});

describe("BMAttributesFields primitive and date rendering", () => {
	beforeEach(() => {
		mockUseAppSelector.mockImplementation((selector) =>
			selector({
				enum: {
					enumObj: {
						loading: false,
						data: { enumDefs: mockEnumDefs },
						error: null
					}
				}
			})
		);
	});

	it("initializes single date when value is empty (positive)", () => {
		render(<TestHarness typeName="date" defaultValue={null} showFormValue />);

		const picker = screen.getByTestId("date-picker");
		expect(picker.getAttribute("data-selected")).toBeTruthy();
	});

	it("sets single date timestamp on picker change (positive)", async () => {
		render(
			<TestHarness
				typeName="date"
				defaultValue={SINGLE_DATE_TIMESTAMP}
				showFormValue
			/>
		);

		const picker = screen.getByTestId("date-picker");
		expect(picker).toHaveAttribute(
			"data-selected",
			String(SINGLE_DATE_TIMESTAMP)
		);

		await act(async () => {
			fireEvent.click(picker);
		});

		await waitFor(() => {
			expect(screen.getByTestId("form-value")).toHaveTextContent(
				String(SINGLE_DATE_TIMESTAMP)
			);
		});
	});

	it("uses current date when single date value is invalid (negative)", () => {
		render(
			<TestHarness typeName="date" defaultValue="not-a-valid-date" showFormValue />
		);

		const picker = screen.getByTestId("date-picker");
		expect(picker).toBeInTheDocument();
		expect(picker.getAttribute("data-selected")).toBeTruthy();
	});

	it("sets multi date timestamps on picker change (positive)", async () => {
		render(
			<TestHarness
				typeName="array<date>"
				defaultValue={[SINGLE_DATE_TIMESTAMP]}
				showFormValue
			/>
		);

		const picker = screen.getByTestId("date-picker-multi");
		expect(picker).toHaveAttribute(
			"data-selected-dates",
			String(SINGLE_DATE_TIMESTAMP)
		);

		await act(async () => {
			fireEvent.click(picker);
		});

		await waitFor(() => {
			expect(screen.getByTestId("form-value")).toHaveTextContent(
				JSON.stringify(MULTI_DATE_TIMESTAMPS)
			);
		});
	});

	it("renders empty multi date picker when no dates selected (negative)", () => {
		render(<TestHarness typeName="array<date>" defaultValue={[]} showFormValue />);

		const picker = screen.getByTestId("date-picker-multi");
		expect(picker).toHaveAttribute("data-selected-dates", "");
		expect(screen.getByTestId("form-value")).toHaveTextContent("[]");
	});

	it("renders ReactQuill for string type (positive)", () => {
		render(<TestHarness typeName="string" defaultValue="hello" />);

		const editor = screen.getByTestId("react-quill");
		expect(editor).toBeInTheDocument();
		fireEvent.change(editor, { target: { value: "updated" } });
		expect(editor).toHaveValue("updated");
	});

	it("renders numeric TextField for int and converts on blur (positive)", () => {
		render(<TestHarness typeName="int" defaultValue="" />);

		const input = screen.getByPlaceholderText("testAttr");
		fireEvent.change(input, { target: { value: "42" } });
		fireEvent.blur(input);
		expect(input).toBeInTheDocument();
	});

	it("renders float TextField with step any (positive)", () => {
		render(<TestHarness typeName="float" defaultValue={1.5} />);

		expect(screen.getByPlaceholderText("testAttr")).toBeInTheDocument();
	});

	it("renders long and short numeric fields (positive)", () => {
		render(<TestHarness typeName="long" defaultValue={100} />);
		expect(screen.getByPlaceholderText("testAttr")).toBeInTheDocument();

		render(<TestHarness typeName="short" defaultValue={5} />);
		expect(screen.getAllByPlaceholderText("testAttr").length).toBeGreaterThan(0);
	});

	it("renders double numeric field (positive)", () => {
		render(<TestHarness typeName="double" defaultValue={2.5} />);
		expect(screen.getByPlaceholderText("testAttr")).toBeInTheDocument();
	});

	it("renders boolean Select with placeholder (positive)", () => {
		render(<TestHarness typeName="boolean" defaultValue={null} />);

		const select = screen.getByRole("combobox");
		expect(select).toHaveTextContent("--Select true or false--");
		fireEvent.mouseDown(select);
		fireEvent.click(screen.getByRole("option", { name: "true" }));
		expect(select).toHaveTextContent("true");
	});

	it("renders array<int> autocomplete (positive)", () => {
		render(<TestHarness typeName="array<int>" defaultValue={[1, 2]} />);
		expect(
			screen.getByPlaceholderText("Select a array<int> from the dropdown list")
		).toBeInTheDocument();
	});

	it("renders array<boolean> without free-text filter additions (negative)", () => {
		render(<TestHarness typeName="array<boolean>" defaultValue={["true"]} />);
		expect(screen.getByRole("combobox")).toBeInTheDocument();
	});

	it("renders array<long> autocomplete (positive)", () => {
		render(<TestHarness typeName="array<long>" defaultValue={[10]} />);
		expect(
			screen.getByPlaceholderText("Select a array<long> from the dropdown list")
		).toBeInTheDocument();
	});

	it("renders array<float> and array<short> autocomplete (positive)", () => {
		render(<TestHarness typeName="array<float>" defaultValue={[1.1]} />);
		expect(
			screen.getByPlaceholderText("Select a array<float> from the dropdown list")
		).toBeInTheDocument();

		render(<TestHarness typeName="array<short>" defaultValue={[2]} />);
		expect(
			screen.getByPlaceholderText("Select a array<short> from the dropdown list")
		).toBeInTheDocument();
	});

	it("updates multi enum selection through autocomplete (positive)", () => {
		render(
			<TestHarness
				typeName="array<adls_gen2_replication>"
				defaultValue={["LRS"]}
			/>
		);

		const autocomplete = screen.getByRole("combobox");
		fireEvent.mouseDown(autocomplete);
		fireEvent.click(screen.getByRole("option", { name: "GRS" }));
		expect(screen.getByText("GRS")).toBeInTheDocument();
	});

	it("renders numeric input attributes for int fields (positive)", () => {
		render(<TestHarness typeName="int" defaultValue={42} name="intAttr" />);
		const input = screen.getByPlaceholderText("intAttr");
		expect(input).toHaveAttribute("type", "number");
	});
});
