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
import { render, screen } from "@testing-library/react";
import { useForm } from "react-hook-form";
import { renderEntityFormControl } from "../Entity/entityFormFields";

jest.mock("@components/Forms/FormInputText", () => ({
	__esModule: true,
	default: ({ data }: { data: { name: string } }) => (
		<input data-testid={`input-${data.name}`} />
	)
}));

jest.mock("@components/Forms/FormSingleSelect", () => ({
	__esModule: true,
	default: ({ data }: { data: { name: string } }) => (
		<select data-testid={`enum-single-${data.name}`} />
	)
}));

jest.mock("@components/Forms/FormEnumMultiSelect", () => ({
	__esModule: true,
	default: ({ data }: { data: { name: string } }) => (
		<select multiple data-testid={`enum-multi-${data.name}`} />
	)
}));

jest.mock("@components/Forms/FormAutocomplete", () => ({
	__esModule: true,
	default: ({ data }: { data: { name: string } }) => (
		<input data-testid={`autocomplete-${data.name}`} />
	)
}));

jest.mock("@components/Forms/FormCreatableSelect", () => ({
	__esModule: true,
	default: ({ data }: { data: { name: string } }) => (
		<input data-testid={`creatable-${data.name}`} />
	)
}));

jest.mock("@components/Forms/FormSelectBoolean", () => ({
	__esModule: true,
	default: ({ data }: { data: { name: string } }) => (
		<select data-testid={`boolean-${data.name}`} />
	)
}));

jest.mock("@components/Forms/FormDatepicker", () => ({
	__esModule: true,
	default: ({ data }: { data: { name: string } }) => (
		<input data-testid={`date-${data.name}`} />
	)
}));

jest.mock("@components/Forms/FormTextArea", () => ({
	__esModule: true,
	default: ({ data }: { data: { name: string } }) => (
		<textarea data-testid={`textarea-${data.name}`} />
	)
}));

const mockEnumDefs = [
	{
		name: "adls_gen2_replication",
		elementDefs: [{ value: "LRS" }, { value: "ZRS" }, { value: "GRS" }]
	},
	{
		name: "ozone_storage_type",
		elementDefs: [
			{ value: "RAM_DISK" },
			{ value: "SSD" },
			{ value: "DISK" },
			{ value: "ARCHIVE" }
		]
	}
];

const TestHarness = ({
	typeName,
	name,
	cardinality = "SINGLE"
}: {
	typeName: string;
	name: string;
	cardinality?: string;
}) => {
	const { control } = useForm();

	return renderEntityFormControl({
		obj: {
			name,
			typeName,
			isOptional: true,
			cardinality
		},
		control,
		entityDefs: [{ name: "DataSet" }],
		typeHeaderData: [{ name: "CustomStruct", category: "STRUCT" }],
		enumDefs: mockEnumDefs
	});
};

describe("entityFormFields enum rendering", () => {
	it("renders single enum attributes as dropdown select", () => {
		render(
			<TestHarness
				name="replication"
				typeName="adls_gen2_replication"
			/>
		);

		expect(screen.getByTestId("enum-single-replication")).toBeInTheDocument();
	});

	it("renders array enum attributes as multi-select dropdown", () => {
		render(
			<TestHarness
				name="replicationTypes"
				typeName="array<adls_gen2_replication>"
				cardinality="SET"
			/>
		);

		expect(screen.getByTestId("enum-multi-replicationTypes")).toBeInTheDocument();
	});

	it("falls back to text input for unknown enum typedef names", () => {
		render(
			<TestHarness name="unknownEnum" typeName="unknown_enum_type" />
		);

		expect(screen.getByTestId("input-unknownEnum")).toBeInTheDocument();
		expect(
			screen.queryByTestId("enum-single-unknownEnum")
		).not.toBeInTheDocument();
	});

	it("keeps primitive string attributes on text input", () => {
		render(<TestHarness name="description" typeName="string" />);

		expect(screen.getByTestId("input-description")).toBeInTheDocument();
		expect(screen.queryByTestId("enum-single-description")).not.toBeInTheDocument();
	});

	it("keeps entity reference arrays on autocomplete instead of enum dropdown", () => {
		render(
			<TestHarness
				name="inputs"
				typeName="array<DataSet>"
				cardinality="SET"
			/>
		);

		expect(screen.getByTestId("autocomplete-inputs")).toBeInTheDocument();
		expect(screen.queryByTestId("enum-multi-inputs")).not.toBeInTheDocument();
	});

	it("renders ozone_storage_type single enum as dropdown (positive)", () => {
		render(
			<TestHarness name="storageType" typeName="ozone_storage_type" />
		);

		expect(screen.getByTestId("enum-single-storageType")).toBeInTheDocument();
		expect(screen.queryByTestId("input-storageType")).not.toBeInTheDocument();
	});

	it("renders array<ozone_storage_type> as multi-select (positive)", () => {
		render(
			<TestHarness
				name="storageTypes"
				typeName="array<ozone_storage_type>"
				cardinality="SET"
			/>
		);

		expect(screen.getByTestId("enum-multi-storageTypes")).toBeInTheDocument();
	});

	it("renders map attributes as textarea instead of enum dropdown (negative)", () => {
		render(
			<TestHarness name="metadata" typeName="map<string,string>" />
		);

		expect(screen.getByTestId("textarea-metadata")).toBeInTheDocument();
		expect(
			screen.queryByTestId("enum-single-metadata")
		).not.toBeInTheDocument();
	});

	it("renders boolean attributes as boolean select instead of enum dropdown (negative)", () => {
		render(<TestHarness name="isActive" typeName="boolean" />);

		expect(screen.getByTestId("boolean-isActive")).toBeInTheDocument();
		expect(screen.queryByTestId("enum-single-isActive")).not.toBeInTheDocument();
	});

	it("falls back to text input when enumDefs list is empty (negative)", () => {
		const EmptyEnumHarness = () => {
			const { control } = useForm();

			return renderEntityFormControl({
				obj: {
					name: "storageType",
					typeName: "ozone_storage_type",
					isOptional: true
				},
				control,
				entityDefs: [{ name: "DataSet" }],
				typeHeaderData: [],
				enumDefs: []
			});
		};

		render(<EmptyEnumHarness />);

		expect(screen.getByTestId("input-storageType")).toBeInTheDocument();
		expect(
			screen.queryByTestId("enum-single-storageType")
		).not.toBeInTheDocument();
	});

	it("renders date attributes with datepicker (hdfs_path-style primitive)", () => {
		render(<TestHarness name="modifiedTime" typeName="date" />);

		expect(screen.getByTestId("date-modifiedTime")).toBeInTheDocument();
		expect(
			screen.queryByTestId("enum-single-modifiedTime")
		).not.toBeInTheDocument();
	});

	it("renders struct attributes as textarea instead of enum dropdown", () => {
		const StructHarness = () => {
			const { control } = useForm();

			return renderEntityFormControl({
				obj: {
					name: "customStruct",
					typeName: "CustomStruct",
					isOptional: true
				},
				control,
				entityDefs: [],
				typeHeaderData: [{ name: "CustomStruct", category: "STRUCT" }],
				enumDefs: mockEnumDefs
			});
		};

		render(<StructHarness />);

		expect(screen.getByTestId("textarea-customStruct")).toBeInTheDocument();
		expect(
			screen.queryByTestId("enum-single-customStruct")
		).not.toBeInTheDocument();
	});

	it("renders entity reference arrays with creatable select for SINGLE cardinality", () => {
		render(
			<TestHarness
				name="path"
				typeName="array<hdfs_path>"
				cardinality="SINGLE"
			/>
		);

		expect(screen.getByTestId("creatable-path")).toBeInTheDocument();
		expect(screen.queryByTestId("enum-multi-path")).not.toBeInTheDocument();
	});
});
