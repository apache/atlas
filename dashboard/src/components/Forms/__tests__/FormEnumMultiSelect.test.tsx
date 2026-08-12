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
import { render, screen } from "@utils/test-utils";
import userEvent from "@testing-library/user-event";
import { useForm } from "react-hook-form";
import FormEnumMultiSelect from "../FormEnumMultiSelect";

const optionsList = [
	{ label: "LRS", value: "LRS" },
	{ label: "ZRS", value: "ZRS" },
	{ label: "GRS", value: "GRS" }
];

const TestForm = ({
	defaultValue = []
}: {
	defaultValue?: unknown;
}) => {
	const { control } = useForm({
		defaultValues: {
			replicationTypes: defaultValue
		}
	});

	return (
		<FormEnumMultiSelect
			data={{
				name: "replicationTypes",
				isOptional: false,
				typeName: "array<adls_gen2_replication>",
				cardinality: "SET"
			}}
			control={control}
			optionsList={optionsList}
		/>
	);
};

describe("FormEnumMultiSelect", () => {
	it("renders a multi-select dropdown for array enum attributes", async () => {
		const user = userEvent.setup();
		render(<TestForm defaultValue={["LRS"]} />);

		expect(screen.getByText("ReplicationTypes")).toBeInTheDocument();
		expect(screen.getByText("LRS")).toBeInTheDocument();

		const valueInput = screen.getByRole("combobox");
		await user.click(valueInput);
		expect(await screen.findByRole("option", { name: "ZRS" })).toBeInTheDocument();
		expect(screen.getByRole("option", { name: "GRS" })).toBeInTheDocument();
	});

	it("renders empty selection when no default value is provided", () => {
		render(<TestForm />);

		expect(screen.getByPlaceholderText("Select enum values")).toBeInTheDocument();
	});

	it("allows optional multi enum to remain empty (negative validation path)", () => {
		const OptionalForm = () => {
			const { control } = useForm({
				defaultValues: {
					storageTypes: []
				}
			});

			return (
				<FormEnumMultiSelect
					data={{
						name: "storageTypes",
						isOptional: true,
						typeName: "array<ozone_storage_type>",
						cardinality: "SET"
					}}
					control={control}
					optionsList={[
						{ label: "SSD", value: "SSD" },
						{ label: "DISK", value: "DISK" }
					]}
				/>
			);
		};

		render(<OptionalForm />);
		expect(screen.getByPlaceholderText("Select enum values")).toBeInTheDocument();
	});
});
