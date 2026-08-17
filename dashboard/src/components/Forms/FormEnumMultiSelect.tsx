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

import { LightTooltip } from "@components/muiComponents";
import { Autocomplete, InputLabel, TextField, Typography } from "@mui/material";
import {
	areEnumOptionsEqual,
	EnumOption,
	getEnumOptionLabel,
	normalizeMultiEnumValue
} from "@utils/enumTypeUtils";
import { Capitalize, isEmpty } from "@utils/Utils";
import { Controller } from "react-hook-form";

const FormEnumMultiSelect = ({
	data,
	control,
	optionsList,
	fieldName
}: {
	data: { name: string; isOptional: boolean; typeName: string; cardinality?: string };
	control: any;
	optionsList: EnumOption[];
	fieldName?: string;
}) => {
	const { name, isOptional, typeName, cardinality } = data;

	return (
		<Controller
			name={!isEmpty(fieldName) ? `${fieldName}.${name}` : name}
			control={control}
			key={`enum-multi-${name}`}
			rules={{
				required: isOptional ? false : true
			}}
			defaultValue={[]}
			render={({ field: { onChange, value }, fieldState: { error } }) => {
				const selectedValues = normalizeMultiEnumValue(value, optionsList);

				return (
					<>
						<div className="form-fields">
							<InputLabel
								className="form-textfield-label"
								required={isOptional ? false : true}
							>
								{Capitalize(name)}
							</InputLabel>
							<LightTooltip title={`Data Type: (${typeName})`}>
								<Typography
									color="#666666"
									overflow="hidden"
									maxWidth="160px"
									fontSize={14}
									noWrap
								>{`(${typeName})${cardinality ? ` ${cardinality}` : ""}`}</Typography>
							</LightTooltip>
						</div>
						<Autocomplete
							size="small"
							multiple
							disableCloseOnSelect
							className="form-autocomplete-field"
							onChange={(_event, selectedOptions) => {
								onChange(selectedOptions);
							}}
							sx={{ width: "100%" }}
							value={selectedValues}
							filterSelectedOptions
							getOptionLabel={getEnumOptionLabel}
							isOptionEqualToValue={areEnumOptionsEqual}
							options={optionsList}
							renderInput={(params) => (
								<TextField
									{...params}
									error={!!error}
									className="form-textfield"
									size="small"
									InputProps={{
										...params.InputProps
									}}
									placeholder="Select enum values"
								/>
							)}
						/>
					</>
				);
			}}
		/>
	);
};

export default FormEnumMultiSelect;
