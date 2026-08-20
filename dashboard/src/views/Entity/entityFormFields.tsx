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

import FormAutocomplete from "@components/Forms/FormAutocomplete";
import FormCreatableSelect from "@components/Forms/FormCreatableSelect";
import FormDatepicker from "@components/Forms/FormDatepicker";
import FormEnumMultiSelect from "@components/Forms/FormEnumMultiSelect";
import FormInputText from "@components/Forms/FormInputText";
import FormSelectBoolean from "@components/Forms/FormSelectBoolean";
import FormSingleSelect from "@components/Forms/FormSingleSelect";
import FormTextArea from "@components/Forms/FormTextArea";
import {
	buildEnumOptionsForTypeName,
	EnumDef,
	isArrayTypeName,
	isEnumTypeName
} from "@utils/enumTypeUtils";

export const renderEntityFormControl = ({
	obj,
	control,
	entityDefs,
	typeHeaderData,
	enumDefs
}: {
	obj: {
		name: string;
		typeName: string;
		isOptional: boolean;
		cardinality?: string;
	};
	control: any;
	entityDefs: Array<{ name: string }>;
	typeHeaderData: Array<{ name: string; category?: string }>;
	enumDefs: EnumDef[];
}) => {
	const attributeObj = entityDefs.find(
		(typedef) => typedef.name === obj.typeName
	);
	const typeHeaderObj = typeHeaderData.find(
		(typeheader) => typeheader.name === obj.typeName
	);
	const enumOptions = buildEnumOptionsForTypeName(obj.typeName, enumDefs);
	const isEnumType = isEnumTypeName(obj.typeName, enumDefs);
	const isMultiEnumType = isArrayTypeName(obj.typeName) && isEnumType;
	const isEntityReference = Boolean(attributeObj);
	const isArrayType = obj.typeName.indexOf("array") > -1;

	if (
		obj.typeName.indexOf("map") > -1 ||
		typeHeaderObj?.category === "STRUCT"
	) {
		return <FormTextArea data={obj} control={control} />;
	}

	if (isMultiEnumType) {
		return (
			<FormEnumMultiSelect
				data={obj}
				control={control}
				optionsList={enumOptions}
			/>
		);
	}

	if (isEnumType) {
		return (
			<FormSingleSelect
				data={obj}
				control={control}
				optionsList={enumOptions}
				typeName={obj.typeName}
			/>
		);
	}

	if (isEntityReference || (isArrayType && obj.cardinality === "SET")) {
		return <FormAutocomplete data={obj} control={control} />;
	}

	if (isEntityReference || (isArrayType && obj.cardinality === "SINGLE")) {
		return <FormCreatableSelect data={obj} control={control} />;
	}

	if (obj.typeName === "boolean") {
		return <FormSelectBoolean data={obj} control={control} />;
	}

	if (obj.typeName === "date" || obj.typeName === "time") {
		return <FormDatepicker data={obj} control={control} />;
	}

	return <FormInputText data={obj} control={control} />;
};
