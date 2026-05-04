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

import { Divider, Stack } from "@mui/material";
import CustomModal from "@components/Modal";
import { getValues } from "@components/commonComponents";
import { StyledPaper } from "@utils/Muiutils";
import { category } from "@utils/Enum";
import { isArray, isEmpty } from "@utils/Utils";

export interface TypeDefAuditDetail {
	category?: string;
	name?: string;
	[key: string]: unknown;
}

interface TypeDefAuditDetailModalProps {
	open: boolean;
	onClose: () => void;
	/** Full type-definition payload from audit `result` (same shape as audit tab click). */
	detailObject: TypeDefAuditDetail | Record<string, unknown> | null;
	maxWidth?: "xs" | "sm" | "md" | "lg" | "xl";
}

const buildTitle = (detailObject: TypeDefAuditDetailModalProps["detailObject"]): string => {
	if (isEmpty(detailObject)) return "Type Details";
	const cat = detailObject?.category;
	const name = detailObject?.name;
	if (cat != null && name != null && category[String(cat)]) {
		return `${category[String(cat)]} Type Details: ${String(name)}`;
	}
	if (name != null) return `Type Details: ${String(name)}`;
	return "Type Details";
};

export const TypeDefAuditDetailModal = ({
	open,
	onClose,
	detailObject,
	maxWidth = "md",
}: TypeDefAuditDetailModalProps) => {
	return (
		<CustomModal
			open={open}
			onClose={onClose}
			title={buildTitle(detailObject)}
			footer={false}
			button1Handler={undefined}
			button2Handler={undefined}
			maxWidth={maxWidth}
		>
			<StyledPaper variant="outlined">
				{!isEmpty(detailObject)
					? Object.entries(detailObject as Record<string, unknown>)
							.sort(([a], [b]) => a.localeCompare(b))
							.map(([keys, value]) => (
								<div key={keys}>
									<Stack direction="row" spacing={4} marginBottom={1} marginTop={1}>
										<div
											style={{
												flex: 1,
												wordBreak: "break-all",
												textAlign: "left",
												fontWeight: "600",
											}}
										>
											{`${keys} ${isArray(value) ? `(${(value as unknown[]).length})` : ""}`}
										</div>
										<div
											style={{
												flex: 1,
												wordBreak: "break-all",
												textAlign: "left",
											}}
										>
											{getValues(
												value,
												undefined,
												undefined,
												undefined,
												"properties"
											)}
										</div>
									</Stack>
									<Divider />
								</div>
							))
					: "No Record Found"}
			</StyledPaper>
		</CustomModal>
	);
};

export default TypeDefAuditDetailModal;
