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

import CustomModal from "@components/Modal";
import GlossaryForm from "./GlossaryForm";
import { useForm } from "react-hook-form";
import {
	createGlossary,
	editGlossary
} from "@api/apiMethods/glossaryApiMethod";
import { isEmpty, serverError } from "@utils/Utils";
import {
	downloadGlossaryImportTemplate,
	postGlossaryImportFormData
} from "@utils/glossaryImportFlow";
import { getApiErrorToastMessage } from "@utils/apiErrorToastMessage";
import { toast } from "react-toastify";
import { useCallback, useEffect, useRef, useState } from "react";
import type { MouseEvent } from "react";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { fetchGlossaryData } from "@redux/slice/glossarySlice";
import {
	Button,
	IconButton,
	List,
	ListItem,
	ListItemText,
	Stack,
	ToggleButton,
	ToggleButtonGroup
} from "@mui/material";
import FileDownloadIcon from "@mui/icons-material/FileDownload";
import ArrowBackIosNewIcon from "@mui/icons-material/ArrowBackIosNew";
import ImportLayout from "@views/SideBar/Import/ImportLayout";
import { LightTooltip } from "@components/muiComponents";

type GlossaryCreateTab = "create" | "import";

const hasSelectedImportFile = (fileData: unknown): boolean => {
	if (fileData == null) return false;
	if (fileData instanceof File) return true;
	if (typeof fileData === "object" && Object.keys(fileData as object).length > 0)
		return true;
	return false;
};

const AddUpdateGlossaryForm = (props: {
	open: any;
	onClose: any;
	isAdd: any;
	node: Record<string, any> | undefined;
}) => {
	const { open, onClose, isAdd, node } = props;
	const dispatch = useAppDispatch();
	const toastId: any = useRef(null);
	const { glossaryData }: any = useAppSelector((state: any) => state.glossary);
	const { id } = node || {};
	let defaultValue: Record<string, string> = {};
	let glossaryObj: Record<string, string> = {};
	if (!isAdd) {
		glossaryObj = glossaryData.find((obj: { name: string }) => {
			return obj.name == id;
		});
		const { name, shortDescription, longDescription } = glossaryObj || {};

		defaultValue["name"] = name;
		defaultValue["shortDescription"] = shortDescription;
		defaultValue["longDescription"] = longDescription;
	}
	const {
		control,
		handleSubmit,
		setValue,
		formState: { isSubmitting }
	} = useForm({
		defaultValues: isAdd ? {} : defaultValue,
		mode: "onChange",
		shouldUnregister: true
	});

	const [glossaryCreateTab, setGlossaryCreateTab] =
		useState<GlossaryCreateTab>("create");
	const [importFileData, setImportFileData] = useState<any>([]);
	const [importProgress, setImportProgress] = useState(0);
	const [importErrorDetails, setImportErrorDetails] = useState(false);
	const [importData, setImportData] = useState<any>(null);
	const [importUploading, setImportUploading] = useState(false);

	const resetImportUi = useCallback(() => {
		setGlossaryCreateTab("create");
		setImportFileData([]);
		setImportProgress(0);
		setImportErrorDetails(false);
		setImportData(null);
		setImportUploading(false);
	}, []);

	const handleModalClose = useCallback(() => {
		resetImportUi();
		onClose();
	}, [onClose, resetImportUi]);

	useEffect(() => {
		if (!open) {
			resetImportUi();
		}
	}, [open, resetImportUi]);

	const handleGlossaryCreateTabChange = (
		_event: MouseEvent<HTMLElement>,
		next: GlossaryCreateTab | null
	) => {
		if (next == null) return;
		setGlossaryCreateTab(next);
		if (next === "create") {
			setImportFileData([]);
			setImportProgress(0);
			setImportErrorDetails(false);
			setImportData(null);
		}
	};

	const handleDownloadTemplate = async () => {
		try {
			await downloadGlossaryImportTemplate();
		} catch {
			toast.dismiss(toastId.current);
			toastId.current = toast.error("Could not download template");
		}
	};

	const handleImportUpload = async () => {
		if (!hasSelectedImportFile(importFileData)) return;
		setImportUploading(true);
		try {
			const importResp = await postGlossaryImportFormData(
				importFileData,
				(pct) => setImportProgress(pct)
			);
			if (importResp.data.failedImportInfoList == undefined) {
				toast.dismiss(toastId.current);
				toastId.current = toast.success(
					`File: ${importFileData.name} imported successfully`
				);
				await dispatch(fetchGlossaryData());
				handleModalClose();
				return;
			}
			if (importResp.data.failedImportInfoList != undefined) {
				toast.dismiss(toastId.current);
				toastId.current = toast.error(
					importResp.data.failedImportInfoList[0].remarks
				);
				setImportErrorDetails(true);
			}
			setImportData(importResp.data);
		} catch (error) {
			const message = getApiErrorToastMessage(error);
			if (message === null) {
				return;
			}
			toast.dismiss(toastId.current);
			toastId.current = toast.error(message);
		} finally {
			setImportUploading(false);
		}
	};

	const onSubmit = async (formValues: any) => {
		let formData = { ...formValues };
		const { guid, qualifiedName } = glossaryObj;
		const {
			name,
			shortDescription,
			longDescription
		}: { name: string; shortDescription: string; longDescription: string } =
			formData;
		let data: Record<string, string> = {};
		if (!isAdd) {
			data["guid"] = guid;
			data["qualifiedName"] = qualifiedName;
		}
		data["name"] = name;
		data["shortDescription"] = !isEmpty(shortDescription)
			? shortDescription
			: "";
		data["longDescription"] = !isEmpty(longDescription) ? longDescription : "";

		try {
			if (isAdd) {
				await createGlossary(data);
			} else {
				await editGlossary(guid, data);
			}

			await dispatch(fetchGlossaryData());
			toast.dismiss(toastId.current);
			toastId.current = toast.success(
				`Glossary ${name} was ${isAdd ? "created" : "updated"} successfully`
			);
			handleModalClose();
		} catch (error) {
			serverError(error, toastId);
		}
	};

	const showImportError = isAdd && glossaryCreateTab === "import" && importErrorDetails;

	const modalTitle = showImportError
		? "Error Details"
		: isAdd
			? "Create Glossary"
			: "Edit Glossary";

	const titleIconNode =
		showImportError ? (
			<IconButton
				aria-label="Back to import file"
				onClick={(e) => {
					e.stopPropagation();
					setImportErrorDetails(false);
				}}
				size="small"
				sx={{ color: (theme) => theme.palette.grey[500] }}
			>
				<LightTooltip title="Back to import file">
					<ArrowBackIosNewIcon sx={{ fontSize: "1.25rem" }} />
				</LightTooltip>
			</IconButton>
		) : undefined;

	const isImportMode = isAdd && glossaryCreateTab === "import" && !importErrorDetails;

	const button2Label = (() => {
		if (!isAdd) return "Update";
		if (glossaryCreateTab === "create") return "Create";
		if (importErrorDetails) return "";
		return "Upload";
	})();

	const button2Handler = (() => {
		if (!isAdd || glossaryCreateTab === "create") return handleSubmit(onSubmit);
		if (importErrorDetails) return () => {};
		return handleImportUpload;
	})();

	const disableButton2 = (() => {
		if (!isAdd) return isSubmitting;
		if (glossaryCreateTab === "create") return isSubmitting;
		if (importErrorDetails) return true;
		return !hasSelectedImportFile(importFileData);
	})();

	const button2Loading =
		isAdd && glossaryCreateTab === "import" && !importErrorDetails
			? importUploading
			: isSubmitting;

	const hideButton2 = Boolean(
		isAdd && glossaryCreateTab === "import" && importErrorDetails
	);

	return (
		<>
			<CustomModal
				open={open}
				onClose={handleModalClose}
				title={modalTitle}
				titleIcon={titleIconNode}
				button1Label="Cancel"
				button1Handler={handleModalClose}
				button2Label={button2Label}
				maxWidth="sm"
				button2Handler={button2Handler}
				disableButton2={disableButton2}
				button2Loading={button2Loading}
				hideButton2={hideButton2}
			>
				<Stack>
					{isAdd && (
						<Stack marginBottom="1rem">
							<ToggleButtonGroup
								exclusive
								value={glossaryCreateTab}
								onChange={handleGlossaryCreateTabChange}
								size="small"
								color="primary"
								aria-label="Choose create glossary or import glossary terms"
							>
								<ToggleButton
									value="create"
									className="entity-form-toggle-btn"
									data-cy="create-glossary-tab"
								>
									Create glossary
								</ToggleButton>
								<ToggleButton
									value="import"
									className="entity-form-toggle-btn"
									data-cy="import-glossary-terms-tab"
								>
									Import glossary terms
								</ToggleButton>
							</ToggleButtonGroup>
						</Stack>
					)}
					{(!isAdd || glossaryCreateTab === "create") && (
						<GlossaryForm
							control={control}
							handleSubmit={handleSubmit(onSubmit)}
							setValue={setValue}
						/>
					)}
					{isImportMode && (
						<Stack gap={2}>
							<Button
								variant="outlined"
								color="primary"
								startIcon={<FileDownloadIcon />}
								onClick={handleDownloadTemplate}
								aria-label="Download import template"
							>
								Download import template
							</Button>
							<ImportLayout
								setFileData={setImportFileData}
								progressVal={importProgress}
								setProgress={setImportProgress}
								selectedFile={
									importFileData !== undefined &&
									hasSelectedImportFile(importFileData)
										? [importFileData]
										: []
								}
								errorDetails={importErrorDetails}
							/>
						</Stack>
					)}
					{showImportError && importData?.failedImportInfoList && (
						<Stack
							sx={{
								width: "100%",
								minHeight: 200,
								maxHeight: 400,
								bgcolor: "background.paper"
							}}
						>
							<List>
								{importData.failedImportInfoList.map(
									(
										value: {
											index: number;
											remarks: string;
										},
										index: number
									) => (
										<ListItem key={value.index} disableGutters disablePadding>
											<ListItemText
												className="dropzone-listitem"
												primary={`${index + 1}. ${value.remarks}`}
											/>
										</ListItem>
									)
								)}
							</List>
						</Stack>
					)}
				</Stack>
			</CustomModal>
		</>
	);
};

export default AddUpdateGlossaryForm;
