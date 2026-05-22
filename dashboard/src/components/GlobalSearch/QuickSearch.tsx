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

import {
	Autocomplete,
	CircularProgress,
	FormControl,
	InputAdornment,
	MenuItem,
	Select,
	Stack,
	TextField,
	Typography,
	type SelectChangeEvent
} from "@mui/material";
import { useCallback, useMemo, useRef, useState } from "react";
import { getGlobalSearchResult } from "../../api/apiMethods/searchApiMethod";
import DisplayImage from "../EntityDisplayImage";
import SearchIcon from "@mui/icons-material/Search";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { entityStateReadOnly } from "../../utils/Enum";
import {
	extractKeyValueFromEntity,
	isEmpty,
	serverError
} from "../../utils/Utils";
import parse from "autosuggest-highlight/parse";
import match from "autosuggest-highlight/match";
import ClickAwayListener from "@mui/material/ClickAwayListener";
import AdvancedSearch from "./AdvancedSearch";
import {
	HandleValuesType,
	QuickSearchOptionListType,
	SuggestionDataType
} from "../../models/globalSearchType";
import { AxiosResponse } from "axios";
import { CustomButton } from "@components/muiComponents";
import { useAppSelector } from "@hooks/reducerHook";
import {
	buildBusinessMetadataScopedOptions,
	buildClassificationScopedOptions,
	buildEntitiesTreeForQuickSearch,
	buildGlossaryTermsTreeForQuickSearch,
	entityTreeToScopedOptions,
	glossaryTreeToScopedOptions,
	type QuickSearchScope,
	type ScopedQuickSearchOption
} from "@utils/scopedQuickSearchUtils";
import {
	navigateToBasicTextQuery,
	navigateToBusinessMetadataDetailPage,
	navigateToClassificationDetailPage,
	navigateToGlossaryTermDetailPage,
	navigateToServiceTypeEntitySearch
} from "@utils/dashboardSearchUtils";

interface GlobalOptionRow {
	title: string;
	types: string;
	entityObj?: any;
	scoped?: ScopedQuickSearchOption;
}

const SCOPE_SELECT_ID = "quick-search-scope-select";

const SCOPE_LABELS: Record<QuickSearchScope, string> = {
	default: "Select All",
	entity: "Entity",
	classification: "Classification",
	glossary: "Glossary / Terms",
	businessMetadata: "Business Metadata"
};

const QuickSearch = () => {
	const navigate = useNavigate();
	const location = useLocation();
	const toastId = useRef(null);
	const searchParams = new URLSearchParams(location.search);
	const [options, setOptions] = useState<GlobalOptionRow[]>([]);
	const [open, setOpen] = useState<boolean>(false);
	const [openAdvanceSearch, setOpenAdvanceSearch] = useState<boolean>(false);
	const [loading, setLoading] = useState<boolean>(false);
	const [inputText, setInputText] = useState<string>("");
	const [scope, setScope] = useState<QuickSearchScope>("default");

	const { typeHeaderData } = useAppSelector((state: any) => state.typeHeader);
	const { metricsData } = useAppSelector((state: any) => state.metrics);
	const { allEntityTypesData } = useAppSelector((state: any) => state.allEntityTypes);
	const { classificationData } = useAppSelector((state: any) => state.classification);
	const { glossaryData } = useAppSelector((state: any) => state.glossary);
	const { businessMetaData } = useAppSelector((state: any) => state.businessMetaData);

	const entityTree = useMemo(
		() =>
			buildEntitiesTreeForQuickSearch(
				typeHeaderData,
				metricsData?.data?.entity,
				allEntityTypesData?.category
			),
		[typeHeaderData, metricsData, allEntityTypesData]
	);

	const rebuildScopedOptions = useCallback(
		(term: string): GlobalOptionRow[] => {
			if (scope === "entity") {
				return entityTreeToScopedOptions(entityTree, term).map((o) => ({
					title: o.title,
					types: o.group,
					scoped: o
				}));
			}
			if (scope === "classification") {
				return buildClassificationScopedOptions(
					classificationData,
					metricsData?.data?.tag?.tagEntities,
					term
				).map((o) => ({
					title: o.title,
					types: o.group,
					scoped: o
				}));
			}
			if (scope === "glossary") {
				const gTree = buildGlossaryTermsTreeForQuickSearch(glossaryData);
				return glossaryTreeToScopedOptions(gTree, term).map((o) => ({
					title: o.title,
					types: o.group,
					scoped: o
				}));
			}
			if (scope === "businessMetadata") {
				return buildBusinessMetadataScopedOptions(
					businessMetaData?.businessMetadataDefs,
					term
				).map((o) => ({
					title: o.title,
					types: o.group,
					scoped: o
				}));
			}
			return [];
		},
		[
			scope,
			entityTree,
			classificationData,
			metricsData,
			glossaryData,
			businessMetaData
		]
	);

	const getGlobalSearchData = async (searchTerm: string) => {
		let entities: QuickSearchOptionListType = [];
		let suggestionNames: QuickSearchOptionListType = [];
		let quickSearchResp: AxiosResponse | null = null;
		let suggestionSearchResp: AxiosResponse | null = null;
		try {
			setLoading(true);
			quickSearchResp = await getGlobalSearchResult("quick", {
				params: { query: searchTerm, limit: 5, offset: 0 }
			});
			setLoading(false);
		} catch (error) {
			setLoading(false);
			console.error("Error fetching quick search results:", error);
			serverError(error, toastId);
		}

		try {
			setLoading(true);
			suggestionSearchResp = await getGlobalSearchResult("suggestions", {
				params: { prefixString: searchTerm }
			});
			setLoading(false);
		} catch (error) {
			setLoading(false);
			console.error("Error fetching suggestion search results:", error);
			serverError(error, toastId);
		}
		const { searchResults = [] } = quickSearchResp?.data || {};
		const { suggestions = [] }: SuggestionDataType =
			suggestionSearchResp?.data || {};

		entities = !isEmpty(searchResults?.entities)
			? searchResults?.entities?.map((entityDef: any) => {
					const { name }: { name: string; found: boolean; key: any } =
						extractKeyValueFromEntity(entityDef);
					return {
						title: `${name}`,
						parent: entityDef.typeName,
						types: "Entities",
						entityObj: entityDef
					};
				})
			: [{ title: "No Entities Found", types: "Entities" }];

		suggestionNames = !isEmpty(suggestions)
			? suggestions.map((suggestion: any) => {
					return {
						title: `${suggestion}`,
						types: "Suggestions"
					};
				})
			: [{ title: "No Suggestions Found", types: "Suggestions" }];

		setOptions([...entities, ...suggestionNames] as GlobalOptionRow[]);
	};

	const onInputChange = (_event: unknown, value: string) => {
		const sanitizedValue = value ? value.trim() : "";
		setInputText(value ?? "");
		if (!sanitizedValue) {
			setOptions([]);
			setOpen(false);
			return;
		}
		setOpen(true);
		if (scope === "default") {
			void getGlobalSearchData(sanitizedValue);
		} else {
			setOptions(rebuildScopedOptions(sanitizedValue));
		}
	};

	const handleScopedSelection = (opt: ScopedQuickSearchOption) => {
		setOpen(false);
		setOptions([]);
		setInputText("");
		if (opt.kind === "entity-type" && opt.entityTypeName) {
			navigateToBasicTextQuery(navigate, opt.entityTypeName);
			return;
		}
		if (opt.kind === "entity-service" && opt.serviceUnderlyingTypeNames?.length) {
			navigateToServiceTypeEntitySearch(
				navigate,
				opt.serviceUnderlyingTypeNames,
				false
			);
			return;
		}
		if (opt.kind === "classification" && opt.classificationName) {
			navigateToClassificationDetailPage(navigate, opt.classificationName);
			return;
		}
		if (
			opt.kind === "glossary-term" &&
			opt.termGuid &&
			opt.glossaryGuid &&
			opt.termId &&
			opt.termParent
		) {
			navigateToGlossaryTermDetailPage(navigate, {
				termGuid: opt.termGuid,
				termId: opt.termId,
				glossaryGuid: opt.glossaryGuid,
				parentName: opt.termParent
			});
			return;
		}
		if (opt.kind === "business-metadata" && opt.bmGuid) {
			navigateToBusinessMetadataDetailPage(navigate, opt.bmGuid);
		}
	};

	const handleValues = (option: HandleValuesType | GlobalOptionRow | string | null) => {
		if (typeof option === "string") {
			const queryValue = option.trim();
			if (!queryValue) return;
			setOpen(false);
			setOptions([]);
			setInputText("");
			if (scope !== "default") {
				return;
			}
			const sanitizedQuery = queryValue || "*";
			searchParams.set("query", sanitizedQuery);
			searchParams.set("searchType", "basic");
			navigate(
				{
					pathname: `/search/searchResult`,
					search: searchParams.toString()
				},
				{ replace: true }
			);
			return;
		}

		if (!option || typeof option !== "object") return;

		const row = option as GlobalOptionRow;
		if (row.scoped) {
			handleScopedSelection(row.scoped);
			return;
		}

		const { entityObj, title, types } = row as HandleValuesType;
		if (!title || title === "undefined" || typeof title !== "string") return;

		const sanitizedTitle = title.trim();
		if (!sanitizedTitle) return;

		setOpen(false);
		setOptions([]);
		setInputText("");

		searchParams.set("query", sanitizedTitle);
		searchParams.set("searchType", "basic");

		if (types === "Entities" && entityObj && entityObj.guid) {
			navigate(
				{
					pathname: `/detailPage/${entityObj.guid}`
				},
				{ replace: true }
			);
		} else {
			navigate(
				{
					pathname: `/search/searchResult`,
					search: searchParams.toString()
				},
				{ replace: true }
			);
		}
	};

	const handleSubmitSearch = () => {
		const q = inputText.trim();
		if (scope !== "default") {
			const activeOption = options.find((o) => o.title === q);
			if (activeOption?.scoped) {
				handleScopedSelection(activeOption.scoped);
			}
			return;
		}
		if (!q) {
			handleValues("*");
			return;
		}
		const activeOption = options.find(
			(o) => typeof o !== "string" && o.title === q
		);
		if (activeOption) {
			handleValues(activeOption as HandleValuesType);
		} else {
			handleValues(q);
		}
	};

	const handleScopeChange = (e: SelectChangeEvent<QuickSearchScope>) => {
		const v = e.target.value as QuickSearchScope;
		setScope(v);
		setOptions([]);
		setInputText("");
		setOpen(false);
	};

	const handleClickAway = () => {
		setOpen(false);
	};

	const handleCloseModal = () => {
		setOpenAdvanceSearch(false);
	};

	const inputPlaceholder =
		scope === "default" ? "Search Entities..." : "Contains text...";

	return (
		<>
			<Stack
				direction="row"
				className="global-search-stack"
				alignItems="center"
				gap="0.5rem"
			>
				<FormControl
					size="small"
					sx={{ minWidth: 160, backgroundColor: "white", borderRadius: 1 }}
				>
					<Select<QuickSearchScope>
						id={SCOPE_SELECT_ID}
						value={scope}
						onChange={handleScopeChange}
						aria-label="Search scope"
						displayEmpty
						renderValue={(v) => SCOPE_LABELS[v as QuickSearchScope]}
					>
						<MenuItem value="default">Select All</MenuItem>
						<MenuItem value="entity">Entity</MenuItem>
						<MenuItem value="classification">Classification</MenuItem>
						<MenuItem value="glossary">Glossary / Terms</MenuItem>
						<MenuItem value="businessMetadata">Business Metadata</MenuItem>
					</Select>
				</FormControl>

				<ClickAwayListener onClickAway={handleClickAway}>
					<Autocomplete
						open={open}
						loading={loading}
						inputValue={inputText}
						onInputChange={onInputChange}
						onKeyDown={(e) => {
							const code = e.keyCode || e.which;
							switch (code) {
								case 13: {
									e.preventDefault();
									handleSubmitSearch();
									break;
								}
								case 9:
								case 27:
									setOpen(false);
									break;
								default:
									break;
							}
						}}
						freeSolo
						id="global-search"
						disablePortal
						className="global-search-autocomplete"
						sx={{
							minWidth: 280,
							flex: 1,
							"& + .MuiAutocomplete-popper .MuiAutocomplete-option": {
								backgroundColor: "white"
							},
							"& + .MuiAutocomplete-popper .MuiAutocomplete-option:hover": {
								backgroundColor: "#c7e3ff"
							}
						}}
						value={null}
						onChange={(_event: unknown, newValue: unknown) => {
							if (newValue && typeof newValue === "object") {
								const o = newValue as GlobalOptionRow;
								if (o.title) {
									handleValues(o);
								}
							}
						}}
						clearOnBlur={false}
						autoComplete
						includeInputInList
						noOptionsText={scope === "default" ? "No Entities" : "No matches"}
						getOptionLabel={(option: string | GlobalOptionRow) => {
							if (typeof option === "string") return option;
							return option?.title && typeof option.title === "string"
								? option.title
								: "";
						}}
						renderOption={(props, option, { inputValue }) => {
							const row = option as GlobalOptionRow;
							if (row.scoped) {
								const title = row.title;
								const matches = match(title, inputValue || "", {
									findAllOccurrences: true,
									insideWords: true
								});
								const parts = parse(title, matches);
								return (
									<li {...props} key={row.scoped.id}>
										<Typography component="span" variant="body2">
											{parts.map((part, index) => (
												<span
													key={index}
													style={{
														fontWeight: part.highlight ? 700 : 400
													}}
												>
													{part.text}
												</span>
											))}
										</Typography>
									</li>
								);
							}

							const { entityObj, types, parent } =
								typeof option !== "string" &&
								"entityObj" in option &&
								"types" in option &&
								"parent" in option
									? (option as {
											entityObj: { status?: string; guid?: string };
											types: string;
											parent: string;
										})
									: { entityObj: null, types: "", parent: "" };
							const title =
								typeof option !== "string" && "title" in option
									? option.title
									: option;
							const safeTitle = typeof title === "string" ? title : "";
							const guid = (entityObj as { guid?: string })?.guid;
							const safeGuid = guid && typeof guid === "string" ? guid : "";
							const href = safeGuid ? `/detailPage/${safeGuid}` : "#";
							const { name }: { name: string; found: boolean; key: any } =
								extractKeyValueFromEntity(entityObj);
							const safeName = name && typeof name === "string" ? name : "";
							const matches = match(
								types === "Entities" ? safeName : safeTitle,
								inputValue || "",
								{
									findAllOccurrences: true,
									insideWords: true
								}
							);
							const parts = parse(
								types === "Entities" ? safeName : safeTitle,
								matches
							);
							return (
								<Stack
									flexDirection="row"
									component="li"
									className="global-search-options"
									sx={{
										"& > span": {
											mr: 2,
											flexShrink: 0
										}
									}}
									{...props}
									onClick={() => {
										handleValues(option as GlobalOptionRow);
									}}
								>
									{types === "Entities" && !isEmpty(entityObj) ? (
										<Link
											className="entity-name text-decoration-none"
											style={{
												maxWidth: "100%",
												width: "100%",
												color: "black",
												textDecoration: "none",
												display: "inline-flex",
												alignItems: "center",
												flexWrap: "wrap"
											}}
											to={{ pathname: href }}
											color={
												entityObj?.status &&
												entityStateReadOnly[entityObj.status]
													? "error"
													: "primary"
											}
										>
											{types === "Entities" && !isEmpty(entityObj) && (
												<DisplayImage entity={entityObj} />
											)}
											{types === "Entities" && !isEmpty(entityObj)
												? parts.map((part, index) => (
														<Stack
															flexDirection="row"
															key={index}
															style={{
																fontWeight: part.highlight ? "bold" : "regular"
															}}
														>
															{entityObj?.guid !== "-1" && !part.highlight ? (
																<Link
																	className="entity-name text-blue text-decoration-none"
																	style={{
																		color: "black",
																		textDecoration: "none",
																		maxWidth: "100%",
																		width: "100%"
																	}}
																	to={{ pathname: href }}
																	color={
																		entityObj?.status &&
																		entityStateReadOnly[entityObj.status]
																			? "error"
																			: "primary"
																	}
																>
																	{part.text}
																</Link>
															) : (
																part.text
															)}
														</Stack>
													))
												: parts.map((part, index) => (
														<Stack
															flexDirection="row"
															key={index}
															style={{
																fontWeight: part.highlight ? "bold" : "regular"
															}}
														>
															{part.text}
														</Stack>
													))}
											{types === "Entities" &&
												!isEmpty(entityObj) &&
												` (${parent})`}
										</Link>
									) : (
										parts.map((part, index) => (
											<Typography
												component="p"
												key={index}
												className="global-search-options-text"
												sx={{
													fontWeight: part.highlight ? "bold" : "regular"
												}}
											>
												{part.text}
											</Typography>
										))
									)}
								</Stack>
							);
						}}
						renderInput={(params) => (
							<TextField
								{...params}
								placeholder={inputPlaceholder}
								fullWidth
								onClick={() => {
									setOpen(true);
								}}
								className="text-black-default"
								InputProps={{
									style: {
										padding: "1px 10px",
										borderRadius: "4px",
										color: "#1a1a1a",
										backgroundColor: "white"
									},
									...params.InputProps,
									type: "search",
									endAdornment: (
										<InputAdornment position="end">
											{loading ? (
												<CircularProgress sx={{ color: "gray" }} size={15} />
											) : (
												<SearchIcon fontSize="small" sx={{ color: "gray" }} />
											)}
										</InputAdornment>
									)
								}}
								inputProps={{
									...params.inputProps,
									"aria-label": "Global search"
								}}
							/>
						)}
						groupBy={(option) =>
							typeof option !== "string" && "types" in option
								? String((option as GlobalOptionRow).types)
								: ""
						}
						options={options}
						filterOptions={(x) => x}
					/>
				</ClickAwayListener>

				<CustomButton
					variant="contained"
					size="small"
					sx={{
						backgroundColor: "#4a90e2 !important",
						color: "#fff !important",
						textTransform: "none",
						fontWeight: 600
					}}
					onClick={handleSubmitSearch}
					aria-label="Run search"
				>
					Search
				</CustomButton>

				<CustomButton
					variant="outlined"
					size="small"
					sx={{
						backgroundColor: "white !important",
						color: "#4a90e2 !important",
						borderColor: "#dddddd !important",
						"&:hover": {
							backgroundColor: "rgba(74, 144, 226, 0.08) !important",
							color: "#4a90e2 !important"
						}
					}}
					onClick={() => {
						setOpenAdvanceSearch(true);
					}}
				>
					<Typography
						sx={{
							color: "#4a90e2 !important",
							fontWeight: "600 !important",
							fontSize: "0.875rem !important"
						}}
						display="inline"
					>
						Advanced
					</Typography>
				</CustomButton>
			</Stack>

			{openAdvanceSearch && (
				<AdvancedSearch
					openAdvanceSearch={openAdvanceSearch}
					handleCloseModal={handleCloseModal}
				/>
			)}
		</>
	);
};

export default QuickSearch;
