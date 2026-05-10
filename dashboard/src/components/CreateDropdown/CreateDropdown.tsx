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

import { useState, useCallback } from "react";
import { Button, Menu, MenuItem } from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import CategoryIcon from "@mui/icons-material/Category";
import LocalOfferIcon from "@mui/icons-material/LocalOffer";
import MenuBookIcon from "@mui/icons-material/MenuBook";
import BusinessCenterIcon from "@mui/icons-material/BusinessCenter";
import ListIcon from "@mui/icons-material/List";
import { useNavigate } from "react-router-dom";
import EntityForm from "@views/Entity/EntityForm";
import ClassificationForm from "@views/Classification/ClassificationForm";
import AddUpdateGlossaryForm from "@views/Glossary/AddUpdateGlossaryForm";
const CreateDropdown = () => {
	const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
	const [entityModal, setEntityModal] = useState(false);
	const [classificationModal, setClassificationModal] = useState(false);
	const [glossaryModal, setGlossaryModal] = useState(false);
	const navigate = useNavigate();

	const open = Boolean(anchorEl);

	const handleClick = useCallback((event: React.MouseEvent<HTMLElement>) => {
		setAnchorEl(event.currentTarget);
	}, []);

	const handleClose = useCallback(() => {
		setAnchorEl(null);
	}, []);

	const handleEntityClick = useCallback(() => {
		handleClose();
		setEntityModal(true);
	}, [handleClose]);

	const handleClassificationClick = useCallback(() => {
		handleClose();
		setClassificationModal(true);
	}, [handleClose]);

	const handleGlossaryClick = useCallback(() => {
		handleClose();
		setGlossaryModal(true);
	}, [handleClose]);

	const handleBusinessMetadataClick = useCallback(() => {
		handleClose();
		navigate({
			pathname: "/administrator",
			search: "tabActive=businessMetadata&create=true"
		});
	}, [handleClose, navigate]);

	const handleEnumClick = useCallback(() => {
		handleClose();
		navigate({
			pathname: "/administrator",
			search: "tabActive=enum"
		});
	}, [handleClose, navigate]);

	return (
		<>
			<Button
				variant="contained"
				color="primary"
				size="small"
				onClick={handleClick}
				endIcon={<ExpandMoreIcon />}
				startIcon={<AddIcon />}
				aria-controls={open ? "create-menu" : undefined}
				aria-haspopup="true"
				aria-expanded={open ? "true" : undefined}
				data-cy="create-dropdown"
			>
				Create
			</Button>
			<Menu
				id="create-menu"
				anchorEl={anchorEl}
				open={open}
				onClose={handleClose}
				anchorOrigin={{ horizontal: "left", vertical: "bottom" }}
				transformOrigin={{ horizontal: "left", vertical: "top" }}
				slotProps={{
					paper: {
						elevation: 2,
						sx: { mt: 1.5, minWidth: 200 }
					}
				}}
			>
				<MenuItem
					onClick={handleEntityClick}
					data-cy="create-entity"
					sx={{ gap: 1.5 }}
				>
					<CategoryIcon fontSize="small" />
					Entity
				</MenuItem>
				<MenuItem
					onClick={handleClassificationClick}
					data-cy="create-classification"
					sx={{ gap: 1.5 }}
				>
					<LocalOfferIcon fontSize="small" />
					Classification
				</MenuItem>
				<MenuItem
					onClick={handleGlossaryClick}
					data-cy="create-glossary"
					sx={{ gap: 1.5 }}
				>
					<MenuBookIcon fontSize="small" />
					Glossary
				</MenuItem>
				<MenuItem
					onClick={handleBusinessMetadataClick}
					data-cy="create-business-metadata"
					sx={{ gap: 1.5 }}
				>
					<BusinessCenterIcon fontSize="small" />
					Business Metadata
				</MenuItem>
				<MenuItem
					onClick={handleEnumClick}
					data-cy="create-enum"
					sx={{ gap: 1.5 }}
				>
					<ListIcon fontSize="small" />
					Enum
				</MenuItem>
			</Menu>

			{entityModal && (
				<EntityForm
					open={entityModal}
					onClose={() => setEntityModal(false)}
				/>
			)}
			{classificationModal && (
				<ClassificationForm
					open={classificationModal}
					isAdd={true}
					onClose={() => setClassificationModal(false)}
				/>
			)}
			{glossaryModal && (
				<AddUpdateGlossaryForm
					open={glossaryModal}
					isAdd={true}
					onClose={() => setGlossaryModal(false)}
					node={undefined}
				/>
			)}
		</>
	);
};

export default CreateDropdown;
