// @ts-nocheck

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

import { SetStateAction, useState } from "react";
import LineageHelper from "./atlas-lineage/src";
import { useSelector } from "react-redux";
import { EntityState } from "@models/relationshipSearchType";
import { isEmpty } from "@utils/Utils";
import {
  Checkbox,
  FormControl,
  FormControlLabel,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  Typography
} from "@mui/material";
import { Close as CloseIcon } from "@mui/icons-material";
import { lineageDepth } from "@utils/Enum";

const LineageLayout = ({
  lineageData,
  lineageDivRef,
  legendRef,
  entity
}: any) => {
  const [filters, setFilters] = useState<any>({
    hideProcess: false,
    hideDeletedEntity: false,
    showOnlyHoverPath: true,
    showTooltip: false,
    labelFullName: false
  });

  const { entityData } = useSelector((state: EntityState) => state.entity);
  const typedef: any = useSelector((state: any) => state.classification);
  const { classificationData }: any = typedef;
  let classificationNamesArray: any = [];
  if (!isEmpty(classificationData)) {
    classificationData.classificationDefs.forEach(function (item: {
      typeName: any;
    }) {
      classificationNamesArray.push(item.typeName);
    });
  }
  let currentEntityData = {
    classificationNames: classificationNamesArray,
    displayText: entity.attributes.name,
    labels: [],
    meaningNames: [],
    meanings: []
  };

  const filterObj = {
    isProcessHideCheck: false,
    isDeletedEntityHideCheck: false,
    depthCount: lineageDepth
  };

  const { isProcessHideCheck, isDeletedEntityHideCheck } = filterObj;

  const myClassInstance = new LineageHelper({
    entityDefCollection: entityData.entityDefs,
    data: lineageData,
    el: lineageDivRef.current,
    legendsEl: legendRef.current,
    legends: lineageData.legends,
    getFilterObj: function () {
      return {
        isProcessHideCheck: isProcessHideCheck,
        isDeletedEntityHideCheck: isDeletedEntityHideCheck
      };
    },
    isShowHoverPath: function () {
      setFilters({ showOnlyHoverPath: true });
    },
    isShowTooltip: function () {
      setFilters({ showTooltip: true });
    },
    onPathClick: function (d: { pathRelationObj: { relationshipId: any } }) {
      if (d.pathRelationObj) {
        var relationshipId = d.pathRelationObj.relationshipId;
      }
    }
  });

  const [depth, setDepth] = useState("");
  const [searchType, setSearchType] = useState("");
  const [nodeCount, setNodeCount] = useState("");

  const handleCheckboxChange = (event: {
    target: { name: any; checked: any };
  }) => {
    const { name, checked } = event.target;
    setFilters((prev: any) => ({ ...prev, [name]: checked }));
  };

  const handleDepthChange = (event: {
    target: { value: SetStateAction<string> };
  }) => setDepth(event.target.value);
  const handleSearchTypeChange = (event: {
    target: { value: SetStateAction<string> };
  }) => setSearchType(event.target.value);
  const handleNodeCountChange = (event: {
    target: { value: SetStateAction<string> };
  }) => setNodeCount(event.target.value);

  const resetLineage = () => {
    console.log("Reset lineage");
  };

  const saveAsPNG = () => {
    console.log("Save as PNG");
  };
  return (
    <>
      <Stack sx={{ backgroundColor: "white", padding: 2, borderRadius: 2 }}>
        <Typography
          variant="h6"
          sx={{ display: "flex", justifyContent: "space-between" }}
        >
          Filters
          <IconButton size="small">
            <CloseIcon />
          </IconButton>
        </Typography>
        <Stack sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <FormControlLabel
            control={
              <Checkbox
                checked={filters.hideProcess}
                onChange={handleCheckboxChange}
                name="hideProcess"
              />
            }
            label="Hide Process"
          />
          <FormControlLabel
            control={
              <Checkbox
                checked={filters.hideDeletedEntity}
                onChange={handleCheckboxChange}
                name="hideDeletedEntity"
              />
            }
            label="Hide Deleted Entity"
          />
          <FormControl fullWidth>
            <InputLabel>Depth</InputLabel>
            <Select value={depth} onChange={handleDepthChange} label="Depth">
              <MenuItem value={1}>Depth 1</MenuItem>
              <MenuItem value={2}>Depth 2</MenuItem>
              <MenuItem value={3}>Depth 3</MenuItem>
            </Select>
          </FormControl>
        </Stack>
      </Stack>

      <Stack sx={{ backgroundColor: "white", padding: 2, borderRadius: 2 }}>
        <Typography
          variant="h6"
          sx={{ display: "flex", justifyContent: "space-between" }}
        >
          Search
          <IconButton size="small">
            <CloseIcon />
          </IconButton>
        </Typography>
        <Stack sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <Typography variant="body2">Search Lineage Entity</Typography>
          <FormControl fullWidth>
            <Select value={searchType} onChange={handleSearchTypeChange}>
              <MenuItem value="entity1">Entity 1</MenuItem>
              <MenuItem value="entity2">Entity 2</MenuItem>
              <MenuItem value="entity3">Entity 3</MenuItem>
            </Select>
          </FormControl>
        </Stack>
      </Stack>

      <Stack sx={{ backgroundColor: "white", padding: 2, borderRadius: 2 }}>
        <Typography
          variant="h6"
          sx={{ display: "flex", justifyContent: "space-between" }}
        >
          Settings
          <IconButton size="small">
            <CloseIcon />
          </IconButton>
        </Typography>
        <Stack sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <FormControlLabel
            control={
              <Checkbox
                checked={filters.showOnlyHoverPath}
                onChange={handleCheckboxChange}
                name="showOnlyHoverPath"
              />
            }
            label="On hover show current path"
          />
          <FormControlLabel
            control={
              <Checkbox
                checked={filters.showTooltip}
                onChange={handleCheckboxChange}
                name="showTooltip"
              />
            }
            label="Show node details on hover"
          />
          <FormControlLabel
            control={
              <Checkbox
                checked={filters.labelFullName}
                onChange={handleCheckboxChange}
                name="labelFullName"
              />
            }
            label="Display full name"
          />
        </Stack>
      </Stack>
    </>
  );
};

export default LineageLayout;
