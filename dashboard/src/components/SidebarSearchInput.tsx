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

import React, { ChangeEvent } from "react";
import { Paper, InputBase, Stack } from "@mui/material";
import ClearIcon from "@mui/icons-material/Clear";
import { IconButton } from "@components/muiComponents";

interface SidebarSearchInputProps {
  searchTerm: string;
  onChange: (value: string) => void;
  dataCy?: string;
}

export const SidebarSearchInput: React.FC<SidebarSearchInputProps> = ({
  searchTerm,
  onChange,
  dataCy
}) => (
  <Paper
    sx={{
      width: "100%",
      paddingLeft: "8px",
      display: "flex",
      alignItems: "center"
    }}
    className="sidebar-searchbar"
  >
    <InputBase
      fullWidth
      sx={{ color: "rgba(0, 0, 0, 0.7)" }}
      placeholder="Search"
      inputProps={{ "aria-label": "search" }}
      value={searchTerm}
      onChange={(e: ChangeEvent<HTMLInputElement>) => onChange(e.target.value)}
      data-cy={dataCy}
      endAdornment={
        <Stack direction="row" alignItems="center" gap="4px">
          {searchTerm.length > 0 && (
            <IconButton
              size="small"
              onClick={() => onChange("")}
              edge="end"
              sx={{ padding: "4px" }}
            >
              <ClearIcon fontSize="small" sx={{ color: "rgba(0, 0, 0, 0.4)" }} />
            </IconButton>
          )}
          <img
            src="/img/sidebar-icons/icon-search.svg"
            style={{
              width: "16px",
              height: "16px",
              filter: "brightness(0.4)",
              opacity: 1,
              cursor: "pointer",
              marginLeft: "4px"
            }}
            alt="Search"
          />
        </Stack>
      }
    />
  </Paper>
);
