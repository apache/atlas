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

import QuickSearch from "@components/GlobalSearch/QuickSearch";
import { CustomButton } from "@components/muiComponents";
import { useAppSelector } from "@hooks/reducerHook";
import { Stack } from "@mui/material";
import { useState } from "react";
import EntityForm from "./Entity/EntityForm";
import AddIcon from "@mui/icons-material/Add";

const DashBoard = () => {
  const { sessionObj = "" }: any = useAppSelector(
    (state: any) => state.session
  );
  const [entityModal, setEntityModal] = useState<boolean>(false);
  const { data } = sessionObj || {};
  const key = "atlas.entity.create.allowed";
  const entityCreate = data?.[key] || "";

  const handleOpenEntityModal = () => {
    setEntityModal(true);
  };
  const handleCloseEntityModal = () => {
    setEntityModal(false);
  };
  return (
    <Stack
      alignItems="flex-start"
      justifyContent="space-between"
      position="relative"
      height="100%"
      flex="1"
      padding="0"
    >
      <Stack direction="row" justifyContent="flex-end" width={"100%"}>
        {entityCreate && (
          <CustomButton
            variant="contained"
            size="small"
            onClick={(_e: any) => handleOpenEntityModal()}
            startIcon={<AddIcon />}
          >
            Create Entity
          </CustomButton>
        )}
      </Stack>
      <Stack
        justifyContent="center"
        flex="1"
        alignItems={"center"}
        height={"100%"}
        width={"100%"}
        className="dashboard-quick-search"
      >
        <QuickSearch />
      </Stack>
      {entityModal && (
        <EntityForm open={entityModal} onClose={handleCloseEntityModal} />
      )}
    </Stack>
  );
};

export default DashBoard;
