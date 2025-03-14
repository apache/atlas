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
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Typography
} from "@components/muiComponents";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";
import QueryBuilder, { defaultValidator } from "react-querybuilder";
import { useLocation } from "react-router-dom";
import { TypeCustomValueEditor } from "./TypeCustomValueEditor";

const TypeFilters = ({ fieldsObj, typeQuery, setTypeQuery }: any) => {
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const typeParams = searchParams.get("type");

  return (
    <Accordion
      defaultExpanded
      sx={{ borderBottom: "1px solid rgba(0, 0, 0, 0.12) !important" }}
    >
      <AccordionSummary aria-controls="panel1-content" id="panel1-header">
        <Typography
          className="text-color-green"
          fontSize="16px"
          fontWeight="600"
        >
          Type: {typeParams}
        </Typography>
      </AccordionSummary>
      <AccordionDetails>
        <QueryBuilder
          fields={fieldsObj}
          query={typeQuery}
          controlClassnames={{ queryBuilder: "queryBuilder-branches" }}
          onQueryChange={setTypeQuery}
          controlElements={{ valueEditor: TypeCustomValueEditor }}
          validator={defaultValidator}
          translations={{
            addGroup: {
              label: (
                <>
                  <AddOutlinedIcon fontSize="small" /> Add filter group
                </>
              )
            },
            addRule: {
              label: (
                <>
                  <AddOutlinedIcon fontSize="small" /> Add filter
                </>
              )
            }
          }}
        />
      </AccordionDetails>
    </Accordion>
  );
};

export default TypeFilters;
