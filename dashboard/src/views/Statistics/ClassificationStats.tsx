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
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography
} from "@mui/material";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  CustomButton,
  LightTooltip
} from "@components/muiComponents";
import { useAppSelector } from "@hooks/reducerHook";
import { numberFormatWithComma } from "@utils/Helper";
import { useLocation, useNavigate } from "react-router-dom";
import { isEmpty } from "@utils/Utils";

const ClassificationStats = ({ handleClose }: any) => {
  const DATA_MAX_LENGTH = 25;
  const location = useLocation();
  const navigate = useNavigate();
  const { metricsData }: any = useAppSelector((state: any) => state.metrics);
  const { tag } = metricsData?.data || {};
  const classificationData = tag || {};
  let tagEntitiesData = classificationData
    ? classificationData.tagEntities || {}
    : {};
  let tagsCount = 0;
  let newTagEntitiesData: any = {};
  let tagEntitiesKeys = Object.keys(tagEntitiesData);
  const sortedKeys = tagEntitiesKeys.sort((a, b) =>
    a.toLocaleLowerCase().localeCompare(b.toLocaleLowerCase())
  );

  for (const key of sortedKeys) {
    const val = tagEntitiesData[key];
    newTagEntitiesData[key] = val;
    tagsCount += val;
  }

  tagEntitiesData = newTagEntitiesData;

  const handleClick = (key: string) => {
    const searchParams = new URLSearchParams(location.search);

    searchParams.set("searchType", "basic");
    searchParams.set("tag", key);

    navigate({
      pathname: "/search/searchResult",
      search: searchParams.toString()
    });
    handleClose();
  };

  return (
    <Accordion
      sx={{
        width: "100%",
        borderBottom: "1px solid rgba(0, 0, 0, 0.12) !important",
        boxShadow: "none"
      }}
      defaultExpanded={tagEntitiesData.length > DATA_MAX_LENGTH ? false : true}
    >
      <AccordionSummary
        aria-controls="technical-properties-content"
        id="technical-properties-header"
      >
        <Stack direction="row" alignItems="center" flex="1">
          <div className="properties-panel-name">
            <Typography fontWeight="600" className="text-color-green">
              {`Classifications (${numberFormatWithComma(tagsCount)})`}
            </Typography>
          </div>
        </Stack>
      </AccordionSummary>
      <AccordionDetails>
        <TableContainer component={Paper}>
          <Table className="classificationTable" size="small">
            <TableHead>
              <TableRow hover className="table-header-row">
                <TableCell>
                  <Typography fontWeight="600">Name</Typography>
                </TableCell>
                <TableCell align="right">
                  <Typography fontWeight="600">
                    {" "}
                    Count
                    <span className="count">{`(${numberFormatWithComma(
                      tagsCount
                    )})`}</span>
                  </Typography>
                </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {isEmpty(tagEntitiesData) ? (
                <TableRow className="empty text-center">
                  <TableCell colSpan={2} align="center">
                    <span>No records found!</span>
                  </TableCell>
                </TableRow>
              ) : (
                Object.entries(tagEntitiesData).map(([key, value]: any) => (
                  <TableRow key={key}>
                    <TableCell sx={{ wordBreak: "break-all" }}>{key}</TableCell>
                    <TableCell align="right">
                      <LightTooltip
                        title={`Search for entities associated with '${key}'`}
                      >
                        <CustomButton
                          size="small"
                          variant="text"
                          className="entity-name text-blue text-decoration-none"
                          onClick={() => {
                            handleClick(key);
                          }}
                          color="primary"
                        >
                          {value}
                        </CustomButton>
                      </LightTooltip>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </AccordionDetails>
    </Accordion>
  );
};

export default ClassificationStats;
