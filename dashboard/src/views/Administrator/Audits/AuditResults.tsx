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
  Divider,
  Grid,
  Link,
  List,
  ListItem,
  ListItemText,
  Stack,
  Typography,
} from "@mui/material";
import { auditAction, category } from "@utils/Enum";
import { isArray, isEmpty, jsonParse } from "@utils/Utils";
import CustomModal from "@components/Modal";
import { useState } from "react";
import { getValues } from "@components/commonComponents";
import { Item, StyledPaper } from "@utils/Muiutils";
import AuditsTab from "@views/DetailPage/EntityDetailTabs/AuditsTab";
import ImportExportAudits from "./ImportExportAudits";

const AuditResults = ({ componentProps, row }: any) => {
  const { auditData } = componentProps || {};
  const [openModal, setOpenModal] = useState<boolean>(false);
  const [openPurgeModal, setOpenPurgeModal] = useState<boolean>(false);
  const [currentResultObj, setCurrentObj] = useState<any>({});
  const [currentPurgeResultObj, setCurrentPurgeResultObj] = useState<any>("");
  const handleCloseModal = () => {
    setOpenModal(false);
  };
  const handleClosePurgeModal = () => {
    setOpenPurgeModal(false);
  };
  const auditObj = !isEmpty(auditData)
    ? auditData.find((obj: { guid: string }) => obj.guid == row.original.guid)
    : {};

  const { operation, params, result } = auditObj;

  const resultObj =
    operation == "PURGE"
      ? result.replace("[", "").replace("]", "").split(",")
      : jsonParse(result);

  return (
    <>
      {operation != "PURGE" &&
      operation != "IMPORT" &&
      operation != "EXPORT" &&
      !isEmpty(resultObj) ? (
        <Grid container spacing={2}>
          {params.split(",").length > 1 ? (
            <>
              {params.split(",")?.map((param: { param: string }) => {
                return (
                  <Grid item md={4}>
                    <Item
                      sx={{
                        height: "100%",
                        maxHeight: "300px",
                        overflow: "auto",
                      }}
                    >
                      <Typography
                        sx={{ padding: "1rem 0 0 1rem", textAlign: "left" }}
                      >{`${category[param as any]} ${
                        auditAction[operation]
                      }`}</Typography>

                      <List className="audit-results-list">
                        {resultObj[param as any].map(
                          (obj: { name: string }) => {
                            const { name } = obj;
                            return (
                              <>
                                <ListItem className="audit-results-list-item">
                                  <Link
                                    className="audit-results-entityid"
                                    component="button"
                                    variant="body2"
                                    onClick={() => {
                                      setOpenModal(true);
                                      setCurrentObj(obj);
                                    }}
                                  >
                                    {name}
                                  </Link>
                                </ListItem>
                              </>
                            );
                          }
                        )}
                      </List>
                    </Item>
                  </Grid>
                );
              })}
            </>
          ) : (
            <>
              <Grid item md={4}>
                <Item
                  sx={{
                    height: "100%",
                    maxHeight: "300px",
                    overflow: "auto",
                  }}
                >
                  <Typography
                    sx={{ padding: "1rem 0 0 1rem", textAlign: "left" }}
                  >{`${category[params as any]} ${
                    auditAction[operation]
                  }`}</Typography>
                  <List className="audit-results-list">
                    {resultObj[params].map((obj: { name: string }) => {
                      const { name } = obj;
                      return (
                        <>
                          <ListItem className="audit-results-list-item">
                            <Link
                              className="audit-results-entityid"
                              component="button"
                              variant="body2"
                              onClick={() => {
                                setOpenModal(true);
                                setCurrentObj(obj);
                              }}
                            >
                              {name}
                            </Link>
                          </ListItem>
                        </>
                      );
                    })}
                  </List>
                </Item>
              </Grid>
            </>
          )}
        </Grid>
      ) : (
        operation != "PURGE" &&
        operation != "IMPORT" &&
        operation != "EXPORT" && <Typography>No Results Found</Typography>
      )}

      {operation == "PURGE" && !isEmpty(resultObj) ? (
        <>
          <Typography>{`${category[operation]}`}</Typography>
          <List className="audit-results-list">
            {resultObj.map((obj: string) => {
              return (
                <ListItem className="audit-results-list-item">
                  <ListItemText
                    primary={
                      <Link
                        className="audit-results-entityid"
                        component="button"
                        variant="body2"
                        onClick={() => {
                          setOpenPurgeModal(true);
                          setCurrentPurgeResultObj(obj);
                        }}
                      >
                        {obj}
                      </Link>
                    }
                  />
                </ListItem>
              );
            })}
          </List>
        </>
      ) : (
        operation == "PURGE" && <Typography>No Results Found</Typography>
      )}

      {(operation == "IMPORT" || operation == "EXPORT") && (
        <ImportExportAudits auditObj={auditObj} />
      )}

      <CustomModal
        open={openModal}
        onClose={handleCloseModal}
        title={`${category[currentResultObj.category]} Type Details: ${
          currentResultObj.name
        }`}
        footer={false}
        button1Handler={undefined}
        button2Handler={undefined}
      >
        <StyledPaper variant="outlined">
          {" "}
          {!isEmpty(currentResultObj)
            ? Object.entries(currentResultObj)
                .sort()
                .map(([keys, value]: [string, any]) => {
                  return (
                    <>
                      <Stack
                        direction="row"
                        spacing={4}
                        marginBottom={1}
                        marginTop={1}
                      >
                        <div
                          style={{
                            flex: 1,
                            wordBreak: "break-all",
                            textAlign: "left",
                            fontWeight: "600",
                          }}
                        >
                          {`${keys} ${
                            isArray(value) ? `(${value.length})` : ""
                          }`}
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
                    </>
                  );
                })
            : "No Record Found"}
        </StyledPaper>
      </CustomModal>

      {operation == "PURGE" && (
        <CustomModal
          open={openPurgeModal}
          onClose={handleClosePurgeModal}
          title={`Purged Entity Details: ${currentPurgeResultObj}`}
          footer={false}
          button1Handler={undefined}
          button2Handler={undefined}
          maxWidth="lg"
        >
          <AuditsTab auditResultGuid={currentPurgeResultObj} />
        </CustomModal>
      )}
    </>
  );
};
export default AuditResults;
