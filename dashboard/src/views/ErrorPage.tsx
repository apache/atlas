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

import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import errorIcon from "/img/error-404-icon.png";
import { Stack, Typography } from "@mui/material";
import { CustomButton } from "@components/muiComponents";

export const ErrorPage = (props: { errorCode: string }) => {
  let navigate = useNavigate();
  const [errorCode, setErrorCode] = useState<string | null>(null);
  const [errorInfo, setErrorInfo] = useState<any>(null);

  useEffect(() => {
    if (props.errorCode == "checkSSOTrue") {
      setErrorCode("Sign Out Is Not Complete!");
      setErrorInfo(
        <Typography>
          Authentication to this instance of Ranger is managed externally(for
          example,Apache Knox). You can still open this instance of Ranger from
          the same web browser without re-authentication.To prevent
          additionalPage not found (404). access to Ranger,
          <strong>close all browser windows and exit the browser</strong>.
        </Typography>
      );
    }
    if (props.errorCode == "404") {
      setErrorCode("Page not found (404).");
      setErrorInfo("Sorry, this page isn't here or has moved.");
    }
  });

  return (
    <Stack
      data-id="pageNotFoundPage"
      className="new-error-page"
      spacing={2}
      alignItems="center"
    >
      <Stack className="new-error-box" spacing={2}>
        <Stack
          className="error-white-bg"
          direction="row"
          spacing={2}
          gap="1rem"
          padding="1rem"
          alignItems="center"
        >
          <Stack className="new-icon-box" padding="0">
            <img src={errorIcon} alt="Error Icon" />
          </Stack>
          <Stack className="new-description-box" spacing={1}>
            <Typography fontSize="1.5rem" data-id="msg">
              {errorCode}
            </Typography>
            <Typography fontSize="1rem" data-id="moreInfo">
              {errorInfo}
            </Typography>
          </Stack>
        </Stack>
        <Stack direction="row" spacing={2} className="mt-2">
          {props.errorCode !== "checkSSOTrue" && (
            <CustomButton
              size="small"
              variant="contained"
              color="primary"
              onClick={() => navigate("/search")}
            >
              Return to Dashboard
            </CustomButton>
          )}
        </Stack>
      </Stack>
    </Stack>
  );
};
export default ErrorPage;
