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
import errorIcon from "/img/error-404-icon.png";
import { Stack, Typography } from "@mui/material";
import { CustomButton } from "@components/muiComponents";
import { useLocation } from "react-router-dom";
import { useNavigate } from "react-router-dom";


export const ErrorPage = (props: { errorCode: string }) => {
  let navigate = useNavigate();
  const location = useLocation();
  const code = location.state?.errorCode || props.errorCode || "404";

  const [errorCode, setErrorCode] = useState<string>("");
  const [errorInfo, setErrorInfo] = useState<any>(null);

  useEffect(() => {
    if (code == "checkSSOTrue") {
      setErrorCode("Sign Out Is Not Complete!");
      setErrorInfo(
        <Typography>
          Authentication to this instance of Atlas is managed externally(for
          example,Apache Knox). You can still open this instance of Atlas from
          the same web browser without re-authentication.To prevent
          additionalPage not found (404). access to Atlas,
          <strong>close all browser windows and exit the browser</strong>.
        </Typography>
      );
    }
    else if (code == "404") {
      setErrorCode("Page not found (404).");
      setErrorInfo("Sorry, this page isn't here or has moved.");
    }
  });

  const handleBackWithReload = () => {
    localStorage.setItem("doGoBackAfterReload", "true");
    window.location.reload();
  };

  useEffect(() => {
    if (localStorage.getItem("doGoBackAfterReload") === "true") {
      localStorage.removeItem("doGoBackAfterReload");
      setTimeout(() => {
        window.history.back();
      }, 1000); 
    }
  }, []);

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
          {code !== "checkSSOTrue" && (
            <CustomButton
              size="small"
              variant="contained"
              color="primary"
              onClick={() => navigate("/search")}
            >
              Return to Dashboard
            </CustomButton>
          )}
          {code == "checkSSOTrue" && (
            <CustomButton
              size="small"
              variant="contained"
              color="primary"
              onClick={handleBackWithReload}
            >
              Go Back
            </CustomButton>
          )}
        </Stack>
      </Stack>
    </Stack>
  );
};
export default ErrorPage;
