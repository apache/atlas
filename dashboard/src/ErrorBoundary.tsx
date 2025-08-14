/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Component } from "react";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import errorIcon from "/img/error-404-icon.png";
import { CustomButton } from "@components/muiComponents";

interface ErrorBoundaryState {
  error: Error | null;
  errorInfo: any | null;
}

class ErrorBoundary extends Component<any, ErrorBoundaryState> {
  constructor(props: any) {
    super(props);
    this.state = {
      error: null,
      errorInfo: null
    };
  }

  refresh = () => window.location.reload();

  handleNavigation = () => {
    this.props.history.push("/search");
  };

  componentDidCatch(error: any, errorInfo: any) {
    this.setState({
      error: error,
      errorInfo: errorInfo
    });
  }

  render() {
    return (
      <>
        {this.state.errorInfo ? (
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
                  <Typography fontSize="1rem" data-id="moreInfo">
                    Oops! Something went wrong...
                  </Typography>
                </Stack>
              </Stack>
              <Stack direction="row" spacing={2} className="mt-2">
                {this.state.error?.name &&
                  this.state.error.name !== "ChunkLoadError" && (
                    <CustomButton
                      variant="contained"
                      color="primary"
                      size="small"
                      onClick={() => {
                        this.setState({
                          error: null,
                          errorInfo: null
                        });
                        this.handleNavigation();
                      }}
                      style={{ marginRight: "8px" }}
                    >
                      <i className="fa-fw fa fa-long-arrow-left"></i> Return to
                      Dashboard
                    </CustomButton>
                  )}
              </Stack>
            </Stack>
          </Stack>
        ) : (
          this.props.children
        )}
      </>
    );
  }
}

export default ErrorBoundary;
