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

import axios, { AxiosRequestConfig, AxiosResponse } from "axios";
import { globalSessionData } from "../../utils/Enum";
import { toast } from "react-toastify";
import { serverErrorHandler } from "@utils/Utils";

let isSessionActive = true;
let prevNetworkErrorTime = 0;

const fetchApi = async (url: string, config: AxiosRequestConfig) => {
  const configs: AxiosRequestConfig = {
    url: url,
    method: config.method,
    params: config.params,
    data: config.data,
    onUploadProgress: config.onUploadProgress,
    headers: {
      [globalSessionData.restCrsfHeader]: globalSessionData.crsfToken
        ? [globalSessionData.crsfToken]
        : '""'
    }
  };

  try {
    const resp: AxiosResponse = await axios(configs as AxiosRequestConfig);
    return resp;
  } catch (error: any) {
    if (axios.isAxiosError(error) && error.response?.status === 419) {
      if (isSessionActive) {
        toast.warning("Session Time Out !!");
        isSessionActive = false;
        window.location.replace("login.jsp");
      }
      if (axios.isAxiosError(error) && error.response?.status) {
        if (Number(error.response.status) === 401) {
          window.location.replace("login.jsp");
        } else if (Number(error.response.status) == 419) {
          window.location.replace("login.jsp");
        } else if (Number(error.response.status) == 403) {
          serverErrorHandler(
            { responseJSON: error.response?.data },
            "You are not authorized"
          );
        } else if (
          Number(error.response.status) === 0 &&
          error.response?.statusText != "abort"
        ) {
          var diffTime = new Date().getTime() - prevNetworkErrorTime;
          if (diffTime > 3000) {
            prevNetworkErrorTime = new Date().getTime();
            toast.error(
              "Network Connection Failure : " +
                "It seems you are not connected to the internet. Please check your internet connection and try again"
            );
          }
        }
      }
    }
    throw error;
  }
};

export { fetchApi };
