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

/** Keep 403 toasts readable (Atlas authorization message). */
const FORBIDDEN_ERROR_TOAST_MS = 5_000;
/**
 * Callers often `toast.dismiss(ref.current)` in catch when `ref.current` is null;
 * react-toastify then clears all toasts and removes the 403 toast we just showed.
 * Queue the toast as a macrotask so it runs after that dismiss.
 */
const FETCH_API_FORBIDDEN_TOAST_ID = "fetch-api-http-403";

const showForbiddenToastLater = (
  responseData: unknown,
  defaultMessage: string
) => {
  setTimeout(() => {
    let message = defaultMessage;
    if (responseData && typeof responseData === "object") {
      const d = responseData as {
        errorMessage?: unknown;
        message?: unknown;
        error?: unknown;
      };
      message =
        (d.errorMessage as string | undefined) ||
        (d.message as string | undefined) ||
        (d.error as string | undefined) ||
        message;
    }
    toast.error(message, {
      toastId: FETCH_API_FORBIDDEN_TOAST_ID,
      autoClose: FORBIDDEN_ERROR_TOAST_MS
    });
  }, 0);
};

let prevNetworkErrorTime = 0;

function errorHandelingForAbortAndStatus0() {
  const diffTime = new Date().getTime() - prevNetworkErrorTime;
  if (diffTime > 3000) {
    prevNetworkErrorTime = new Date().getTime();
    toast.error(
      "Network Connection Failure : " +
        "It seems you are not connected to the internet. Please check your internet connection and try again"
    );
  }
}

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
    if (axios.isAxiosError(error)) {
      if(error.response?.status){
        switch (Number(error.response.status)) {
          case 0:
            errorHandelingForAbortAndStatus0();
            break;
          case 401:
            window.location.replace("login.jsp");
            break;
          case 403:
            // Match classic UI: toast only, no redirect. Defer toast so callers
            // that dismiss all toasts in catch (e.g. toast.dismiss(null ref)) do
            // not remove this notification before it is shown — see
            // showForbiddenToastLater.
            showForbiddenToastLater(
              error.response?.data,
              "You are not authorized"
            );
            break;
          case 404:
            serverErrorHandler(
              { responseJSON: error.response?.data },
              "Resource not found"
            );
            break;
          case 419:
              toast.warning("Session Time Out !!");
            window.location.replace("login.jsp");
            break;
          case 500:
            serverErrorHandler(
              { responseJSON: error.response?.data },
              "Internal Server Error"
            );
            break;
          case 503:
            serverErrorHandler(
              { responseJSON: error.response?.data },
              "Service Unavailable"
            );
            break;
          case 504:
            serverErrorHandler(
              { responseJSON: error.response?.data },
              "Gateway Timeout"
            );
            break;
          default:
            break;
        }
      }
      // Only treat as offline / connection failure when there is no HTTP
      // response (or status 0). Do not run this for 403/404/5xx — those are
      // handled above and would wrongly show a network toast.
      const res = error.response;
      const isAbort =
        error.code === "ERR_CANCELED" || res?.statusText === "abort";
      if (!isAbort && (!res || Number(res.status) === 0)) {
        errorHandelingForAbortAndStatus0();
      }
    }
    throw error;
  }
};

export { fetchApi };
