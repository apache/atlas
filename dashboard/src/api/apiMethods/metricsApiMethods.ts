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
  debugMetricsUrl,
  metricsAllCollectionTimeApiUrl,
  metricsApiUrl,
  metricsCollectionTimeApiUrl,
  metricsGraphUrl
} from "../apiUrlLinks/metricsApiUrl";
import { _get } from "./apiMethod";

interface configs {
  method: string;
  params: object;
}

const getMetricsEntity = (params: object) => {
  const config: configs = {
    method: "GET",
    params: params
  };
  return _get(metricsApiUrl(), config);
};

const getMetricsStats = (dateValue: string) => {
  const config: configs = {
    method: "GET",
    params: {}
  };
  return _get(
    dateValue == "Current"
      ? metricsAllCollectionTimeApiUrl()
      : `${metricsCollectionTimeApiUrl()}/${dateValue}`,
    config
  );
};

const getMetricsGraph = (params: object) => {
  const config: configs = {
    method: "GET",
    params: params
  };
  return _get(metricsGraphUrl(), config);
};

const getDebugMetrics = () => {
  const config: configs = {
    method: "GET",
    params: {}
  };
  return _get(debugMetricsUrl(), config);
};

export { getMetricsEntity, getMetricsStats, getMetricsGraph, getDebugMetrics };
