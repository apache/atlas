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

import { getBaseApiUrl } from "./commonApiUrl";

const metricsApiUrl = () => {
  return `${getBaseApiUrl("url")}/admin/metrics`;
};

const metricsAllCollectionTimeApiUrl = () => {
  return `${getBaseApiUrl("url")}/admin/metricsstats`;
};
const metricsCollectionTimeApiUrl = () => {
  return `${getBaseApiUrl("url")}/admin/metricsstat`;
};

const metricsGraphUrl = () => {
  return `${getBaseApiUrl("url")}/admin/metricsstats/charts`;
};

const debugMetricsUrl = () => {
  return `${getBaseApiUrl("url")}/admin/debug/metrics`;
};

export {
  metricsApiUrl,
  metricsAllCollectionTimeApiUrl,
  metricsCollectionTimeApiUrl,
  metricsGraphUrl,
  debugMetricsUrl
};
