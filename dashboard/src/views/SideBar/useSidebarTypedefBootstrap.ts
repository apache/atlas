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

import { useEffect } from "react";
import { useAppDispatch, useAppSelector } from "@hooks/reducerHook";
import { fetchEnumData } from "@redux/slice/enumSlice";
import { fetchRootClassification } from "@redux/slice/rootClassificationSlice";
import { fetchTypeHeaderData } from "@redux/slice/typeDefSlices/typeDefHeaderSlice";
import { fetchRootEntity } from "@redux/slice/allEntityTypesSlice";
import { fetchMetricEntity } from "@redux/slice/metricsSlice";
import { fetchVersionData } from "@redux/slice/sessionSlice";

const isUnset = (value: unknown): boolean =>
  value === null || value === undefined;

/**
 * Loads sidebar typedef / metrics / version data once per mount.
 * Fetch runs only when the slice field is null or undefined — not when it is
 * {} or other empty-but-loaded values, so we avoid duplicate API calls after
 * a successful empty response.
 */
export const useSidebarTypedefBootstrap = (): void => {
  const dispatch = useAppDispatch();
  const { typeHeaderData } = useAppSelector((state) => state.typeHeader || {});
  const { allEntityTypesData } = useAppSelector(
    (state) => state.allEntityTypes || {}
  );
  const { rootClassificationTypeData } = useAppSelector(
    (state) => state.rootClassification || {}
  );
  const { enumObj } = useAppSelector((state) => state.enum || {});
  const { metricsData } = useAppSelector((state) => state.metrics || {});
  const { data: versionData } = useAppSelector(
    (state) => state.session?.versionData || {}
  );

  useEffect(() => {
    if (isUnset(typeHeaderData)) {
      dispatch(fetchTypeHeaderData());
    }
    if (isUnset(allEntityTypesData)) {
      dispatch(fetchRootEntity());
    }
    if (isUnset(rootClassificationTypeData?.rootClassificationData)) {
      dispatch(fetchRootClassification());
    }
    if (isUnset(enumObj?.data)) {
      dispatch(fetchEnumData());
    }
    if (isUnset(metricsData)) {
      dispatch(fetchMetricEntity());
    }
    if (isUnset(versionData)) {
      dispatch(fetchVersionData());
    }
  }, [
    dispatch,
    typeHeaderData,
    allEntityTypesData,
    rootClassificationTypeData?.rootClassificationData,
    enumObj?.data,
    metricsData,
    versionData,
  ]);
};
