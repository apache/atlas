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

import { useEffect, useRef, useState } from "react";
import CustomModal from "@components/Modal";
import EntityStats from "./EntityStats";
import { getMetricsStats } from "@api/apiMethods/metricsApiMethods";
import ClassificationStats from "./ClassificationStats";
import {
  Autocomplete,
  Badge,
  CircularProgress,
  IconButton,
  Stack,
  TextField
} from "@mui/material";
import ServerStats from "./ServerStats";
import SystemDetails from "./SystemDetails";
import { formatedDate, millisecondsToTime, serverError } from "@utils/Utils";
import { numberFormatWithComma } from "@utils/Helper";
import { Refresh } from "@mui/icons-material";
import { LightTooltip } from "@components/muiComponents";
import { useAppDispatch } from "@hooks/reducerHook";
import { fetchMetricEntity } from "@redux/slice/metricsSlice";
import { toast } from "react-toastify";

export const getStatsValue = (options: { value: any; type: any }) => {
  let value = options.value;
  let type = options.type;
  if (type == "time") {
    return millisecondsToTime(value);
  } else if (type == "day") {
    return formatedDate({ date: value });
  } else if (type == "number") {
    return numberFormatWithComma(value);
  } else if (type == "millisecond") {
    return numberFormatWithComma(value) + " millisecond/s";
  } else if (type == "status-html") {
    return <Badge color="success" variant="dot"></Badge>;
  } else {
    return value;
  }
};

const Statistics = ({
  open,
  handleClose
}: {
  open: boolean;
  handleClose: () => void;
}) => {
  const dispatch = useAppDispatch();
  const toastId = useRef<any>(null);
  const [metricsStatsData, setMetricsStatsData] = useState([]);
  const [currentMetricsData, seCurrentMetricsData] = useState([]);
  const [selectedValue, setSelectedValue] = useState({
    label: "Current",
    value: "Current"
  });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchMetricsStatsDetails();
  }, []);

  const fetchMetricsStatsDetails = async () => {
    try {
      setLoading(true);
      const { label } = selectedValue;
      const metricsStatsResp = await getMetricsStats(label);
      const { data } = metricsStatsResp;
      setMetricsStatsData(data);
      setLoading(false);
    } catch (error) {
      console.error(`Error occur while fetching metrics  details`, error);
      serverError(error, toastId);
      setLoading(false);
    }
  };

  let metricStatsListOptions = metricsStatsData?.map(
    (obj: { collectionTime: string }) => {
      let collectionTime = getStatsValue({
        type: "day",
        value: obj.collectionTime
      });
      return { label: collectionTime, value: obj.collectionTime };
    }
  );

  const fetchCurrentMetricsDetails = async (newValue: any) => {
    try {
      setLoading(true);
      const metricsResp = await getMetricsStats(newValue);
      const { data } = metricsResp;
      seCurrentMetricsData(data);
      setLoading(false);
    } catch (error) {
      console.error(`Error occur while fetching metrics stats details`, error);
      serverError(error, toastId);
      setLoading(false);
    }
  };

  const handleRefresh = async () => {
    await fetchMetricsStatsDetails();
    await dispatch(fetchMetricEntity());
    if (toastId.current) {
      toast.dismiss(toastId.current);
    }
    toastId.current = toast.success("Metric data is refreshed");
  };

  return (
    <>
      <CustomModal
        open={open}
        onClose={handleClose}
        title={`Statistics`}
        button2Label="Cancel"
        button2Handler={handleClose}
        maxWidth="md"
        postTitleIcon={
          <Stack justifyContent="center" alignItems="center">
            <LightTooltip title="Refresh Data">
              <IconButton
                size="small"
                onClick={() => {
                  handleRefresh();
                }}
              >
                <Refresh />
              </IconButton>
            </LightTooltip>
          </Stack>
        }
        button1Handler={undefined}
      >
        <Stack
          spacing={4}
          sx={{ width: "100%", minHeight: "100px", alignItems: "flex-end" }}
        >
          <Stack spacing={2} sx={{ alignSelf: "flex-end" }}>
            <Autocomplete
              value={selectedValue}
              onChange={(_event: any, newValue: any) => {
                const { value } = newValue;
                setSelectedValue(newValue);
                if (value != "Current") {
                  fetchCurrentMetricsDetails(value);
                }
              }}
              options={[
                { label: "Current", value: "Current" },
                ...metricStatsListOptions
              ]}
              getOptionLabel={(option) => option.label}
              disableClearable
              clearOnEscape={false}
              size="small"
              renderInput={(params) => (
                <TextField
                  {...params}
                  variant="outlined"
                  size="small"
                  fullWidth
                />
              )}
              defaultValue={"Current"}
              sx={{
                minWidth: "250px",
                backgroundColor: "#f6f7fb"
              }}
            />
          </Stack>

          {loading ? (
            <CircularProgress
              disableShrink
              sx={{
                display: "inline-block",
                position: "absolute",
                left: "50%",
                marginLeft: "-20px !important"
              }}
            />
          ) : (
            <Stack spacing={2} width="100%">
              <EntityStats
                selectedValue={selectedValue}
                handleClose={handleClose}
                currentMetricsData={currentMetricsData}
                setLoading={setLoading}
              />
              <ClassificationStats handleClose={handleClose} />
              <ServerStats
                selectedValue={selectedValue}
                currentMetricsData={currentMetricsData}
              />
              <SystemDetails />
            </Stack>
          )}
        </Stack>
      </CustomModal>
    </>
  );
};

export default Statistics;
