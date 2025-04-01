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

import { useState } from "react";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
} from "chart.js";
import "chartjs-plugin-zoom";
import moment from "moment";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

const StackedAreaCharts = ({ metricsGraphData }) => {
  let data = metricsGraphData;
  const keys = data.map((item) => item.key);
  const values = data.map((item) => item.values);

  const chartData = {
    labels: data[0].values.map((d) => d[0]),
    datasets: values.map((value, index) => ({
      label: keys[index],
      data: value.map((d) => d[1]),
      fill: true,
      backgroundColor:
        index === 0
          ? "rgba(255, 99, 132, 0.2)"
          : index === 1
          ? "rgba(54, 162, 235, 0.2)"
          : "rgba(75, 192, 192, 0.2)",
      borderColor:
        index === 0
          ? "rgba(255, 99, 132, 1)"
          : index === 1
          ? "rgba(54, 162, 235, 1)"
          : "rgba(75, 192, 192, 1)",
      borderWidth: 1,
      tension: 0.1,
      stack: "stacked"
    }))
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: "top"
      },
      tooltip: {
        callbacks: {
          label: (tooltipItem) => {
            return `${tooltipItem.dataset.label}: ${tooltipItem.raw}`;
          }
        }
      }
    },
    scales: {
      x: {
        type: "linear",
        position: "bottom",
        ticks: {
          callback: function (value) {
            return moment(value).format("MM/DD/YYYY");
          }
        },
        stacked: "stacked"
      },
      y: {
        min: -1.0,
        max: 1.0,
        ticks: {
          stepSize: 0.5,
          callback: function (value) {
            if (value === -1) return "-1";
            if (value === -0.5) return "-0.50";
            if (value === 0) return "0";
            if (value === 0.5) return "0.50";
            if (value === 1) return "1";
            return "";
          }
        },
        stacked: "stacked"
      }
    }
  };

  const handleModeChange = (mode) => {
    setChartMode(mode);
  };

  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center"
      }}
    >
      <div style={{ height: "500px", width: "75%" }}>
        <Line data={chartData} options={chartOptions} />
      </div>
    </div>
  );
};

export default StackedAreaCharts;
