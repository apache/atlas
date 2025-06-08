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

import { FormControl, IconButton, NativeSelect, Stack } from "@mui/material";
import { getYear, getMonth } from "date-fns";
import ArrowBackIosIcon from "@mui/icons-material/ArrowBackIos";
import ArrowForwardIosIcon from "@mui/icons-material/ArrowForwardIos";

const years = Array.from(
  { length: 100 },
  (_, i) => new Date().getFullYear() - 50 + i
);
const months = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December"
];

type CustomHeaderProps = {
  date: Date;
  changeYear: (year: number) => void;
  changeMonth: (month: number) => void;
  decreaseMonth: () => void;
  increaseMonth: () => void;
  prevMonthButtonDisabled: boolean;
  nextMonthButtonDisabled: boolean;
};

const CustomHeader = ({
  date,
  changeYear,
  changeMonth,
  decreaseMonth,
  increaseMonth,
  prevMonthButtonDisabled,
  nextMonthButtonDisabled
}: CustomHeaderProps) => {
  return (
    <Stack
      direction="row"
      spacing={1}
      alignItems="center"
      justifyContent="center"
      sx={{ margin: 1 }}
    >
      <IconButton
        size="small"
        onClick={decreaseMonth}
        disabled={prevMonthButtonDisabled}
      >
        <ArrowBackIosIcon fontSize="inherit" />
      </IconButton>
      <Stack minWidth={"60px"}>
        <FormControl fullWidth>
          <NativeSelect
            value={getYear(date)}
            onChange={({ target: { value } }) => changeYear(parseInt(value))}
          >
            {" "}
            {years.map((year) => (
              <option className="text-center" key={year} value={year}>
                {year}
              </option>
            ))}
          </NativeSelect>
        </FormControl>
      </Stack>

      <FormControl fullWidth>
        <NativeSelect
          value={months[getMonth(date)]}
          onChange={({ target: { value } }) =>
            changeMonth(months.indexOf(value))
          }
        >
          {months.map((month) => (
            <option className="text-center" key={month} value={month}>
              {month}
            </option>
          ))}
        </NativeSelect>
      </FormControl>

      <IconButton
        size="small"
        onClick={increaseMonth}
        disabled={nextMonthButtonDisabled}
      >
        <ArrowForwardIosIcon fontSize="inherit" />
      </IconButton>
    </Stack>
  );
};

export default CustomHeader;
