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

import React from "react";
import { render, screen, fireEvent, within } from "@utils/test-utils";
import CustomHeader from "@components/DatePicker/CustomHeader";

// Mock date-fns to avoid transforming node_modules with optional chaining
jest.mock("date-fns", () => ({
  getYear: (d: Date) => d.getFullYear(),
  getMonth: (d: Date) => d.getMonth()
}));

describe("CustomHeader", () => {
  it("renders controls and triggers navigation handlers", () => {
    const date = new Date(2024, 4, 15); // May 15, 2024
    const changeYear = jest.fn();
    const changeMonth = jest.fn();
    const decreaseMonth = jest.fn();
    const increaseMonth = jest.fn();

    const { container } = render(
      <CustomHeader
        date={date}
        changeYear={changeYear}
        changeMonth={changeMonth}
        decreaseMonth={decreaseMonth}
        increaseMonth={increaseMonth}
        prevMonthButtonDisabled={false}
        nextMonthButtonDisabled={true}
      />
    );

    const buttons = container.querySelectorAll("button");
    expect(buttons.length).toBe(2);
    expect(buttons[0].hasAttribute('disabled')).toBe(false);
    expect(buttons[1].hasAttribute('disabled')).toBe(true);

    fireEvent.click(buttons[0]);
    expect(decreaseMonth).toHaveBeenCalledTimes(1);

    // Clicking disabled button should not invoke handler
    fireEvent.click(buttons[1]);
    expect(increaseMonth).not.toHaveBeenCalled();

    const selects = screen.getAllByRole("combobox");
    expect(selects.length).toBe(2);

    // Year select
    const yearSelect = selects[0];
    const yearOptions = within(yearSelect).getAllByRole("option");
    expect(yearOptions.length).toBe(100);
    fireEvent.change(yearSelect, { target: { value: String(2020) } });
    expect(changeYear).toHaveBeenCalledWith(2020);

    // Month select
    const monthSelect = selects[1];
    const monthOptions = within(monthSelect).getAllByRole("option");
    expect(monthOptions.length).toBe(12);
    fireEvent.change(monthSelect, { target: { value: "March" } });
    expect(changeMonth).toHaveBeenCalledWith(2); // March index
  });

  it("enables next-month button and triggers increase when allowed", () => {
    const date = new Date(2024, 7, 10);
    const increaseMonth = jest.fn();
    const decreaseMonth = jest.fn();

    const { container } = render(
      <CustomHeader
        date={date}
        changeYear={jest.fn()}
        changeMonth={jest.fn()}
        decreaseMonth={decreaseMonth}
        increaseMonth={increaseMonth}
        prevMonthButtonDisabled={false}
        nextMonthButtonDisabled={false}
      />
    );

    const buttons = container.querySelectorAll("button");
    expect(buttons.length).toBe(2);
    fireEvent.click(buttons[1]);
    expect(increaseMonth).toHaveBeenCalledTimes(1);
  });
});


