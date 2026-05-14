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
import { render, screen } from "@utils/test-utils";
import CustomDatepicker from "@components/DatePicker/CustomDatePicker";

let capturedProps: any;

// Mock date-fns used by CustomHeader
jest.mock("date-fns", () => ({
  getYear: (d: Date) => d.getFullYear(),
  getMonth: (d: Date) => d.getMonth()
}));

// Mock react-datepicker to a simple input-like component to avoid types/transform issues
jest.mock("react-datepicker", () => {
  const React = require("react");
  return React.forwardRef((props: any, ref: any) => {
    capturedProps = props;
    const { renderCustomHeader } = props;
    const headerProps = {
      date: new Date(2024, 0, 1),
      changeYear: jest.fn(),
      changeMonth: jest.fn(),
      decreaseMonth: jest.fn(),
      increaseMonth: jest.fn(),
      prevMonthButtonDisabled: false,
      nextMonthButtonDisabled: false
    };
    return (
      <div>
        {renderCustomHeader ? renderCustomHeader(headerProps) : null}
        <div data-testid="mock-datepicker" />
      </div>
    );
  });
});

// react-datepicker renders inputs and popper elements; we verify key props

describe("CustomDatepicker", () => {
  it("renders with selected date, forwards props, and uses custom header", () => {
    const selected = new Date(2024, 0, 1, 10, 30, 45);
    const onChange = jest.fn();

    const { container, rerender } = render(
      <CustomDatepicker
        selected={selected}
        onChange={onChange}
        placeholderText="Pick a date"
        showTimeInput
        // Intentionally override with a different format to verify passthrough precedence
        dateFormat="yyyy-MM-dd"
      />
    );

    // Input should exist and have placeholder (prop passthrough)
    const mock = screen.getByTestId("mock-datepicker");
    expect(mock).toBeTruthy();

    // Assert forwarded props on initial render (override in rest should take precedence)
    expect(capturedProps.selected).toBe(selected);
    expect(capturedProps.onChange).toBe(onChange);
    expect(capturedProps.timeInputLabel).toBe("");
    expect(capturedProps.dateFormat).toBe("yyyy-MM-dd");
    expect(typeof capturedProps.renderCustomHeader).toBe("function");

    // Changing props should re-render
    rerender(
      <CustomDatepicker
        selected={selected}
        onChange={onChange}
        placeholderText="Choose"
        showTimeInput
      />
    );
    const mock2 = screen.getByTestId("mock-datepicker");
    expect(mock2).toBeTruthy();

    // After rerender without overriding dateFormat, the component's default should apply
    expect(capturedProps.dateFormat).toBe("MM/dd/yyyy h:mm:ss aa");

    // Ensure custom header render function is invoked by our mock
    // The mocked component renders the header immediately if provided
    // So presence of the wrapper ensures no crash
    expect(container.firstChild).toBeTruthy();
  });
});


