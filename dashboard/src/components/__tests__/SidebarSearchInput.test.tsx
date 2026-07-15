/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  "See the License"); you may not use this file except in compliance with
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
import { render, screen, fireEvent } from "@testing-library/react";
import { SidebarSearchInput } from "../SidebarSearchInput";

describe("SidebarSearchInput Component", () => {
  it("should render correctly with empty search term and not show clear icon", () => {
    render(<SidebarSearchInput searchTerm="" onChange={jest.fn()} />);
    const input = screen.getByPlaceholderText("Search");
    expect(input).toBeInTheDocument();
    expect(input).toHaveValue("");
    
    // ClearIcon should not be in the document
    expect(screen.queryByTestId("ClearIcon")).not.toBeInTheDocument();
  });

  it("should render correctly with search term and show clear icon", () => {
    render(<SidebarSearchInput searchTerm="test query" onChange={jest.fn()} />);
    const input = screen.getByPlaceholderText("Search");
    expect(input).toHaveValue("test query");
    
    // ClearIcon should be visible
    expect(screen.getByTestId("ClearIcon")).toBeInTheDocument();
  });

  it("should invoke onChange prop when user types", () => {
    const mockOnChange = jest.fn();
    render(<SidebarSearchInput searchTerm="" onChange={mockOnChange} />);
    const input = screen.getByPlaceholderText("Search");
    
    fireEvent.change(input, { target: { value: "h" } });
    expect(mockOnChange).toHaveBeenCalledWith("h");
  });

  it("should invoke onChange with empty string when clear button is clicked", () => {
    const mockOnChange = jest.fn();
    render(<SidebarSearchInput searchTerm="some search" onChange={mockOnChange} />);
    
    const clearButton = screen.getByRole("button");
    fireEvent.click(clearButton);
    expect(mockOnChange).toHaveBeenCalledWith("");
  });

  it("should support data-cy prop for cypress testing", () => {
    render(<SidebarSearchInput searchTerm="" onChange={jest.fn()} dataCy="my-search-input" />);
    const input = screen.getByPlaceholderText("Search");
    const container = input.closest(".MuiInputBase-root");
    expect(container).toHaveAttribute("data-cy", "my-search-input");
  });
});
