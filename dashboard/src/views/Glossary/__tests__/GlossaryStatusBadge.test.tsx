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

import { render, screen } from "@testing-library/react";
import GlossaryStatusBadge from "../GlossaryStatusBadge";

describe("GlossaryStatusBadge", () => {
  it("renders active status with success color only", () => {
    render(<GlossaryStatusBadge status="ACTIVE" />);
    const badge = screen.getByLabelText("Active status");
    expect(badge).toBeInTheDocument();
    expect(badge).toHaveTextContent("Active");
    expect(badge).toHaveClass("MuiChip-colorSuccess");
    expect(badge.querySelector(".MuiChip-icon")).not.toBeInTheDocument();
  });

  it("renders draft status with warning color only", () => {
    render(<GlossaryStatusBadge status="draft" />);
    const badge = screen.getByLabelText("Draft status");
    expect(badge).toHaveClass("MuiChip-colorWarning");
    expect(badge).toHaveTextContent("Draft");
    expect(badge.querySelector(".MuiChip-icon")).not.toBeInTheDocument();
  });

  it("renders deprecated status with error color and short label", () => {
    render(<GlossaryStatusBadge status="DEPRECATED" />);
    const badge = screen.getByLabelText("Deprecated status");
    expect(badge).toHaveClass("MuiChip-colorError");
    expect(badge).toHaveTextContent("Depr.");
    expect(badge.querySelector(".MuiChip-icon")).not.toBeInTheDocument();
  });

  it("renders unknown status with default color and em dash label", () => {
    render(<GlossaryStatusBadge status="INVALID" />);
    const badge = screen.getByLabelText("Unknown status");
    expect(badge).toHaveClass("MuiChip-colorDefault");
    expect(badge).toHaveTextContent("—");
    expect(badge.querySelector(".MuiChip-icon")).not.toBeInTheDocument();
  });
});
