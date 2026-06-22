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
import "./masonry.css";

export type MasonryGridProps = {
  minColumnWidth?: number;
  columnGap?: number;
  rowGap?: number;
  rowHeight?: number;
  className?: string;
  style?: React.CSSProperties;
  children: React.ReactNode;
};

/**
 * Responsive CSS Grid container that supports a Masonry-like layout.
 * Children should set their own grid-row-end span based on measured height.
 */
const MasonryGrid: React.FC<MasonryGridProps> = ({
  minColumnWidth = 280,
  columnGap = 16,
  rowGap = 16,
  rowHeight = 8,
  className,
  style,
  children
}) => {
  const mergedStyle: React.CSSProperties = {
    // grid settings
    display: "grid",
    gridTemplateColumns: `repeat(auto-fill, minmax(${minColumnWidth}px, 1fr))`,
    gridAutoFlow: "dense",
    gridAutoRows: `${rowHeight}px`,
    columnGap,
    rowGap,
    ...style
  };

  return (
    <div
      className={`masonry-grid${className ? ` ${className}` : ""}`}
      style={mergedStyle}
      data-row-height={rowHeight}
      data-row-gap={rowGap}
    >
      {children}
    </div>
  );
};

export default MasonryGrid;





