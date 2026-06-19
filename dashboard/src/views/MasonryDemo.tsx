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
import MasonryGrid from "@components/Masonry/MasonryGrid";
import MasonryCard from "@components/Masonry/MasonryCard";

const randomText = (lines: number) =>
  Array.from({ length: lines }, (_, i) => `Line ${i + 1}: Lorem ipsum dolor sit amet.`).join(
    "\n"
  );

const MasonryDemo: React.FC = () => {
  const cards = [
    { title: "Columns (1000)", lines: 6 },
    { title: "ddlQueries (1)", lines: 2 },
    { title: "inputToProcesses", lines: 1 },
    { title: "meanings", lines: 3 },
    { title: "model", lines: 12 },
    { title: "outputFromProcesses", lines: 4 },
    { title: "partitionKeys", lines: 2 },
    { title: "Extra", lines: 10 },
    { title: "Another", lines: 7 }
  ];

  return (
    <div style={{ padding: 16 }}>
      <MasonryGrid minColumnWidth={280} rowHeight={8} columnGap={16} rowGap={16}>
        {cards.map((c, idx) => (
          <MasonryCard key={idx} title={c.title} maxBodyHeight={260}>
            <pre style={{ margin: 0, whiteSpace: "pre-wrap" }}>{randomText(c.lines)}</pre>
          </MasonryCard>
        ))}
      </MasonryGrid>
    </div>
  );
};

export default MasonryDemo;





