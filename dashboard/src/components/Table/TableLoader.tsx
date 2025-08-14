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

import SkeletonLoader from "@components/SkeletonLoader";
import { TableCell, TableRow } from "@mui/material";

const TableRowsLoader = ({ rowsNum }: { rowsNum: number }) => {
  return [...Array(rowsNum)].map((_row, index) => (
    <TableRow key={index}>
      <TableCell component="th" scope="row">
        <SkeletonLoader animation="wave" variant="text" count={1} />
      </TableCell>
      <TableCell>
        <SkeletonLoader animation="wave" variant="text" count={1} />
      </TableCell>
      <TableCell>
        <SkeletonLoader animation="wave" variant="text" count={1} />
      </TableCell>
      <TableCell>
        <SkeletonLoader animation="wave" variant="text" count={1} />
      </TableCell>
    </TableRow>
  ));
};

export default TableRowsLoader;
