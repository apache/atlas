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
import {
  CircularProgress,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Typography
} from "@mui/material";
import { Link } from "react-router-dom";
import { getSimilarEntities } from "@api/apiMethods/searchApiMethod";
import DisplayImage from "@components/EntityDisplayImage";
import { extractKeyValueFromEntity, serverError } from "@utils/Utils";
import { entityStateReadOnly } from "@utils/Enum";

const SimilarEntitiesTab = ({ guid }: { guid: string }) => {
  const [loading, setLoading] = useState(true);
  const [results, setResults] = useState<{ entity: any; score?: number }[]>(
    []
  );
  const toastId = useRef(null);

  useEffect(() => {
    let cancelled = false;

    (async () => {
      try {
        setLoading(true);
        const resp = await getSimilarEntities(guid);
        if (cancelled) {
          return;
        }
        const data = resp?.data || {};
        setResults(
          Array.isArray(data.fullTextResult) ? data.fullTextResult : []
        );
      } catch (error) {
        if (!cancelled) {
          serverError(error, toastId);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [guid]);

  if (loading) {
    return (
      <Stack alignItems="center" py={4}>
        <CircularProgress size={32} />
      </Stack>
    );
  }

  if (!results.length) {
    return (
      <Typography sx={{ p: 2 }} color="text.secondary">
        No similar entities found. Ensure entities are indexed.
      </Typography>
    );
  }

  return (
    <Table size="small" data-cy="similar-entities-list">
      <TableHead>
        <TableRow>
          <TableCell>Name</TableCell>
          <TableCell>Type</TableCell>
          <TableCell align="right">Similarity</TableCell>
        </TableRow>
      </TableHead>
      <TableBody>
        {results.map(({ entity, score }, index) => {
          const { name } = extractKeyValueFromEntity(entity);
          return (
            <TableRow key={`${entity.guid}-${index}`}>
              <TableCell>
                <Stack direction="row" gap={1} alignItems="center">
                  <DisplayImage entity={entity} />
                  <Link
                    className={`nav-link text-decoration-none ${
                      entity.status && entityStateReadOnly[entity.status]
                        ? "text-red"
                        : "text-blue"
                    }`}
                    to={{ pathname: `/detailPage/${entity.guid}` }}
                  >
                    {name}
                  </Link>
                </Stack>
              </TableCell>
              <TableCell>{entity.typeName}</TableCell>
              <TableCell align="right">
                {score != null ? score.toFixed(4) : "—"}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
};

export default SimilarEntitiesTab;
