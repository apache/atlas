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
  List,
  ListItem,
  Stack,
  Typography
} from "@mui/material";
import { Link } from "react-router-dom";
import { getSimilarEntities } from "@api/apiMethods/searchApiMethod";
import DisplayImage from "@components/EntityDisplayImage";
import { extractKeyValueFromEntity, serverError } from "@utils/Utils";
import { entityStateReadOnly } from "@utils/Enum";

interface SimilarEntitiesTabProps {
  guid: string;
  typeName?: string;
}

const SimilarEntitiesTab = ({ guid, typeName }: SimilarEntitiesTabProps) => {
  const [loading, setLoading] = useState(true);
  const [entities, setEntities] = useState<any[]>([]);
  const [scores, setScores] = useState<Record<string, number>>({});
  const toastId = useRef(null);

  useEffect(() => {
    if (!guid) {
      setLoading(false);
      return;
    }

    let cancelled = false;

    (async () => {
      try {
        setLoading(true);
        const resp = await getSimilarEntities(guid, {
          params: {
            topK: 25,
            ...(typeName ? { typeName } : {})
          }
        });
        if (cancelled) {
          return;
        }
        const data = resp?.data || {};
        setEntities(Array.isArray(data.entities) ? data.entities : []);
        setScores(
          data.similarityScores && typeof data.similarityScores === "object"
            ? data.similarityScores
            : {}
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
  }, [guid, typeName]);

  if (loading) {
    return (
      <Stack alignItems="center" py={4}>
        <CircularProgress size={32} />
      </Stack>
    );
  }

  if (!entities.length) {
    return (
      <Typography sx={{ p: 2 }} color="text.secondary">
        No similar entities found. Ensure semantic search is enabled and entities
        are indexed.
      </Typography>
    );
  }

  return (
    <List data-cy="similar-entities-list">
      {entities.map((entity) => {
        const { name } = extractKeyValueFromEntity(entity);
        const score = scores[entity.guid];
        return (
          <ListItem
            key={entity.guid}
            sx={{ gap: 1, alignItems: "center" }}
          >
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
            <Typography variant="body2" color="text.secondary" sx={{ ml: "auto" }}>
              {entity.typeName}
              {score != null ? ` · score ${score.toFixed(4)}` : ""}
            </Typography>
          </ListItem>
        );
      })}
    </List>
  );
};

export default SimilarEntitiesTab;
