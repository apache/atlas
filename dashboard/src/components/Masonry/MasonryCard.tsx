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

import React, { useEffect, useRef, useState } from "react";
import "./masonry.css";

export type MasonryCardProps = {
  title: string;
  maxBodyHeight?: number; // max visible height for body scroll area
  footer?: React.ReactNode;
  className?: string;
  style?: React.CSSProperties;
  children: React.ReactNode; // card body content
};

/**
 * A card that measures its height and sets grid-row-end to fill the CSS Grid tracks.
 * The body section gets a max-height with internal scroll to keep card height bounded.
 */
const MasonryCard: React.FC<MasonryCardProps> = ({
  title,
  maxBodyHeight = 260,
  footer,
  className,
  style,
  children
}) => {
  const cardRef = useRef<HTMLDivElement | null>(null);
  const [rowSpan, setRowSpan] = useState<number>(1);

  useEffect(() => {
    const el = cardRef.current;
    if (!el) return;

    const grid = el.parentElement as HTMLElement | null;
    const rowHeight = Number(grid?.dataset.rowHeight || 8);
    const rowGap = Number(grid?.dataset.rowGap || 16);

    const measure = () => {
      const height = el.getBoundingClientRect().height;
      const span = Math.max(1, Math.ceil((height + rowGap) / (rowHeight + rowGap)));
      setRowSpan(span);
    };

    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  return (
    <div
      ref={cardRef}
      className={`masonry-card${className ? ` ${className}` : ""}`}
      style={{ gridRowEnd: `span ${rowSpan}`, ...style }}
    >
      <div className="masonry-card__header">{title}</div>
      <div className="masonry-card__body" style={{ maxHeight: maxBodyHeight }}>
        {children}
      </div>
      {footer ? <div className="masonry-card__footer">{footer}</div> : null}
    </div>
  );
};

export default MasonryCard;





