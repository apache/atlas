/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

define(['require',
    'backbone',
    'hbs!tmpl/schema/SchemaTableLayoutView_tmpl',
    'collection/VSchemaList',
    'utils/Utils',
    'utils/CommonViewFunction',
    'utils/Messages',
    'utils/Globals',
    'utils/Enums',
    'utils/UrlLinks'
], function(require, Backbone, SchemaTableLayoutViewTmpl, VSchemaList, Utils, CommonViewFunction, Messages, Globals, Enums, UrlLinks) {
    'use strict';

    /** Per-entity cache so switching detail tabs does not refetch schema (same guid). */
    var schemaTabCacheByGuid = {};

    var SchemaTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends SchemaTableLayoutView */
        {
            _viewName: 'SchemaTableLayoutView',

            template: SchemaTableLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RSchemaTableLayoutView: "#r_schemaTableLayoutView",
            },
            /** ui selector cache */
            ui: {
                tagClick: '[data-id="tagClick"]',
                addTag: "[data-id='addTag']",
                addAssignTag: "[data-id='addAssignTag']",
                checkDeletedEntity: "[data-id='checkDeletedEntity']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.addTag] = 'checkedValue';
                events["click " + this.ui.addAssignTag] = 'checkedValue';
                events["click " + this.ui.tagClick] = function(e) {
                    if (e.target.nodeName.toLocaleLowerCase() == "i") {
                        this.onClickTagCross(e);
                    } else {
                        var value = e.currentTarget.text;
                        Utils.setUrl({
                            url: '#!/tag/tagAttribute/' + value,
                            mergeBrowserUrl: false,
                            trigger: true
                        });
                    }
                };
                events["click " + this.ui.checkDeletedEntity] = 'onCheckDeletedEntity';

                return events;
            },
            /**
             * intialize a new SchemaTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'classificationDefCollection', 'entityDefCollection', 'attribute', 'fetchCollection', 'enumDefCollection', 'schemaRelationNames', 'referredEntities'));
                this.schemaRelationNames = options.schemaRelationNames || [];
                this.schemaCollection = new VSchemaList([], {});
                /* Match relationship search chunk size to table page size (default 100). */
                this.schemaCollection.state.pageSize = 100;
                /*
                 * TableLayout onClicknextData / changePageLimit mirror offset here; sync must
                 * read this before overwriting tl.offset from Pageable state (stale currentPage
                 * leaves footer at 1–100 while Next already requested offset 100).
                 */
                if (!this.schemaCollection.queryParams) {
                    this.schemaCollection.queryParams = { limit: 100, offset: 0 };
                }
                this.commonTableOptions = {
                    collection: this.schemaCollection,
                    includeFilter: false,
                    includePagination: false,
                    includeAtlasPagination: true,
                    includeAtlasPageSize: true,
                    includePageSize: false,
                    includeGotoPage: false,
                    includeFooterRecords: false,
                    includeOrderAbleColumns: false,
                    /* Relationship order is by position (see sortSchemaEntityBlocks on load).
                     * Client header-sort reorders fullCollection and breaks pagination slices
                     * after classification refresh; keep schema order only. */
                    includeAtlasTableSorting: false,
                    gridOpts: {
                        className: "table table-hover backgrid table-quickMenu",
                        emptyText: 'No records found!'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
                this.bindEvents();
                this.bradCrumbList = [];
                this.schemaRowsByRelation = {};
                this.schemaTotals = {};
                this._schemaPaginationGuardAttached = false;
                this._schemaTableLayout = null;
                /** Last successful UI page (limit + offset); survives if TableLayout is briefly missing. */
                this._lastKnownSchemaPagination = null;
                /**
                 * Canonical client pagination for the merged schema list (cache is indexed by
                 * offset into `attribute` / mergedAttribute). Survives fullCollection.reset and
                 * TableLayout rebuild so page N × limit always maps to rows (N-1)*limit+1 … N*limit
                 * of cached data, independent of PageableCollection currentPage drift.
                 */
                this._schemaCanonicalPagination = null;
                /** True while sync chunk load runs inside getPage; defer clearing atlas busy until grid renders. */
                this._schemaChunkFetchInProgress = false;
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.schemaCollection, 'backgrid:selected', function(model, checked) {
                    this.arr = [];
                    if (checked === true) {
                        model.set("isEnable", true);
                    } else {
                        model.set("isEnable", false);
                    }
                    this.schemaCollection.find(function(item) {
                        var obj = item.toJSON();
                        if (item.get('isEnable') && item.get('status') === 'ACTIVE') {
                            that.arr.push({
                                id: obj.guid,
                                model: obj
                            });
                        }
                    });
                    if (this.arr.length > 0) {
                        this.$('.multiSelectTag').show();
                    } else {
                        this.$('.multiSelectTag').hide();
                    }
                });
            },
            onRender: function() {
                var that = this;
                if (!this.guid || _.isEmpty(this.schemaRelationNames)) {
                    this.generateTableData();
                    return;
                }
                var cacheKey = this.guid;
                var cached = schemaTabCacheByGuid[cacheKey];
                var namesKey = this.schemaRelationNames.slice().sort().join('|');
                if (cached && cached.namesKey === namesKey && !cached.rowsByRelation) {
                    delete schemaTabCacheByGuid[cacheKey];
                    cached = null;
                }
                if (cached && cached.namesKey === namesKey && cached.rowsByRelation) {
                    this.schemaRowsByRelation = cached.rowsByRelation;
                    this.schemaTotals = cached.totals || {};
                    this.attribute = cached.mergedAttribute;
                    this.referredEntities = _.extend({}, this.referredEntities || {}, cached.referredEntities);
                    if (cached.paginationCanonical &&
                        _.isFinite(cached.paginationCanonical.pageSize) &&
                        cached.paginationCanonical.pageSize > 0) {
                        this._schemaCanonicalPagination = {
                            pageSize: cached.paginationCanonical.pageSize,
                            offset: _.isFinite(cached.paginationCanonical.offset)
                                ? Math.max(0, cached.paginationCanonical.offset)
                                : 0
                        };
                        this._lastKnownSchemaPagination = _.extend({}, this._schemaCanonicalPagination);
                    }
                    this.generateTableData();
                    return;
                }
                this.showLoader();
                this.fetchSchemaFromRelationshipApi(function(err, merged, refMap) {
                    if (err) {
                        that.hideLoader();
                        that.generateTableData();
                        return;
                    }
                    that.attribute = merged;
                    that.referredEntities = _.extend({}, that.referredEntities || {}, refMap);
                    schemaTabCacheByGuid[cacheKey] = {
                        namesKey: namesKey,
                        mergedAttribute: merged,
                        referredEntities: refMap,
                        rowsByRelation: that.schemaRowsByRelation,
                        totals: that.schemaTotals,
                        paginationCanonical: null
                    };
                    that.hideLoader();
                    that.generateTableData();
                });
            },
            /**
             * Match React SchemaTab: order by relationship `attributes.position` (numeric)
             * when present; rows with position sort before rows without; then name; then guid.
             */
            sortRowsInRelation: function(rows) {
                if (!rows || !rows.length) {
                    return rows || [];
                }
                var positionKey = function(row) {
                    var p = row && row.attributes && row.attributes.position;
                    if (p === undefined || p === null || p === '') {
                        return null;
                    }
                    var n = Number(p);
                    return _.isFinite(n) ? n : null;
                };
                return rows.slice().sort(function(a, b) {
                    var pa = positionKey(a);
                    var pb = positionKey(b);
                    if (pa !== null && pb !== null && pa !== pb) {
                        return pa - pb;
                    }
                    if (pa !== null && pb === null) {
                        return -1;
                    }
                    if (pa === null && pb !== null) {
                        return 1;
                    }
                    var na = (a.attributes && a.attributes.name) ? String(a.attributes.name) : '';
                    var nb = (b.attributes && b.attributes.name) ? String(b.attributes.name) : '';
                    var cmp = na.localeCompare(nb);
                    if (cmp !== 0) {
                        return cmp;
                    }
                    var ga = a.guid ? String(a.guid) : '';
                    var gb = b.guid ? String(b.guid) : '';
                    return ga.localeCompare(gb);
                });
            },
            sortSchemaEntityBlocks: function(blocks) {
                var that = this;
                return _.flatten(_.map(blocks, function(block) {
                    return that.sortRowsInRelation(block || []);
                }));
            },
            /**
             * Re-sort every cached relation block and the flattened merged list by
             * attributes.position (relationship order). Call when Page Limit changes
             * so pagination slices match position order; keeps add/remove classification
             * display consistent with sorted cache.
             */
            resortCachedSchemaMergedAttributeByPosition: function() {
                var that = this;
                if (!this.schemaRelationNames || _.isEmpty(this.schemaRowsByRelation)) {
                    return;
                }
                _.each(this.schemaRelationNames, function(relName) {
                    var rows = that.schemaRowsByRelation[relName];
                    if (rows && rows.length) {
                        that.schemaRowsByRelation[relName] = that.sortRowsInRelation(rows);
                    }
                });
                var blocks = _.map(this.schemaRelationNames, function(n) {
                    return that.schemaRowsByRelation[n] || [];
                });
                this.attribute = _.flatten(blocks);
                var cacheKey = this.guid;
                if (cacheKey) {
                    var cached = schemaTabCacheByGuid[cacheKey];
                    if (cached) {
                        cached.rowsByRelation = this.schemaRowsByRelation;
                        cached.mergedAttribute = this.attribute;
                    }
                }
            },
            /**
             * After child GET merge, refresh merged row objects in `attribute` without
             * re-sorting. Full sortSchemaEntityBlocks() would reorder if compare keys shift
             * (e.g. col_289 moves off page 12 after adding a classification).
             */
            patchAttributeRowsFromSchemaRowsByRelation: function() {
                var that = this;
                var prev = this.attribute;
                if (!prev || !prev.length) {
                    var blocks = _.map(this.schemaRelationNames, function(n) {
                        return that.schemaRowsByRelation[n] || [];
                    });
                    this.attribute = this.sortSchemaEntityBlocks(blocks);
                    return;
                }
                var mergedByGuid = {};
                _.each(this.schemaRelationNames, function(relName) {
                    _.each(that.schemaRowsByRelation[relName] || [], function(row) {
                        if (row && row.guid) {
                            mergedByGuid[row.guid] = row;
                        }
                    });
                });
                this.attribute = _.map(prev, function(row) {
                    if (row && row.guid && mergedByGuid[row.guid]) {
                        return mergedByGuid[row.guid];
                    }
                    return row;
                });
            },
            /** Write canonical pagination into `schemaTabCacheByGuid` for this entity. */
            persistSchemaCanonicalToCache: function() {
                var snap = this._schemaCanonicalPagination;
                var key = this.guid;
                if (!key || !snap || !_.isFinite(snap.pageSize) || snap.pageSize <= 0) {
                    return;
                }
                var cached = schemaTabCacheByGuid[key];
                if (!cached) {
                    return;
                }
                cached.paginationCanonical = {
                    pageSize: snap.pageSize,
                    offset: _.isFinite(snap.offset) ? Math.max(0, snap.offset) : 0
                };
            },
            /** Derive canonical pagination from TableLayout (footer offset/limit) and sync cache. */
            persistSchemaCanonicalFromTableLayout: function() {
                var tl = this._schemaTableLayout;
                if (!tl || !_.isFinite(tl.limit) || tl.limit <= 0) {
                    return;
                }
                var off = _.isFinite(tl.offset) ? Math.max(0, tl.offset) : 0;
                this._schemaCanonicalPagination = {
                    pageSize: tl.limit,
                    offset: off
                };
                this._lastKnownSchemaPagination = {
                    pageSize: tl.limit,
                    offset: off
                };
                this.persistSchemaCanonicalToCache();
                this.syncSchemaCollectionQueryParamsToPagination(tl.limit, off);
            },
            /** Keep collection.queryParams aligned with footer offset/limit (TableLayout reads this in sync). */
            syncSchemaCollectionQueryParamsToPagination: function(pageSize, offset) {
                var col = this.schemaCollection;
                if (!col || !_.isFinite(pageSize) || pageSize <= 0) {
                    return;
                }
                if (!col.queryParams) {
                    col.queryParams = {};
                }
                col.queryParams.limit = pageSize;
                col.queryParams.offset = _.isFinite(offset) ? Math.max(0, offset) : 0;
            },
            /**
             * Match React relationshipSearchQuery.normalizeRelationshipApproximateCount:
             * Atlas may expose approximateCount and/or totalCount; ignore negative / -1.
             */
            normalizeRelationshipApproximateCount: function(resp) {
                if (!resp) {
                    return undefined;
                }
                var raw = resp.approximateCount !== undefined && resp.approximateCount !== null
                    ? resp.approximateCount
                    : resp.totalCount;
                if (raw === undefined || raw === null) {
                    return undefined;
                }
                var n = Number(raw);
                if (!_.isFinite(n) || n < 0) {
                    return undefined;
                }
                return n;
            },
            /**
             * Rows to request per relationship search call (per-relation offset cursor).
             * Kept at 100 for fewer round-trips on large schemas; grid Page Limit is separate
             * (TableLayout / canonical pagination slice the merged list). Network may show
             * limit=100 while footer shows limit=10 — that is expected.
             */
            getRelationshipApiLimit: function() {
                return 100;
            },
            /**
             * Merge relationship-search row with fresh GET entity. Atlas often omits
             * `classifications` when empty; shallow _.extend keeps stale chips otherwise.
             * Mirrors React resolveEntity (classifications + attributes precedence).
             */
            mergeSchemaRowWithFreshEntity: function(row, entity, opts) {
                opts = opts || {};
                if (!entity) {
                    return row;
                }
                var merged = _.extend({}, row, entity);
                /*
                 * GET with minExtInfo often returns partial attributes (no position/name).
                 * Replacing attributes wholesale made sortSchemaEntityBlocks treat position as
                 * missing (1e9), moving the row to the end — wrong slice on paginated pages.
                 */
                merged.attributes = _.extend(
                    {},
                    (row && row.attributes) || {},
                    (entity && entity.attributes) || {}
                );
                /*
                 * Relationship search row is authoritative for schema order (position, name).
                 * minExtInfo GET can still overwrite or omit these; keep row values.
                 */
                var ra = row && row.attributes;
                if (ra) {
                    if (ra.position !== undefined && ra.position !== null) {
                        merged.attributes.position = ra.position;
                    }
                    if (ra.name !== undefined && ra.name !== null && ra.name !== '') {
                        merged.attributes.name = ra.name;
                    }
                }
                if (Object.prototype.hasOwnProperty.call(entity, 'classifications')) {
                    merged.classifications = entity.classifications == null ? [] : entity.classifications;
                } else if (opts.fromClassificationRefresh) {
                    /*
                     * After remove-tag (or last tag removed), Atlas may omit the
                     * classifications key entirely when empty (same as [] in Preview).
                     * Keeping row.classifications here left stale chips on the row.
                     */
                    merged.classifications = [];
                } else {
                    /*
                     * minExtInfo GET may omit classifications entirely; do not replace
                     * with [] or add-tag refresh appears to lose the new assignment.
                     */
                    merged.classifications = (row && row.classifications) ? row.classifications : [];
                }
                if (Object.prototype.hasOwnProperty.call(entity, 'classificationNames')) {
                    merged.classificationNames = entity.classificationNames;
                } else if (opts.fromClassificationRefresh &&
                    (!merged.classifications || merged.classifications.length === 0)) {
                    merged.classificationNames = [];
                }
                return merged;
            },
            /**
             * After classification add/remove on schema row(s), GET each child entity guid
             * (ignoreRelationships) and merge into cache — no relationship search replay.
             */
            refreshChildEntitiesAfterClassification: function(childGuids, done) {
                var that = this;
                childGuids = _.chain(childGuids || []).compact().uniq().value();
                if (childGuids.length === 0) {
                    if (done) {
                        done();
                    }
                    return;
                }
                var index = 0;
                var step = function() {
                    if (index >= childGuids.length) {
                        /*
                         * Do not re-sort the merged list after classification — only patch row
                         * objects from schemaRowsByRelation so global index (page N) is stable.
                         */
                        that.patchAttributeRowsFromSchemaRowsByRelation();
                        var cacheKey = that.guid;
                        var cached = schemaTabCacheByGuid[cacheKey];
                        if (cached) {
                            cached.rowsByRelation = that.schemaRowsByRelation;
                            cached.mergedAttribute = that.attribute;
                            cached.referredEntities = _.extend({}, cached.referredEntities || {}, that.referredEntities || {});
                            if (that._schemaCanonicalPagination) {
                                cached.paginationCanonical = _.extend({}, that._schemaCanonicalPagination);
                            }
                        }
                        that.generateTableData();
                        if (done) {
                            done();
                        }
                        return;
                    }
                    var g = childGuids[index++];
                    var url = UrlLinks.entitiesApiUrl({ guid: g, minExtInfo: true });
                    $.ajax({
                        url: url,
                        type: 'GET',
                        dataType: 'json'
                    }).fail(function() {
                        step();
                    }).done(function(resp) {
                        var entity = resp && resp.entity ? resp.entity : null;
                        if (entity) {
                            that.referredEntities = _.extend({}, that.referredEntities || {});
                            that.referredEntities[g] = entity;
                            _.each(that.schemaRelationNames, function(relName) {
                                var rows = that.schemaRowsByRelation[relName] || [];
                                for (var i = 0; i < rows.length; i++) {
                                    var row = rows[i];
                                    if (row && row.guid === g) {
                                        rows[i] = that.mergeSchemaRowWithFreshEntity(row, entity, {
                                            fromClassificationRefresh: true
                                        });
                                        break;
                                    }
                                }
                            });
                        }
                        step();
                    });
                };
                step();
            },
            fetchSchemaFromRelationshipApi: function(done) {
                var that = this;
                var apiUrl = UrlLinks.relationshipSearchV2ApiUrl();
                /*
                 * jQuery $.when with multiple $.ajax passes each jqXHR result as three
                 * callback args (data, textStatus, jqXHR). Unwrap to one JSON body per
                 * relation so arguments.length === schemaRelationNames.length.
                 */
                var reqs = _.map(this.schemaRelationNames, function(relationName) {
                    var params = {
                        limit: that.getRelationshipApiLimit(),
                        offset: 0,
                        guid: that.guid,
                        disableDefaultSorting: true,
                        excludeDeletedEntities: false,
                        includeSubClassifications: true,
                        includeSubTypes: true,
                        includeClassificationAttributes: true,
                        relation: relationName,
                        getApproximateCount: true,
                        attributes: ['type', 'position']
                    };
                    var qs = Utils.serializeRelationshipQueryParams(params);
                    return $.ajax({
                        url: apiUrl + '?' + qs,
                        type: 'GET',
                        dataType: 'json'
                    }).then(function(data) {
                        return data;
                    });
                });
                if (reqs.length === 0) {
                    return done(null, [], {});
                }
                $.when.apply($, reqs).fail(function() {
                    done(new Error('schema relationship fetch'));
                }).done(function() {
                    var blocks = [];
                    var refMap = {};
                    var rowsByRelation = {};
                    var totals = {};
                    for (var i = 0; i < arguments.length; i++) {
                        var resp = arguments[i];
                        var relationName = that.schemaRelationNames[i];
                        var entities = resp && resp.entities ? resp.entities : [];
                        blocks.push(entities);
                        rowsByRelation[relationName] = entities;
                        var approx = that.normalizeRelationshipApproximateCount(resp);
                        if (approx !== undefined) {
                            totals[relationName] = approx;
                        }
                        if (resp && resp.referredEntities) {
                            _.extend(refMap, resp.referredEntities);
                        }
                    }
                    that.schemaRowsByRelation = rowsByRelation;
                    that.schemaTotals = totals;
                    var merged = that.sortSchemaEntityBlocks(blocks);
                    done(null, merged, refMap);
                });
            },
            schemaHasMore: function() {
                var that = this;
                if (!this.schemaRelationNames || _.isEmpty(this.schemaRowsByRelation)) {
                    return false;
                }
                return _.some(this.schemaRelationNames, function(name) {
                    var rows = that.schemaRowsByRelation[name] || [];
                    var total = that.schemaTotals[name] || 0;
                    return rows.length < total;
                });
            },
            /** Total merged rows across relations (same idea as React totalMergedRows). Prefer over fullCollection.length in prefetch guard. */
            getMergedSchemaRowCount: function() {
                var that = this;
                if (!this.schemaRelationNames || _.isEmpty(this.schemaRowsByRelation)) {
                    return this.schemaCollection && this.schemaCollection.fullCollection
                        ? this.schemaCollection.fullCollection.length
                        : 0;
                }
                return _.reduce(this.schemaRelationNames, function(sum, name) {
                    return sum + ((that.schemaRowsByRelation[name] || []).length);
                }, 0);
            },
            /**
             * Sum of per-relation max(approximateCount, loaded) — matches React SchemaTab
             * mergedApproximateTotal / TablePagination totalCount.
             */
            getMergedApproximateTotal: function() {
                var that = this;
                if (!this.schemaRelationNames || _.isEmpty(this.schemaRowsByRelation)) {
                    return this.getMergedSchemaRowCount();
                }
                return _.reduce(this.schemaRelationNames, function(sum, name) {
                    var loaded = (that.schemaRowsByRelation[name] || []).length;
                    var approx = that.schemaTotals[name];
                    var t = (_.isFinite(approx) && approx >= 0) ? Math.max(approx, loaded) : loaded;
                    return sum + t;
                }, 0);
            },
            handleSchemaAtlasFetch: function(options) {
                var col = this.schemaCollection;
                var that = this;
                if (!col) {
                    return;
                }
                var tlNav = this._schemaTableLayout;
                /*
                 * Page Limit change: re-sort full cached schema by relationship position,
                 * then rebuild the grid so slices and classification updates stay aligned.
                 */
                if (options && options.fromPageLimitChange && this.guid &&
                    !_.isEmpty(this.schemaRelationNames) &&
                    !_.isEmpty(this.schemaRowsByRelation) &&
                    this.getMergedSchemaRowCount() > 0) {
                    /*
                     * captureSchemaPaginationSnapshot prefers _schemaCanonicalPagination
                     * before live TableLayout. After the user changes Page Limit, canonical
                     * still held the previous pageSize/offset until generateTableData runs —
                     * applySchemaPaginationSnapshot sliced the wrong window and classification
                     * refresh used a stale offset. Align canonical + collection with the
                     * dropdown (offset 0) before resort + rebuild.
                     */
                    var tlPl = this._schemaTableLayout;
                    if (tlPl && _.isFinite(tlPl.limit) && tlPl.limit > 0) {
                        this._schemaCanonicalPagination = {
                            pageSize: tlPl.limit,
                            offset: 0
                        };
                    }
                    if (col.state) {
                        if (tlPl && _.isFinite(tlPl.limit) && tlPl.limit > 0) {
                            col.state.pageSize = tlPl.limit;
                        }
                        col.queryParams = col.queryParams || {};
                        if (tlPl && _.isFinite(tlPl.limit) && tlPl.limit > 0) {
                            col.queryParams.limit = tlPl.limit;
                        }
                        col.queryParams.offset = 0;
                    }
                    this.showLoader();
                    if (this._schemaTableLayout && this._schemaTableLayout.setAtlasPaginationBusy) {
                        this._schemaTableLayout.setAtlasPaginationBusy(true, 'both');
                    }
                    this.resortCachedSchemaMergedAttributeByPosition();
                    this.generateTableData(undefined, function() {
                        that.hideLoader();
                        if (that._schemaTableLayout && that._schemaTableLayout.setAtlasPaginationBusy) {
                            that._schemaTableLayout.setAtlasPaginationBusy(false);
                        }
                    });
                    return;
                }
                /*
                 * onClicknextData / onClickpreviousData already updated tl.offset and
                 * queryParams before fetchCollection. getNextPage can leave
                 * state.currentPage stale (0), and syncSchemaAtlasTableOffsets would then
                 * set tl.offset = 0 — footer stays "1–100" while the grid shows page 2.
                 * Preserve the navigated offset for Next/Prev only (not first page).
                 */
                var preservedNavOffset = (tlNav && _.isFinite(tlNav.offset))
                    ? Math.max(0, tlNav.offset)
                    : null;
                this.showLoader();
                if (this._schemaTableLayout && this._schemaTableLayout.setAtlasPaginationBusy) {
                    var busyNavHint = (options && options.next)
                        ? 'next'
                        : (options && options.previous)
                            ? 'prev'
                            : 'both';
                    this._schemaTableLayout.setAtlasPaginationBusy(true, busyNavHint);
                }
                /*
                 * TableLayout Page Limit must drive PageableCollection.pageSize on every
                 * nav (next/prev/first). Otherwise state.pageSize can stay at the initial
                 * chunk size (e.g. 100) while the UI shows limit 5 — getNextPage advances
                 * by the wrong window and classification refresh restores the wrong slice.
                 */
                if (this._schemaTableLayout &&
                    _.isFinite(this._schemaTableLayout.limit) &&
                    this._schemaTableLayout.limit > 0) {
                    col.state.pageSize = this._schemaTableLayout.limit;
                }
                /*
                 * getNextPage may run fetchNextSchemaChunkSync (jQuery async:false). That
                 * blocks the main thread until the relationship API returns, so the
                 * browser cannot paint showLoader() until the request finishes — the
                 * loader looked “late”. Yield one task after showLoader/setBusy so the
                 * overlay can paint, then run navigation + any sync chunk fetch.
                 */
                setTimeout(function() {
                    if (!that.schemaCollection || that.isDestroyed) {
                        return;
                    }
                    if (options && options.next) {
                        col.getNextPage({ reset: true });
                    } else if (options && options.previous) {
                        col.getPreviousPage({ reset: true });
                    } else {
                        col.getFirstPage({ reset: true });
                    }
                    that.syncSchemaAtlasTableOffsets();
                    if (tlNav && preservedNavOffset !== null &&
                        options && (options.next || options.previous)) {
                        var limNav = _.isFinite(col.state.pageSize) && col.state.pageSize > 0
                            ? col.state.pageSize
                            : tlNav.limit;
                        if (_.isFinite(limNav) && limNav > 0) {
                            tlNav.offset = preservedNavOffset;
                            tlNav.limit = limNav;
                            var stNav = col.state;
                            var idxNav = Math.floor(preservedNavOffset / limNav);
                            if (stNav.firstPage === 0) {
                                stNav.currentPage = idxNav;
                            } else {
                                stNav.currentPage = idxNav + 1;
                            }
                            that.syncSchemaCollectionQueryParamsToPagination(limNav, preservedNavOffset);
                        }
                    }
                    that.persistSchemaCanonicalFromTableLayout();
                    if (that._schemaTableLayout && that._schemaTableLayout.renderAtlasPagination) {
                        that._schemaTableLayout.renderAtlasPagination(options || {});
                    }
                    /*
                     * Chunk prefetch inside getNextPage runs generateTableData(async). Defer
                     * clearing busy/loader until that callback runs.
                     */
                    if (!that._schemaChunkFetchInProgress) {
                        setTimeout(function() {
                            that.hideLoader();
                            if (that._schemaTableLayout && that._schemaTableLayout.setAtlasPaginationBusy) {
                                that._schemaTableLayout.setAtlasPaginationBusy(false);
                            }
                        }, 0);
                    }
                }, 0);
            },
            syncSchemaAtlasTableOffsets: function() {
                var col = this.schemaCollection;
                var tl = this._schemaTableLayout;
                if (!col || !tl) {
                    return;
                }
                var st = col.state;
                /*
                 * Page Limit dropdown is source of truth. state.pageSize can stay at the
                 * initial relationship chunk size (100) after load until the user
                 * navigates — sync used to set tl.offset = pageIndex * 100 while the
                 * footer showed limit 4, so the first classification refresh after
                 * changing page limit captured the wrong offset (wrong rows on screen).
                 */
                var lim = tl.limit;
                if (tl.ui && tl.ui.showPage && tl.ui.showPage.length) {
                    var fromSelect = parseInt(tl.ui.showPage.val(), 10);
                    if (_.isFinite(fromSelect) && fromSelect > 0) {
                        lim = fromSelect;
                    }
                }
                if (_.isFinite(lim) && lim > 0) {
                    st.pageSize = lim;
                    tl.limit = lim;
                }
                /*
                 * fetchNextSchemaChunkSync runs inside PageableCollection.getPage while
                 * loading more relationship rows (while mergedLen < needRows). Until
                 * origGetPage returns, state.currentPage still reflects the *previous*
                 * page. Writing tl.offset from that stale currentPage zeros the offset and
                 * rebuilds the grid on the wrong slice — footer can show "From 46–60"
                 * while rows are from another window; classification merge then targets
                 * the wrong models. Skip offset sync until the chunk round finishes.
                 */
                if (this._schemaChunkFetchInProgress) {
                    return;
                }
                /*
                 * Next/Prev update collection.queryParams.offset before fetchCollection; that is
                 * authoritative for client Atlas pagination. PageableCollection.currentPage can
                 * lag or reset after fullCollection rebuild — do not derive tl.offset from it
                 * when queryParams already match the user's navigation.
                 */
                var qp = col.queryParams;
                if (qp && qp.offset !== undefined && qp.offset !== null) {
                    var qo = parseInt(qp.offset, 10);
                    if (_.isFinite(qo) && qo >= 0) {
                        tl.offset = qo;
                        var pageIdx = lim > 0 ? Math.floor(qo / lim) : 0;
                        if (st.firstPage === 0) {
                            st.currentPage = pageIdx;
                        } else {
                            st.currentPage = pageIdx + 1;
                        }
                        return;
                    }
                }
                var pageIndex = (st.firstPage === 0) ? st.currentPage : st.currentPage - 1;
                if (pageIndex < 0) {
                    pageIndex = 0;
                }
                tl.offset = pageIndex * st.pageSize;
                if (!col.queryParams) {
                    col.queryParams = {};
                }
                col.queryParams.offset = tl.offset;
                col.queryParams.limit = st.pageSize;
            },
            /**
             * Read page limit + item offset before fullCollection.reset.
             * Prefer live TableLayout limit/offset (matches footer "From A–B"); then
             * PageableCollection.state; then _lastKnownSchemaPagination;
             * then _schemaPaginationSnapshot. Classification refresh must not restore
             * the wrong slice when page limit and Next/Prev desynced state.pageSize.
             */
            captureSchemaPaginationSnapshot: function() {
                var pageSize = 100;
                var offset = 0;
                var col = this.schemaCollection;
                var st = col && col.state;
                /*
                 * Canonical index into merged `attribute` (cache): offset = pageIndex * limit.
                 * Prefer this over PageableCollection / tl when TableLayout is stale mid-rebuild.
                 */
                if (this._schemaCanonicalPagination &&
                    _.isFinite(this._schemaCanonicalPagination.pageSize) &&
                    this._schemaCanonicalPagination.pageSize > 0) {
                    pageSize = this._schemaCanonicalPagination.pageSize;
                    offset = _.isFinite(this._schemaCanonicalPagination.offset)
                        ? Math.max(0, this._schemaCanonicalPagination.offset)
                        : 0;
                    var canonical = { pageSize: pageSize, offset: offset };
                    this._lastKnownSchemaPagination = canonical;
                    return canonical;
                }
                /*
                 * Prefer live TableLayout offset/limit first — that is what the user sees
                 * (footer "From A–B"). PageableCollection.state can lag after page-limit
                 * changes or chunk prefetch; using it first caused wrong offsets on refresh.
                 */
                if (this._schemaTableLayout &&
                    _.isFinite(this._schemaTableLayout.limit) &&
                    this._schemaTableLayout.limit > 0) {
                    pageSize = this._schemaTableLayout.limit;
                    offset = _.isFinite(this._schemaTableLayout.offset)
                        ? Math.max(0, this._schemaTableLayout.offset)
                        : 0;
                    var live = { pageSize: pageSize, offset: offset };
                    this._lastKnownSchemaPagination = live;
                    return live;
                }
                if (st && _.isFinite(st.pageSize) && st.pageSize > 0) {
                    var pageIdx = 0;
                    if (_.isFinite(st.currentPage)) {
                        pageIdx = (st.firstPage === 0) ? st.currentPage : st.currentPage - 1;
                    }
                    if (pageIdx < 0) {
                        pageIdx = 0;
                    }
                    pageSize = st.pageSize;
                    offset = pageIdx * pageSize;
                    var fromState = { pageSize: pageSize, offset: offset };
                    this._lastKnownSchemaPagination = fromState;
                    return fromState;
                }
                if (this._lastKnownSchemaPagination &&
                    _.isFinite(this._lastKnownSchemaPagination.pageSize) &&
                    this._lastKnownSchemaPagination.pageSize > 0) {
                    return {
                        pageSize: this._lastKnownSchemaPagination.pageSize,
                        offset: _.isFinite(this._lastKnownSchemaPagination.offset)
                            ? Math.max(0, this._lastKnownSchemaPagination.offset)
                            : 0
                    };
                }
                if (this._schemaPaginationSnapshot &&
                    _.isFinite(this._schemaPaginationSnapshot.pageSize) &&
                    this._schemaPaginationSnapshot.pageSize > 0) {
                    var prev = {
                        pageSize: this._schemaPaginationSnapshot.pageSize,
                        offset: _.isFinite(this._schemaPaginationSnapshot.offset)
                            ? Math.max(0, this._schemaPaginationSnapshot.offset)
                            : 0
                    };
                    this._lastKnownSchemaPagination = prev;
                    return prev;
                }
                return { pageSize: pageSize, offset: offset };
            },
            /**
             * After models are pushed into fullCollection, restore page size and visible page
             * using getPageByOffset (same idea as React keeping pageIndex + pageSize).
             */
            applySchemaPaginationSnapshot: function(snap) {
                var col = this.schemaCollection;
                if (!col || !snap || !_.isFinite(snap.pageSize) || snap.pageSize <= 0) {
                    return {
                        pageSize: (snap && snap.pageSize) || 100,
                        offset: 0
                    };
                }
                var ps = snap.pageSize;
                var wantOff = _.isFinite(snap.offset) ? Math.max(0, snap.offset) : 0;
                col.state.pageSize = ps;
                var total = col.fullCollection.length;
                if (total === 0) {
                    return { pageSize: ps, offset: 0 };
                }
                var lastPageOffset = Math.floor((total - 1) / ps) * ps;
                var safeOff = Math.min(wantOff, lastPageOffset);
                if (_.isFunction(col.getPageByOffset)) {
                    col.getPageByOffset(safeOff, { reset: true });
                } else {
                    var pageNum = Math.floor(safeOff / ps);
                    col.getPage(pageNum, { reset: true });
                }
                return { pageSize: ps, offset: safeOff };
            },
            fetchNextSchemaChunkSync: function() {
                var that = this;
                this._schemaChunkFetchInProgress = true;
                if (this._schemaTableLayout &&
                    _.isFinite(this._schemaTableLayout.limit) &&
                    this._schemaTableLayout.limit > 0) {
                    this.schemaCollection.state.pageSize = this._schemaTableLayout.limit;
                } else if (this._schemaCanonicalPagination &&
                    _.isFinite(this._schemaCanonicalPagination.pageSize) &&
                    this._schemaCanonicalPagination.pageSize > 0) {
                    this.schemaCollection.state.pageSize = this._schemaCanonicalPagination.pageSize;
                }
                var target = _.find(this.schemaRelationNames, function(name) {
                    var rows = that.schemaRowsByRelation[name] || [];
                    var total = that.schemaTotals[name] || 0;
                    return rows.length < total;
                });
                if (!target) {
                    this._schemaChunkFetchInProgress = false;
                    this.hideLoader();
                    if (this._schemaTableLayout && this._schemaTableLayout.setAtlasPaginationBusy) {
                        this._schemaTableLayout.setAtlasPaginationBusy(false);
                    }
                    return;
                }
                var existing = this.schemaRowsByRelation[target] || [];
                var offset = existing.length;
                var apiUrl = UrlLinks.relationshipSearchV2ApiUrl();
                var params = {
                    limit: this.getRelationshipApiLimit(),
                    offset: offset,
                    guid: this.guid,
                    disableDefaultSorting: true,
                    excludeDeletedEntities: false,
                    includeSubClassifications: true,
                    includeSubTypes: true,
                    includeClassificationAttributes: true,
                    relation: target,
                    getApproximateCount: false,
                    attributes: ['type', 'position']
                };
                var qs = Utils.serializeRelationshipQueryParams(params);
                var resp = null;
                $.ajax({
                    url: apiUrl + '?' + qs,
                    type: 'GET',
                    dataType: 'json',
                    async: false,
                    success: function(data) {
                        resp = data;
                    }
                }).fail(function() {
                    resp = null;
                });
                if (!resp) {
                    this._schemaChunkFetchInProgress = false;
                    this.hideLoader();
                    if (this._schemaTableLayout && this._schemaTableLayout.setAtlasPaginationBusy) {
                        this._schemaTableLayout.setAtlasPaginationBusy(false);
                    }
                    return;
                }
                var list = resp.entities ? resp.entities : [];
                this.schemaRowsByRelation[target] = existing.concat(list);
                /* Offset > 0: getApproximateCount=false may return -1; keep totals from first page only. */
                if (resp.referredEntities) {
                    this.referredEntities = _.extend({}, this.referredEntities || {}, resp.referredEntities);
                }
                var blocks = _.map(this.schemaRelationNames, function(n) {
                    return that.schemaRowsByRelation[n] || [];
                });
                this.attribute = this.sortSchemaEntityBlocks(blocks);
                var cacheKey = this.guid;
                var cached = schemaTabCacheByGuid[cacheKey];
                if (cached) {
                    cached.rowsByRelation = this.schemaRowsByRelation;
                    cached.totals = this.schemaTotals;
                    cached.mergedAttribute = this.attribute;
                    cached.referredEntities = _.extend({}, cached.referredEntities || {}, this.referredEntities || {});
                    if (this._schemaCanonicalPagination) {
                        cached.paginationCanonical = _.extend({}, this._schemaCanonicalPagination);
                    }
                }
                this.generateTableData(undefined, function() {
                    that._schemaChunkFetchInProgress = false;
                    that.hideLoader();
                    if (that._schemaTableLayout && that._schemaTableLayout.setAtlasPaginationBusy) {
                        that._schemaTableLayout.setAtlasPaginationBusy(false);
                    }
                });
            },
            attachSchemaClientPaginationGuard: function() {
                if (this._schemaPaginationGuardAttached || !this.schemaCollection) {
                    return;
                }
                this._schemaPaginationGuardAttached = true;
                var schemaView = this;
                var col = this.schemaCollection;
                var origGetPage = col.getPage.bind(col);
                col.getPage = function(index, options) {
                    if (this.mode !== 'client') {
                        return origGetPage.call(this, index, options);
                    }
                    var state = this.state;
                    var firstPage = state.firstPage;
                    var currentPage = state.currentPage;
                    var lastPage = state.lastPage;
                    var pageSize = state.pageSize;
                    var pageNum = index;
                    switch (index) {
                        case 'first':
                            pageNum = firstPage;
                            break;
                        case 'prev':
                            pageNum = currentPage - 1;
                            break;
                        case 'next':
                            pageNum = currentPage + 1;
                            break;
                        case 'last':
                            pageNum = lastPage;
                            break;
                        default:
                            pageNum = typeof index === 'number' ? index : parseInt(index, 10);
                    }
                    if (!_.isFinite(pageNum)) {
                        return origGetPage.call(this, index, options);
                    }
                    var pageStart = (firstPage === 0 ? pageNum : pageNum - 1) * pageSize;
                    var needRows = pageStart + pageSize;
                    var guard = 0;
                    var mergedLen = schemaView.getMergedSchemaRowCount();
                    while (mergedLen < needRows && schemaView.schemaHasMore() && guard++ < 80) {
                        var beforeLen = mergedLen;
                        schemaView.fetchNextSchemaChunkSync();
                        mergedLen = schemaView.getMergedSchemaRowCount();
                        if (mergedLen <= beforeLen) {
                            break;
                        }
                    }
                    /* Numeric index: chunk loads reset collection state; avoid 'next'/'prev' using stale currentPage. */
                    return origGetPage.call(this, pageNum, options);
                };
            },
            generateTableData: function(checkedDelete, onTableRenderDone) {
                var that = this,
                    newModel;
                /*
                 * Preserve Page Limit + current page across full rebuild (e.g. classification refresh).
                 * Capture before sync — sync overwrites tl.offset from Pageable state and can
                 * desync from the cached merged list window.
                 */
                var paginationSnap = this.captureSchemaPaginationSnapshot();
                if (this._schemaTableLayout) {
                    this.syncSchemaAtlasTableOffsets();
                }
                this.schemaCollection.fullCollection.reset([]);
                /* Preserve insertion order from this.attribute (position order from API). */
                this.schemaCollection.comparator = null;
                if (this.schemaCollection.fullCollection) {
                    this.schemaCollection.fullCollection.comparator = null;
                }
                if (this.schemaCollection.state) {
                    this.schemaCollection.state.sortKey = null;
                    this.schemaCollection.state.order = null;
                }
                this.activeObj = [];
                this.deleteObj = [];
                this.schemaTableAttribute = null;
                if (this.attribute && this.attribute[0]) {
                    var firstColumn = this.attribute[0],
                        defObj = that.entityDefCollection.fullCollection.find({ name: firstColumn.typeName });
                    if (defObj && defObj.get('options') && defObj.get('options').schemaAttributes) {
                        if (firstColumn) {
                            try {
                                var mapObj = JSON.parse(defObj.get('options').schemaAttributes);
                                that.schemaTableAttribute = _.pick(firstColumn.attributes, mapObj);
                            } catch (e) {}
                        }
                    }
                }
                _.each(this.attribute, function(obj) {
                    if (!Enums.entityStateReadOnly[obj.status]) {
                        that.activeObj.push(obj);
                    } else if (Enums.entityStateReadOnly[obj.status]) {
                        that.deleteObj.push(obj);
                    }
                });
                var histChecked = this.ui.checkDeletedEntity.find("input").prop('checked');
                if (histChecked && this.deleteObj.length === 0) {
                    this.schemaCollection.fullCollection.reset([]);
                } else if (this.activeObj.length === 0 && this.deleteObj.length) {
                    this.ui.checkDeletedEntity.find("input").prop('checked', true);
                    this.schemaCollection.fullCollection.reset(this.deleteObj);
                } else if (histChecked && this.deleteObj.length > 0) {
                    this.schemaCollection.fullCollection.reset(this.activeObj.concat(this.deleteObj));
                } else {
                    this.schemaCollection.fullCollection.reset(this.activeObj);
                }
                if (this.activeObj.length === 0 && this.deleteObj.length === 0) {
                    this.ui.checkDeletedEntity.hide();
                }
                this.attachSchemaClientPaginationGuard();
                var effectiveSnap = this.applySchemaPaginationSnapshot(paginationSnap);
                this._schemaPaginationSnapshot = effectiveSnap;
                this._lastKnownSchemaPagination = effectiveSnap;
                if (effectiveSnap && _.isFinite(effectiveSnap.pageSize) && effectiveSnap.pageSize > 0) {
                    this._schemaCanonicalPagination = {
                        pageSize: effectiveSnap.pageSize,
                        offset: _.isFinite(effectiveSnap.offset)
                            ? Math.max(0, effectiveSnap.offset)
                            : 0
                    };
                    this.persistSchemaCanonicalToCache();
                    this.syncSchemaCollectionQueryParamsToPagination(
                        effectiveSnap.pageSize,
                        effectiveSnap.offset
                    );
                }
                this.renderTableLayoutView(onTableRenderDone);
            },
            showLoader: function() {
                /* Match SearchResultLayoutView — Bootstrap `.show` + SCSS in loader.scss */
                this.$('.fontLoader:not(.for-ignore)').addClass('show');
                this.$('.tableOverlay').addClass('show');
                /* TableLayout spinner uses .for-ignore so collection.request never shows it; schema uses manual push — show explicitly. */
                if (this._schemaTableLayout && this._schemaTableLayout.$) {
                    this._schemaTableLayout.$('div[data-id="r_tableSpinner"]').addClass('show');
                }
            },
            hideLoader: function(argument) {
                this.$('.fontLoader:not(.for-ignore)').removeClass('show');
                this.$('.tableOverlay').removeClass('show');
                if (this._schemaTableLayout && this._schemaTableLayout.$) {
                    this._schemaTableLayout.$('div[data-id="r_tableSpinner"]').removeClass('show');
                }
            },
            renderTableLayoutView: function(onDone) {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var columnCollection = Backgrid.Columns.extend({}),
                        columns = new columnCollection(that.getSchemaTableColumns());
                    var snap = that._schemaPaginationSnapshot ||
                        {
                            pageSize: that.schemaCollection.state.pageSize || 100,
                            offset: 0
                        };
                    var ps = snap.pageSize;
                    var off = _.isFinite(snap.offset) ? Math.max(0, snap.offset) : 0;
                    var table = new TableLayout(_.extend({}, that.commonTableOptions, {
                        columns: columns,
                        clientAtlasPagination: true,
                        atlasShowLoaderBeforeFetch: function() {
                            that.showLoader();
                        },
                        atlasApproximateDatasetTotal: function() {
                            return that.getMergedApproximateTotal();
                        },
                        atlasPaginationOpts: {
                            offset: off,
                            limit: ps,
                            count: ps,
                            fetchCollection: function(opts) {
                                that.handleSchemaAtlasFetch(opts);
                            }
                        }
                    }));
                    that._schemaTableLayout = table;
                    that.RSchemaTableLayoutView.show(table);
                    setTimeout(function() {
                        that.syncSchemaAtlasTableOffsets();
                        that.persistSchemaCanonicalFromTableLayout();
                        if (table.renderAtlasPagination) {
                            table.renderAtlasPagination();
                        }
                        if (_.isFunction(onDone)) {
                            onDone();
                        }
                    }, 0);
                    that.$('.multiSelectTag').hide();
                    Utils.generatePopover({
                        el: that.$('[data-id="showMoreLess"]'),
                        contentClass: 'popover-tag-term',
                        viewFixedPopover: true,
                        popoverOptions: {
                            container: null,
                            content: function() {
                                return $(this).find('.popup-tag-term').children().clone();
                            }
                        }
                    });
                });
            },
            getSchemaTableColumns: function() {
                var that = this,
                    col = {
                        Check: {
                            name: "selected",
                            label: "",
                            cell: "select-row",
                            headerCell: "select-all"
                        }
                    }
                if (this.schemaTableAttribute) {
                    _.each(_.keys(this.schemaTableAttribute), function(key) {
                        if (key !== "position" && key !== "description") {
                            col[key] = {
                                label: key.capitalize(),
                                cell: "html",
                                editable: false,
                                sortable: false,
                                className: "searchTableName",
                                formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                    fromRaw: function(rawValue, model) {
                                        var value = _.escape(model.get('attributes')[key]);
                                        if (key === "name" && model.get('guid')) {
                                            var nameHtml = '<a href="#!/detailPage/' + model.get('guid') + '">' + value + '</a>';
                                            if (model.get('status') && Enums.entityStateReadOnly[model.get('status')]) {
                                                nameHtml += '<button type="button" title="Deleted" class="btn btn-action btn-md deleteBtn"><i class="fa fa-trash"></i></button>';
                                                return '<div class="readOnly readOnlyLink">' + nameHtml + '</div>';
                                            } else {
                                                return nameHtml;
                                            }
                                        } else {
                                            return value
                                        }
                                    }
                                })
                            };
                        }
                    });
                    col['tag'] = {
                        label: "Classifications",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        className: 'searchTag',
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var obj = model.toJSON();
                                if (obj.status && Enums.entityStateReadOnly[obj.status]) {
                                    return '<div class="readOnly">' + CommonViewFunction.tagForTable(obj, that.classificationDefCollection); + '</div>';
                                } else {
                                    return CommonViewFunction.tagForTable(obj, that.classificationDefCollection);
                                }
                            }
                        })
                    };
                    return this.schemaCollection.constructor.getTableCols(col, this.schemaCollection);
                }

            },
            checkedValue: function(e) {
                if (e) {
                    e.stopPropagation();
                }
                var guid = "",
                    that = this,
                    isTagMultiSelect = $(e.currentTarget).hasClass('multiSelectTag');
                if (isTagMultiSelect && this.arr && this.arr.length) {
                    that.addTagModalView(guid, this.arr);
                } else {
                    guid = that.$(e.currentTarget).data("guid");
                    that.addTagModalView(guid);
                }
            },
            addTagModalView: function(guid, multiple) {
                var that = this;
                var tagList = that.schemaCollection.find({ 'guid': guid });
                require(['views/tag/AddTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        guid: guid,
                        multiple: multiple,
                        tagList: _.map((tagList ? tagList.get('classifications') : []), function(obj) {
                            return obj.typeName;
                        }),
                        callback: function() {
                            that.showLoader();
                            var guids = (multiple && multiple.length) ?
                                _.map(multiple, function(x) { return x.id; }) : [guid];
                            guids = _.chain(guids).compact().uniq().value();
                            that.refreshChildEntitiesAfterClassification(guids, function() {
                                that.hideLoader();
                                /* Do not fetch parent entity — child GET + merge updates the row (React parity). */
                                that.arr = [];
                            });
                        },
                        hideLoader: that.hideLoader.bind(that),
                        showLoader: that.showLoader.bind(that),
                        collection: that.classificationDefCollection,
                        enumDefCollection: that.enumDefCollection
                    });
                });
            },
            onClickTagCross: function(e) {
                var that = this,
                    tagName = $(e.target).data("name"),
                    guid = $(e.target).data("guid"),
                    assetName = $(e.target).data("assetname");
                CommonViewFunction.deleteTag({
                    tagName: tagName,
                    guid: guid,
                    msg: "<div class='ellipsis-with-margin'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from <b>" + _.escape(assetName) + " ?</b></div>",
                    titleMessage: Messages.removeTag,
                    okText: "Remove",
                    showLoader: that.showLoader.bind(that),
                    hideLoader: that.hideLoader.bind(that),
                    callback: function() {
                        that.showLoader();
                        that.refreshChildEntitiesAfterClassification([guid], function() {
                            that.hideLoader();
                            /* Do not fetch parent entity — child GET + merge updates the row (React parity). */
                        });
                    }
                });
            },
            /**
             * After toggling "Show historical entities", the grid collection is rebuilt but
             * TableLayout / queryParams / canonical offset could still reflect the previous
             * page (e.g. offset 100 on page 2). Reset to first page before generateTableData
             * so captureSchemaPaginationSnapshot + applySchemaPaginationSnapshot stay in sync.
             */
            resetSchemaPaginationToFirstPageForToggle: function() {
                var col = this.schemaCollection;
                var tl = this._schemaTableLayout;
                var ps = 100;
                if (tl && _.isFinite(tl.limit) && tl.limit > 0) {
                    ps = tl.limit;
                } else if (col && col.state && _.isFinite(col.state.pageSize) && col.state.pageSize > 0) {
                    ps = col.state.pageSize;
                } else if (this._schemaCanonicalPagination &&
                    _.isFinite(this._schemaCanonicalPagination.pageSize) &&
                    this._schemaCanonicalPagination.pageSize > 0) {
                    ps = this._schemaCanonicalPagination.pageSize;
                }
                this._schemaCanonicalPagination = { pageSize: ps, offset: 0 };
                this._lastKnownSchemaPagination = { pageSize: ps, offset: 0 };
                this.syncSchemaCollectionQueryParamsToPagination(ps, 0);
                if (col && col.state) {
                    col.state.pageSize = ps;
                    if (col.state.firstPage === 0) {
                        col.state.currentPage = 0;
                    } else {
                        col.state.currentPage = 1;
                    }
                }
                if (tl) {
                    tl.offset = 0;
                    if (_.isFinite(ps) && ps > 0) {
                        tl.limit = ps;
                    }
                }
                this.persistSchemaCanonicalToCache();
            },
            onCheckDeletedEntity: function() {
                this.resetSchemaPaginationToFirstPageForToggle();
                this.generateTableData();
            }
        });
    return SchemaTableLayoutView;
});