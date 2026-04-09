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

define([
    "require",
    "backbone",
    "hbs!tmpl/detail_page/RelationshipCardsLayoutView_tmpl",
    "utils/UrlLinks",
    "utils/Utils"
], function(require, Backbone, RelationshipCardsLayoutViewTmpl, UrlLinks, Utils) {
    "use strict";

    var RelationshipCardsLayoutView = Backbone.Marionette.LayoutView.extend({
        template: RelationshipCardsLayoutViewTmpl,
        ui: {
            container: "[data-id='relationshipCardsContainer']"
        },
        initialize: function(options) {
            _.extend(this, _.pick(options, "entity", "guid", "attributeDefs", "entityDefCollection", "referredEntities", "showEmptyValues", "onDataLoaded", "onDataLoading"));
            this.cardData = {};
            this.cardCounts = {};
            this.loadingByName = {};
            this.resettingByName = {};
            this.searchByName = {};
            this.lastRequestSizeByName = {};
            this.lastTriggerAtByName = {};
            this.lastScrollTopByName = {};
            this.scrollRestoreByName = {};
            this.exhaustedByName = {};
            this.sortByName = {};
            this.showDeletedByName = {};
            this.pageLimitByName = {};
            /** Previous approximate counts per card — used to grow page limit when totals increase (e.g. new term). */
            this._lastCardCountSnapshot = {};
            this.isInitialLoading = false;
            this.hasApiError = false;
        },
        adjustPageLimitWhenTotalGrew: function(name, previousTotal, newTotal) {
            if (!_.isNumber(previousTotal) || !_.isNumber(newTotal) || newTotal <= previousTotal) {
                return false;
            }
            var cur = this.pageLimitByName[name];
            var minL = newTotal <= 1 ? 1 : 2;
            var defLimit = Math.max(minL, 100);
            if (_.isNumber(cur) && cur === previousTotal && previousTotal > 0) {
                this.pageLimitByName[name] = defLimit;
                return true;
            }
            return false;
        },
        getMinPageLimitForTotal: function(totalCount) {
            var c = _.isNumber(totalCount) ? totalCount : parseInt(totalCount, 10);
            if (!_.isFinite(c) || c <= 0) {
                return 1;
            }
            return c <= 1 ? 1 : 2;
        },
        onRender: function() {
            this.fetchInitialCards();
        },
        getRelationNames: function() {
            var relationDefs = [];
            if (this.attributeDefs) {
                relationDefs = this.attributeDefs.relationshipAttributeDefs || this.attributeDefs.relationships || [];
                if (_.isEmpty(relationDefs) && _.isArray(this.attributeDefs)) {
                    relationDefs = _.filter(this.attributeDefs, function(def) {
                        return def && (def.relationshipTypeName || def.relationshipType);
                    });
                }
            }
            
            if (_.isEmpty(relationDefs) && this.entityDefCollection && this.entity && this.entity.typeName) {
                var entityDef = this.entityDefCollection.fullCollection.findWhere({ name: this.entity.typeName });
                
                if (entityDef) {
                    this.attributeDefs = Utils.getNestedSuperTypeObj({
                        data: entityDef.toJSON(),
                        collection: this.entityDefCollection,
                        attrMerge: true,
                        seperateRelatioshipAttr: true
                    });
                    relationDefs = (this.attributeDefs && this.attributeDefs.relationshipAttributeDefs) || [];
                } else {
                    console.warn("[RelationshipCardsLayoutView] EntityDef not found for type:", this.entity.typeName);
                }
            }
            
            // If still empty, try to extract from entity.relationshipAttributes
            if (_.isEmpty(relationDefs) && this.entity && this.entity.relationshipAttributes) {
                var relationNames = _.uniq(_.keys(this.entity.relationshipAttributes));
                return relationNames;
            }
            
            var relationNames = _.uniq(_.compact(_.map(relationDefs, function(def) {
                if (!def) return null;
                return def.name || def.attributeName || (def.attribute && (def.attribute.name || def.attribute.attributeName));
            })));
            
            return relationNames;
        },
        getPageLimit: function(relationName, totalCount) {
            var count = _.isNumber(totalCount) ? totalCount : parseInt(totalCount, 10);
            var defaultLimit = 100;
            var minL = this.getMinPageLimitForTotal(totalCount);
            var currentLimit = this.pageLimitByName[relationName];
            if (!_.isNumber(currentLimit)) {
                return Math.max(minL, defaultLimit);
            }
            if (_.isFinite(count) && currentLimit > count) {
                return count;
            }
            return Math.max(minL, currentLimit);
        },
        getRelationshipParams: function(relationName, offset, totalCount, overrides) {
            var isSorted = _.isBoolean(overrides && overrides.isSorted)
                ? overrides.isSorted
                : !!this.sortByName[relationName];
            var showDeleted = _.isBoolean(overrides && overrides.showDeleted)
                ? overrides.showDeleted
                : !!this.showDeletedByName[relationName];
            var count = _.isNumber(totalCount) ? totalCount : parseInt(totalCount, 10);
            var minForTotal = this.getMinPageLimitForTotal(totalCount);
            var ignoreStored = overrides && overrides.ignoreStoredLimit;
            var limit;
            if (_.isNumber(overrides && overrides.limit)) {
                limit = overrides.limit;
                if (_.isFinite(count) && count > 0) {
                    var maxL = count < 100 ? count : 100;
                    limit = Math.min(maxL, Math.max(minForTotal, limit));
                }
            } else if (ignoreStored) {
                limit = Math.max(minForTotal, 100);
            } else {
                limit = this.getPageLimit(relationName, totalCount);
            }
            return {
                limit: limit,
                offset: offset,
                guid: this.guid,
                sortBy: isSorted ? "name" : undefined,
                sortOrder: isSorted ? "ASCENDING" : undefined,
                disableDefaultSorting: !isSorted,
                excludeDeletedEntities: !showDeleted,
                includeSubClassifications: true,
                includeSubTypes: true,
                includeClassificationAttributes: true,
                relation: relationName,
                getApproximateCount: offset === 0
            };
        },
        fetchRelationshipData: function(relationName, offset, totalCount, overrides) {
            var params = {
                limit: 2,
                offset: offset,
                guid: this.guid,
                sortBy: "name",
                sortOrder: "ASCENDING",
                disableDefaultSorting: true,
                excludeDeletedEntities: true,
                includeSubClassifications: true,
                includeSubTypes: true,
                includeClassificationAttributes: true,
                relation: relationName,
                getApproximateCount: true
            };
            params = this.getRelationshipParams(relationName, offset, totalCount, overrides);
            
            var apiUrl = UrlLinks.relationshipSearchV2ApiUrl();

            return $.ajax({
                url: apiUrl,
                data: params,
                contentType: "application/json",
                dataType: "json",
                type: "GET",
                success: function(response) {
                },
                error: function(xhr, status, error) {
                    console.error("[RelationshipCardsLayoutView] API ERROR for", relationName, ":", {
                        status: status,
                        error: error,
                        responseText: xhr.responseText,
                        statusCode: xhr.status
                    });
                }
            });
        },
        fetchInitialCards: function() {
            var that = this;
            
            if (!this.guid) {
                console.warn("[RelationshipCardsLayoutView] No GUID provided, cannot fetch cards");
                return;
            }
            
            var relationNames = this.getRelationNames();

            if (_.isEmpty(relationNames)) {
                this.renderCards();
                return;
            }

            this.hasApiError = false;
            if (_.isFunction(this.onDataLoading)) {
                this.onDataLoading(true);
            }
            this.renderCards();

            var processResponse = function(name, response) {
                if (!response) {
                    that.cardData[name] = [];
                    that.cardCounts[name] = 0;
                    if (_.isUndefined(that.sortByName[name])) { that.sortByName[name] = false; }
                    if (_.isUndefined(that.showDeletedByName[name])) { that.showDeletedByName[name] = true; }
                    that._lastCardCountSnapshot[name] = 0;
                    return;
                }
                var entities = response.entities ? response.entities : [];
                if (response && response.referredEntities) {
                    that.referredEntities = _.extend({}, that.referredEntities, response.referredEntities);
                }
                that.cardData[name] = _.isArray(entities) ? entities : (entities ? [entities] : []);
                if (!_.isUndefined(response.approximateCount)) {
                    that.cardCounts[name] = response.approximateCount;
                } else if (!_.isUndefined(response.totalCount)) {
                    that.cardCounts[name] = response.totalCount;
                } else {
                    that.cardCounts[name] = that.cardData[name].length;
                }
                if (_.isUndefined(that.sortByName[name])) {
                    that.sortByName[name] = false;
                }
                if (_.isUndefined(that.showDeletedByName[name])) {
                    that.showDeletedByName[name] = true;
                }
                var prevSnap = that._lastCardCountSnapshot[name];
                var didBump = that.adjustPageLimitWhenTotalGrew(name, prevSnap, that.cardCounts[name]);
                that._lastCardCountSnapshot[name] = that.cardCounts[name];
                var listLen = (that.cardData[name] || []).length;
                var totalN = that.cardCounts[name];
                if (didBump && _.isNumber(totalN) && listLen < totalN) {
                    that.exhaustedByName[name] = false;
                    _.defer(function() {
                        if (that.resettingByName[name] || that.loadingByName[name]) {
                            return;
                        }
                        that.resetRelationshipData(name);
                    });
                    return;
                }
                var minClamp = that.getMinPageLimitForTotal(that.cardCounts[name]);
                if (_.isNumber(that.pageLimitByName[name]) && that.pageLimitByName[name] < minClamp) {
                    that.pageLimitByName[name] = minClamp;
                }
                var updatedCount = that.getTotalCountForName(name, that.cardData[name].length);
                if (updatedCount <= that.cardData[name].length) {
                    that.exhaustedByName[name] = true;
                }
            };

            var completedCount = 0;
            var totalCount = relationNames.length;

            _.each(relationNames, function(name) {
                that.fetchRelationshipData(name, 0, undefined, { showDeleted: true })
                    .then(
                        function(response) {
                            processResponse(name, response);
                            that.renderCards();
                            return response;
                        },
                        function(error) {
                            console.error("[RelationshipCardsLayoutView] API error for", name, ":", error);
                            that.hasApiError = true;
                            processResponse(name, null);
                            that.renderCards();
                            return null;
                        }
                    )
                    .always(function() {
                        completedCount += 1;
                        if (completedCount >= totalCount) {
                            if (_.isFunction(that.onDataLoaded)) {
                                var loadedCounts = {};
                                _.each(that.cardData, function(values, key) {
                                    loadedCounts[key] = values ? values.length : 0;
                                });
                                that.onDataLoaded({
                                    data: _.clone(that.cardData),
                                    counts: _.clone(that.cardCounts),
                                    loadedCounts: loadedCounts,
                                    referredEntities: _.clone(that.referredEntities)
                                });
                            }
                            if (_.isFunction(that.onDataLoading)) {
                                that.onDataLoading(false);
                            }
                        }
                    });
            });
        },
        getTotalCountForName: function(name, fallbackCount) {
            var totalCount = this.cardCounts[name];
            var parsed = _.isNumber(totalCount) ? totalCount : parseInt(totalCount, 10);
            if (!_.isFinite(parsed)) {
                return _.isNumber(fallbackCount) ? fallbackCount : 0;
            }
            return parsed;
        },
        handleLoadMore: function(relationName, containerEl) {
            var that = this;
            if (this.loadingByName[relationName]) {
                return;
            }
            if (this.resettingByName[relationName]) {
                return;
            }
            var existing = this.cardData[relationName] || [];
            var totalCount = this.getTotalCountForName(relationName, existing.length);
            if (this.exhaustedByName[relationName] || totalCount <= existing.length) {
                return;
            }
            if (containerEl && containerEl.length) {
                this.scrollRestoreByName[relationName] = {
                    height: containerEl[0].scrollHeight,
                    top: containerEl[0].scrollTop
                };
            }
            this.loadingByName[relationName] = true;
            this.renderCards();
            if (_.isFunction(this.onDataLoading)) {
                this.onDataLoading(true);
            }
            var totalCount = that.cardCounts[relationName];
            this.fetchRelationshipData(relationName, existing.length, totalCount).then(function(response) {
                var entities = response && response.entities ? response.entities : [];
                if (response && response.referredEntities) {
                    that.referredEntities = _.extend({}, that.referredEntities, response.referredEntities);
                }
                var newItems = _.isArray(entities) ? entities : (entities ? [entities] : []);
                that.cardData[relationName] = existing.concat(newItems);
                var updatedCount = that.getTotalCountForName(relationName, that.cardData[relationName].length);
                if (newItems.length === 0 || updatedCount <= that.cardData[relationName].length) {
                    that.exhaustedByName[relationName] = true;
                }
                that.renderCards();
                if (_.isFunction(that.onDataLoaded)) {
                    var loadedCounts = {};
                    _.each(that.cardData, function(values, key) {
                        loadedCounts[key] = values ? values.length : 0;
                    });
                    that.onDataLoaded({
                        data: _.clone(that.cardData),
                        counts: _.clone(that.cardCounts),
                        loadedCounts: loadedCounts,
                        referredEntities: _.clone(that.referredEntities)
                    });
                }
            }).fail(function(error) {
                Utils.notifyError({
                    content: (error && error.responseJSON && error.responseJSON.errorMessage) || "Failed to load relationship data"
                });
            }).always(function() {
                that.loadingByName[relationName] = false;
                if (_.isFunction(that.onDataLoading)) {
                    that.onDataLoading(false);
                }
            });
        },
        resetRelationshipData: function(relationName, overrides) {
            var that = this;
            if (this.loadingByName[relationName]) {
                return;
            }
            this.resettingByName[relationName] = true;
            this.lastRequestSizeByName[relationName] = null;
            this.lastTriggerAtByName[relationName] = 0;
            this.lastScrollTopByName[relationName] = 0;
            this.renderCards();
            var totalCount = this.cardCounts[relationName];
            this.fetchRelationshipData(relationName, 0, totalCount, overrides).then(function(response) {
                var entities = response && response.entities ? response.entities : [];
                if (response && response.referredEntities) {
                    that.referredEntities = _.extend({}, that.referredEntities, response.referredEntities);
                }
                var newItems = _.isArray(entities) ? entities : (entities ? [entities] : []);
                that.cardData[relationName] = newItems;
                if (!_.isUndefined(response.approximateCount)) {
                    that.cardCounts[relationName] = response.approximateCount;
                } else if (!_.isUndefined(response.totalCount)) {
                    that.cardCounts[relationName] = response.totalCount;
                }
                var prevSnapR = that._lastCardCountSnapshot[relationName];
                that.adjustPageLimitWhenTotalGrew(relationName, prevSnapR, that.cardCounts[relationName]);
                that._lastCardCountSnapshot[relationName] = that.cardCounts[relationName];
                var minAfterReset = that.getMinPageLimitForTotal(that.cardCounts[relationName]);
                if (_.isNumber(that.pageLimitByName[relationName]) && that.pageLimitByName[relationName] < minAfterReset) {
                    that.pageLimitByName[relationName] = minAfterReset;
                }
                that.exhaustedByName[relationName] = false;
            }).fail(function(error) {
                Utils.notifyError({
                    content: (error && error.responseJSON && error.responseJSON.errorMessage) || "Failed to load relationship data"
                });
            }).always(function() {
                that.resettingByName[relationName] = false;
                that.renderCards();
            });
        },
        buildCardHtml: function(name, list, totalCount) {
            var that = this;
            if (_.isEmpty(list) && !this.showEmptyValues && !this.resettingByName[name]) {
                return "";
            }
            var relationNames = this.getRelationNames();
            var showTypeNameInDisplay = relationNames.length > 1;
            var count = this.getTotalCountForName(name, list.length);
            var searchQuery = this.searchByName[name] || "";
            var normalizedQuery = searchQuery.trim().toLowerCase();
            var filteredList = normalizedQuery
                ? _.filter(list, function(item) {
                    var ref = item && item.guid ? that.referredEntities && that.referredEntities[item.guid] : null;
                    var displayText = (ref && (ref.displayText || (ref.attributes && ref.attributes.name))) ||
                        item.displayText || (item.attributes && item.attributes.name) || item.qualifiedName || item.guid || "N/A";
                    var qualifiedName = item && item.qualifiedName ? item.qualifiedName : "";
                    var nameValue = item && item.attributes && item.attributes.name ? item.attributes.name : "";
                    var searchTarget = (displayText + " " + qualifiedName + " " + nameValue).toLowerCase();
                    return searchTarget.includes(normalizedQuery);
                })
                : list;
            var itemsHtml = _.map(filteredList, function(item) {
                var ref = item && item.guid ? that.referredEntities && that.referredEntities[item.guid] : null;
                var displayText = (ref && (ref.displayText || (ref.attributes && ref.attributes.name))) ||
                    item.displayText || (item.attributes && item.attributes.name) || item.qualifiedName || item.guid || "N/A";
                var typeName = (ref && ref.typeName) || item.typeName || "";
                var displayLabel = showTypeNameInDisplay && typeName
                    ? displayText + " (" + typeName + ")"
                    : displayText;
                var status = (ref && ref.status) || item.status || (item.attributes && item.attributes.status) || "";
                var isDeleted = status === "DELETED";
                var deletedClass = isDeleted ? " relationship-card-link--deleted" : "";
                var href = item && item.guid ? "#!/detailPage/" + item.guid + "?tabActive=relationship" : "";
                var qualifiedName = item && item.qualifiedName ? item.qualifiedName : "";
                var nameValue = item && item.attributes && item.attributes.name ? item.attributes.name : "";
                var searchText = _.escape((displayText + " " + qualifiedName + " " + nameValue).toLowerCase());
                return "<li class='relationship-card-item' data-search='" + searchText + "'>" +
                    (href ? "<a class='relationship-card-link" + deletedClass + "' href='" + href + "'>" + _.escape(displayLabel) + "</a>" :
                        "<span class='relationship-card-link" + deletedClass + "'>" + _.escape(displayLabel) + "</span>") +
                    "</li>";
            }).join("");

            var canLoadMore = !this.exhaustedByName[name] && count > list.length;
            var showSearch = list.length > 1;
            var rowHeight = 30;
            var bodyPadding = 8;
            var maxVisibleRows = showSearch ? 8 : 9;
            var visibleRows = Math.min(filteredList.length || 0, maxVisibleRows);
            var scrollHeight = filteredList.length
                ? (visibleRows * rowHeight + bodyPadding)
                : 0;
            var isZeroOrOneRecord = list.length <= 1;
            var scrollStyle;
            if (isZeroOrOneRecord && !canLoadMore) {
                scrollStyle = "style='min-height:72px;overflow-y:auto;'";
            } else if (canLoadMore && isZeroOrOneRecord) {
                var capPx = Math.max(72, Math.min(2 * rowHeight + bodyPadding, maxVisibleRows * rowHeight + bodyPadding));
                scrollStyle = "style='max-height:" + capPx + "px;min-height:72px;overflow-y:auto;'";
            } else {
                scrollStyle = "style='max-height:" + scrollHeight + "px; overflow-y: auto;'";
            }
            var emptyClass = list.length ? "" : " relationship-card--empty";
            var noMatchHtml = "<div class='relationship-card-no-match' style='display:none'></div>";
            var loadMoreHtml = canLoadMore
                ? "<div class='relationship-card-load-more'>Scroll to load more data</div>"
                : "";
            var loaderHtml = this.loadingByName[name] && canLoadMore
                ? "<div class='relationship-card-loader'><i class='fa fa-spinner fa-spin'></i></div>"
                : "";
            var resetLoaderHtml = this.resettingByName[name]
                ? "<div class='relationship-card-inline-loader'><i class='fa fa-spinner fa-spin'></i></div>"
                : "";
            if (this.resettingByName[name]) {
                loadMoreHtml = "";
                loaderHtml = "";
            }
            var searchHtml = showSearch
                ? "<div class='relationship-card-search'><input class='relationship-card-search-input' data-name='" + _.escape(name) + "' type='text' placeholder='Search...' value='" + _.escape(searchQuery) + "'/></div>"
                : "";
            var sortIcon = this.sortByName[name] ? "fa-sort-alpha-asc" : "fa-sort";
            var headerActions = "<div class='relationship-card-actions'>" +
                "<button type='button' class='relationship-card-action-button" + (this.sortByName[name] ? " is-active" : "") + "' data-action='toggleSort' data-name='" + _.escape(name) + "' aria-label='Toggle sort by name'>" +
                "<i class='fa " + sortIcon + "'></i></button>" +
                "<label class='relationship-card-toggle'>" +
                "<input type='checkbox' data-action='toggleDeleted' data-name='" + _.escape(name) + "' " + (this.showDeletedByName[name] ? "checked" : "") + " />" +
                "<span>Show deleted</span></label>" +
                "</div>";
            var pageLimit = this.getPageLimit(name, count);
            var minPageLimit = this.getMinPageLimitForTotal(count);
            var footerHtml = "<div class='relationship-card-footer'>" +
                "<div class='relationship-card-footer-row'>" +
                "<span class='relationship-card-count'>Showing " + list.length + " of " + count + "</span>" +
                "<div class='relationship-card-page-limit'>" +
                "<label for='page-limit-" + _.escape(name) + "'>Limit</label>" +
                "<input id='page-limit-" + _.escape(name) + "' class='relationship-card-page-limit-input' type='number' min='" + minPageLimit + "' data-name='" + _.escape(name) + "' value='" + pageLimit + "' />" +
                "</div>" +
                "</div>" +
                "</div>";

            return "<div class='relationship-card panel panel-default" + emptyClass + "' data-name='" + _.escape(name) + "'>" +
                "<div class='relationship-card-header'>" +
                "<div class='relationship-card-title-wrap'><span class='relationship-card-title'>" + _.escape(name) + "</span><span class='relationship-card-count'> (" + count + ")</span></div>" +
                headerActions + "</div>" +
                "<div class='relationship-card-content'>" +
                searchHtml +
                "<div class='relationship-card-scroll" + (list.length ? "" : " relationship-card-scroll--empty") + "' data-name='" + _.escape(name) + "' " + scrollStyle + ">" +
                (this.resettingByName[name] ? resetLoaderHtml : (itemsHtml ? "<ul class='relationship-card-list'>" + itemsHtml + "</ul>" : "<div class='relationship-card-empty'>No records to display</div>")) +
                noMatchHtml +
                loadMoreHtml +
                loaderHtml +
                "</div>" +
                footerHtml +
                "</div>" +
                "</div>";
        },
        /**
         * Computes column layout based on record count (matches React UI):
         * - >5 records: 1 card per column
         * - 1-5 records: 2 cards per column
         * - 0 records: 2 cards per column (when showEmptyValues is true or during loading)
         * @param {string[]} names - list of relation names to use (relationNames when loading, keys(cardData) when loaded)
         */
        computeColumnsFromNames: function(names) {
            if (_.isEmpty(names)) {
                return [];
            }
            var that = this;
            var getCount = function(name) {
                var total = that.cardCounts[name];
                var parsed = _.isNumber(total) ? total : parseInt(total, 10);
                if (!_.isFinite(parsed)) {
                    return (that.cardData[name] || []).length;
                }
                return parsed;
            };
            var largeCards = [];
            var smallCards = [];
            var emptyCards = [];
            _.each(names, function(name) {
                var count = getCount(name);
                if (count > 5) {
                    largeCards.push(name);
                } else if (count >= 1 && count <= 5) {
                    smallCards.push(name);
                } else {
                    emptyCards.push(name);
                }
            });
            var sortNames = function(arr) {
                return arr.sort(function(a, b) { return a.localeCompare(b); });
            };
            sortNames(largeCards);
            sortNames(smallCards);
            sortNames(emptyCards);
            var columns = [];
            _.each(largeCards, function(name) {
                columns.push([name]);
            });
            for (var i = 0; i < smallCards.length; i += 2) {
                columns.push(smallCards.slice(i, i + 2));
            }
            if (this.showEmptyValues || !_.every(names, function(n) { return _.has(that.cardData, n); })) {
                for (var j = 0; j < emptyCards.length; j += 2) {
                    columns.push(emptyCards.slice(j, j + 2));
                }
            }
            return columns;
        },
        buildSkeletonCardHtml: function() {
            return "<div class='relationship-card-skeleton' aria-hidden='true'>" +
                "<div class='relationship-card-skeleton__header'>" +
                "<div class='relationship-card-skeleton__title-wrap'>" +
                "<span class='relationship-card-skeleton__line relationship-card-skeleton__line--title'></span>" +
                "<span class='relationship-card-skeleton__line relationship-card-skeleton__line--count'></span>" +
                "</div>" +
                "<div class='relationship-card-skeleton__actions'>" +
                "<span class='relationship-card-skeleton__box relationship-card-skeleton__box--button'></span>" +
                "<span class='relationship-card-skeleton__box relationship-card-skeleton__box--checkbox'></span>" +
                "</div>" +
                "</div>" +
                "<div class='relationship-card-skeleton__content'>" +
                "<div class='relationship-card-skeleton__body'>" +
                "<span class='relationship-card-skeleton__line'></span>" +
                "<span class='relationship-card-skeleton__line'></span>" +
                "<span class='relationship-card-skeleton__line'></span>" +
                "<span class='relationship-card-skeleton__line relationship-card-skeleton__line--short'></span>" +
                "</div>" +
                "<div class='relationship-card-skeleton__footer'>" +
                "<span class='relationship-card-skeleton__line relationship-card-skeleton__line--footer'></span>" +
                "<span class='relationship-card-skeleton__box relationship-card-skeleton__box--input'></span>" +
                "</div>" +
                "</div>" +
                "</div>";
        },
        renderCards: function() {
            var that = this;
            var relationNames = this.getRelationNames();

            if (_.isEmpty(relationNames)) {
                this.$el.html("<div class='relationship-cards-loader'><i class='fa fa-spinner fa-spin'></i></div>");
                return;
            }

            var allLoaded = _.every(relationNames, function(name) {
                return _.has(that.cardData, name);
            });
            var namesForColumns = allLoaded ? _.keys(this.cardData) : relationNames;

            var columns = this.computeColumnsFromNames(namesForColumns);
            var html = "<div class='relationship-cards-grid relationship-cards-grid--custom'>";
            var cardCount = 0;
            _.each(columns, function(columnNames) {
                html += "<div class='relationship-cards-column'>";
                _.each(columnNames, function(name) {
                    var hasData = _.has(that.cardData, name);
                    if (hasData) {
                        var list = that.cardData[name] || [];
                        var cardHtml = that.buildCardHtml(name, list, that.cardCounts[name]);
                        if (cardHtml) {
                            html += cardHtml;
                            cardCount++;
                        }
                    } else {
                        html += that.buildSkeletonCardHtml();
                    }
                });
                html += "</div>";
            });
            html += "</div>";

            var allLoadedAndEmpty = _.every(relationNames, function(n) {
                return _.has(that.cardData, n) && _.isEmpty(that.cardData[n]);
            });
            if (cardCount === 0 && allLoadedAndEmpty && !this.showEmptyValues) {
                var emptyMsg = this.hasApiError ? "Failed to load relationship data" : "No relationship data available";
                html = "<div class='relationship-cards-empty'>" + emptyMsg + "</div>";
            }
            
            if (this.$el && this.$el.length) {
                this.$el.html(html);
                this.bindCardEvents();
            } else {
                console.warn("[RelationshipCardsLayoutView] $el not available, cannot render cards");
            }
        }
    });

    RelationshipCardsLayoutView.prototype.bindCardEvents = function() {
        var that = this;
        this.$el.find(".relationship-card-action-button")
            .off("click")
            .on("click", function(e) {
                e.preventDefault();
                var name = $(e.currentTarget).data("name");
                if (!name || that.resettingByName[name]) {
                    return;
                }
                var nextSorted = !that.sortByName[name];
                that.sortByName[name] = nextSorted;
                that.resetRelationshipData(name, { isSorted: nextSorted });
            });

        this.$el.find("input[data-action='toggleDeleted']")
            .off("change")
            .on("change", function(e) {
                var name = $(e.currentTarget).data("name");
                if (!name || that.resettingByName[name]) {
                    return;
                }
                var isChecked = $(e.currentTarget).is(":checked");
                that.showDeletedByName[name] = isChecked;
                that.resetRelationshipData(name, { showDeleted: isChecked });
            });

        this.$el.find(".relationship-card-page-limit-input")
            .off("keydown change")
            .on("change", function(e) {
                var name = $(e.currentTarget).data("name");
                if (!name) {
                    return;
                }
                var rawValue = $(e.currentTarget).val();
                var parsed = parseInt(rawValue, 10);
                if (!_.isFinite(parsed)) {
                    that.pageLimitByName[name] = 0;
                    return;
                }
                that.pageLimitByName[name] = parsed;
            })
            .on("keydown", function(e) {
                if (e.key !== "Enter") {
                    return;
                }
                var name = $(e.currentTarget).data("name");
                if (!name || that.resettingByName[name]) {
                    return;
                }
                var count = that.getTotalCountForName(name, 0);
                var minL = that.getMinPageLimitForTotal(count);
                var rawValue = $(e.currentTarget).val();
                var parsed = parseInt(String(rawValue).trim(), 10);
                var nextLimit = _.isFinite(parsed) && parsed > 0 ? parsed : 100;
                if (count > 0) {
                    nextLimit = Math.max(minL, nextLimit);
                    if (nextLimit > count) {
                        nextLimit = count;
                    }
                } else {
                    nextLimit = Math.max(1, nextLimit);
                }
                that.pageLimitByName[name] = nextLimit;
                that.resetRelationshipData(name, { limit: nextLimit });
            });

        this.$el.find(".relationship-card-search-input")
            .off("keyup keypress keydown")
            .on("keyup keypress", function(e) {
                var name = $(e.currentTarget).data("name");
                that.searchByName[name] = $(e.currentTarget).val() || "";
                that.applySearchFilter(name);
            })
            .on("keydown", function(e) {
                if (e.key === "Escape") {
                    var name = $(e.currentTarget).data("name");
                    that.searchByName[name] = "";
                    $(e.currentTarget).val("");
                    that.applySearchFilter(name);
                }
            });

        this.$el.find(".relationship-card-scroll")
            .off("scroll")
            .on("scroll", function(e) {
                var container = $(e.currentTarget);
                var name = container.data("name");
                
                if (!name) {
                    console.warn("[RelationshipCardsLayoutView] No name found on scroll container");
                    return;
                }
                
                if (that.loadingByName[name]) {
                    return;
                }
                if (that.resettingByName[name]) {
                    return;
                }
                if (that.exhaustedByName[name]) {
                    return;
                }
                
                var list = that.cardData[name] || [];
                var count = that.getTotalCountForName(name, list.length);
                var canLoadMore = count > list.length;
                
                if (!canLoadMore) {
                    return;
                }
                
                var now = Date.now();
                var distanceFromBottom = container[0].scrollHeight - container[0].scrollTop - container[0].clientHeight;
                var isNearBottom = distanceFromBottom <= 10;
                var isScrollingDown = container[0].scrollTop > (that.lastScrollTopByName[name] || 0);
                
                that.lastScrollTopByName[name] = container[0].scrollTop;
                
                if (!isNearBottom || !isScrollingDown) {
                    return;
                }
                
                var timeSinceLastTrigger = now - (that.lastTriggerAtByName[name] || 0);
                if (timeSinceLastTrigger < 400) {
                    return;
                }
                
                if (that.lastRequestSizeByName[name] === list.length) {
                    return;
                }
                
                that.lastRequestSizeByName[name] = list.length;
                that.lastTriggerAtByName[name] = now;
                that.handleLoadMore(name, container);
            });

        _.each(this.scrollRestoreByName, function(scroll, name) {
            var container = that.$el.find(".relationship-card-scroll[data-name='" + name + "']");
            if (container.length && scroll) {
                var nextTop = container[0].scrollHeight - scroll.height + scroll.top;
                container[0].scrollTop = nextTop;
            }
        });
        this.scrollRestoreByName = {};

        _.each(this.searchByName, function(_value, name) {
            that.applySearchFilter(name);
        });
    };

    RelationshipCardsLayoutView.prototype.applySearchFilter = function(name) {
        var rawQuery = (this.searchByName[name] || "").trim();
        var query = rawQuery.toLowerCase();
        var card = this.$el.find(".relationship-card[data-name='" + name + "']");
        if (!card.length) {
            return;
        }
        var items = card.find(".relationship-card-item");
        var noMatch = card.find(".relationship-card-no-match");
        if (!query) {
            items.show();
            noMatch.hide();
            return;
        }
        var visibleCount = 0;
        items.each(function() {
            var searchText = ($(this).data("search") || "").toString();
            if (searchText.includes(query)) {
                $(this).show();
                visibleCount += 1;
            } else {
                $(this).hide();
            }
        });
        if (visibleCount === 0) {
            noMatch.text("No record found for \"" + rawQuery + "\"").show();
        } else {
            noMatch.hide();
        }
    };

    return RelationshipCardsLayoutView;
});
