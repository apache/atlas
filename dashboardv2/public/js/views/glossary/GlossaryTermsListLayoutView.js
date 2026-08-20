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
    'hbs!tmpl/glossary/GlossaryTermsListLayoutView_tmpl',
    'utils/GlossaryExport',
    'utils/CommonViewFunction',
    'utils/Enums',
    'utils/Messages',
    'utils/Utils'
], function(require, Backbone, GlossaryTermsListLayoutViewTmpl, GlossaryExport, CommonViewFunction, Enums, Messages, Utils) {
    'use strict';

    var FILTER_DEBOUNCE_MS = 400;

    var GlossaryTermsListLayoutView = Backbone.Marionette.LayoutView.extend({
        _viewName: 'GlossaryTermsListLayoutView',

        template: GlossaryTermsListLayoutViewTmpl,

        regions: {
            RGlossaryTermsTableLayoutView: '#r_glossaryTermsTableLayoutView'
        },

        ui: {
            refreshButton: "[data-id='refreshGlossaryTermsList']",
            downloadButton: "[data-id='downloadGlossaryTermsList']",
            downloadCsv: "[data-id='downloadGlossaryCsv']",
            downloadXlsx: "[data-id='downloadGlossaryXlsx']",
            filterGlossary: "[data-id='filterGlossary']",
            filterRecordType: "[data-id='filterRecordType']",
            filterStatus: "[data-id='filterStatus']",
            filterSearchText: "[data-id='filterSearchText']",
            tagClick: '[data-id="tagClick"]',
            addTag: '[data-id="addTag"]'
        },

        events: function() {
            var events = {},
                that = this;
            events['click ' + this.ui.refreshButton] = 'onRefresh';
            events['click ' + this.ui.downloadCsv] = 'onDownloadCsv';
            events['click ' + this.ui.downloadXlsx] = 'onDownloadXlsx';
            events['change ' + this.ui.filterGlossary] = 'onFilterChange';
            events['change ' + this.ui.filterRecordType] = 'onFilterChange';
            events['change ' + this.ui.filterStatus] = 'onFilterChange';
            events['keyup ' + this.ui.filterSearchText] = 'onSearchTextChange';
            events['click ' + this.ui.tagClick] = function(e) {
                var scope = $(e.currentTarget);
                if (e.target.nodeName.toLocaleLowerCase() === 'i') {
                    that.onClickTagCross(e);
                } else {
                    Utils.setUrl({
                        url: '#!/tag/tagAttribute/' + scope.text().split('@')[0],
                        mergeBrowserUrl: false,
                        trigger: true
                    });
                }
            };
            events['click ' + this.ui.addTag] = 'onClickAddTag';
            return events;
        },

        initialize: function(options) {
            _.extend(this, _.pick(options, 'glossaryCollection', 'classificationDefCollection', 'enumDefCollection', 'searchVent'));
            this.filters = GlossaryExport.createDefaultFilters();
            this.limit = GlossaryExport.SEARCH_DEFAULTS.limit;
            this.offset = 0;
            this.totalCount = 0;
            this.isDownloading = false;
            this.isFetching = false;
            this.fetchSequence = 0;
            this.sortBy = GlossaryExport.SEARCH_DEFAULTS.sortBy;
            this.sortOrder = GlossaryExport.SEARCH_DEFAULTS.sortOrder;
            this.debounceTimer = null;
            this.tableCollection = new Backbone.Collection();
            this.tableCollection.state = {
                totalRecords: 0,
                pageSize: this.limit,
                currentPage: 0
            };
            this.tableCollection.queryParams = {
                limit: this.limit,
                offset: this.offset
            };
            this.commonTableOptions = {
                collection: this.tableCollection,
                clientAtlasPagination: true,
                includePagination: false,
                includeAtlasPagination: true,
                includeFooterRecords: true,
                includeColumnManager: false,
                includeOrderAbleColumns: false,
                includeSizeAbleColumns: false,
                includeTableLoader: true,
                includeAtlasPageSize: true,
                includeAtlasTableSorting: false,
                atlasShowLoaderBeforeFetch: this.showFetchLoader.bind(this),
                atlasPaginationOpts: {
                    limit: this.limit,
                    offset: this.offset,
                    fetchCollection: this.fetchCollection.bind(this),
                    atlasApproximateDatasetTotal: this.getApproximateTotal.bind(this)
                },
                gridOpts: {
                    emptyText: 'No rows match the current filters.',
                    className: 'table table-hover backgrid table-quickMenu colSort'
                },
                filterOpts: {},
                paginatorOpts: {}
            };
        },

        onRender: function() {
            var that = this;
            if (this.glossaryCollection && !this.glossaryCollection.fullCollection.length) {
                this.glossaryCollection.fetch({
                    reset: true,
                    success: function() {
                        that.populateGlossaryFilter();
                    }
                });
            } else {
                this.populateGlossaryFilter();
            }
            this.renderTableLayoutView();
        },

        getApproximateTotal: function() {
            return this.totalCount;
        },

        getGlossaryGuidByName: function() {
            return GlossaryExport.buildGlossaryGuidByName(this.glossaryCollection);
        },

        populateGlossaryFilter: function() {
            var $select = this.ui.filterGlossary;
            $select.find('option:not(:first)').remove();
            if (!this.glossaryCollection || !this.glossaryCollection.fullCollection) {
                return;
            }
            var names = this.glossaryCollection.fullCollection.map(function(model) {
                return model.get('name');
            });
            names = _.sortBy(_.compact(names), function(name) {
                return name.toLowerCase();
            });
            _.each(names, function(name) {
                $select.append($('<option></option>').attr('value', name).text(name));
            });
        },

        readFiltersFromUi: function() {
            this.filters.glossaryName = this.ui.filterGlossary.val() || '';
            this.filters.recordType = this.ui.filterRecordType.val() || 'all';
            this.filters.statusFilter = this.ui.filterStatus.val() || '';
            this.filters.searchText = this.ui.filterSearchText.val() || '';
        },

        onFilterChange: function() {
            this.readFiltersFromUi();
            this.offset = 0;
            this.fetchCollection({ fromUrl: true, filterChange: true });
        },

        onSearchTextChange: function() {
            var that = this;
            this.readFiltersFromUi();
            if (this.debounceTimer) {
                clearTimeout(this.debounceTimer);
            }
            this.debounceTimer = setTimeout(function() {
                that.offset = 0;
                that.fetchCollection({ fromUrl: true, filterChange: true });
            }, FILTER_DEBOUNCE_MS);
        },

        onRefresh: function() {
            this.fetchCollection({ fromUrl: true, refresh: true });
        },

        mapColumnToSortBy: function(columnName) {
            var sortMap = {
                glossaryName: 'glossaryName',
                name: 'name',
                recordType: 'recordType',
                status: 'status'
            };
            return sortMap[columnName] || 'name';
        },

        createSortableHeaderCell: function() {
            var that = this;
            return Backgrid.HeaderCell.extend({
                onClick: function(e) {
                    e.preventDefault();
                    var column = this.column;
                    if (!column.get('sortable')) {
                        return;
                    }
                    var columnName = column.get('name');
                    var direction = 'ascending';
                    if (that.sortBy === that.mapColumnToSortBy(columnName) && that.sortOrder === 'ASCENDING') {
                        direction = 'descending';
                    }
                    that.sortBy = that.mapColumnToSortBy(columnName);
                    that.sortOrder = direction === 'ascending' ? 'ASCENDING' : 'DESCENDING';
                    that.offset = 0;
                    if (that.tableLayout && that.tableLayout.columns) {
                        that.tableLayout.columns.each(function(col) {
                            col.set('direction', col.get('name') === columnName ? direction : null);
                        });
                    }
                    that.fetchCollection({ fromUrl: true, sortChange: true });
                }
            });
        },

        showFetchLoader: function(options) {
            options = options || {};
            this.isFetching = true;
            this.$('.glossary-terms-filters').addClass('is-loading');
            if (this.tableLayout) {
                if (this.tableLayout.setAtlasPaginationBusy) {
                    var navHint = options.next ? 'next' : (options.previous ? 'prev' : 'both');
                    this.tableLayout.setAtlasPaginationBusy(true, navHint);
                }
                this.tableLayout.$('div[data-id="r_tableSpinner"]').addClass('show');
            }
            this.ui.filterGlossary.prop('disabled', true);
            this.ui.filterRecordType.prop('disabled', true);
            this.ui.filterStatus.prop('disabled', true);
            this.ui.filterSearchText.prop('disabled', true);
            this.ui.refreshButton.attr('disabled', true);
            this.ui.downloadButton.attr('disabled', true);
        },

        hideFetchLoader: function() {
            this.isFetching = false;
            this.$('.glossary-terms-filters').removeClass('is-loading');
            if (this.tableLayout) {
                this.tableLayout.$('div[data-id="r_tableSpinner"]').removeClass('show');
                if (this.tableLayout.setAtlasPaginationBusy) {
                    this.tableLayout.setAtlasPaginationBusy(false);
                }
            }
            this.ui.filterGlossary.prop('disabled', false);
            this.ui.filterRecordType.prop('disabled', false);
            this.ui.filterStatus.prop('disabled', false);
            this.ui.filterSearchText.prop('disabled', false);
            this.ui.refreshButton.attr('disabled', false);
            this.ui.downloadButton.attr('disabled', false);
        },

        completeFetch: function(result, options, fetchId) {
            if (fetchId !== this.fetchSequence) {
                return;
            }
            this.totalCount = result.totalCount;
            this.tableCollection.state.totalRecords = result.totalCount;
            this.tableCollection.state.pageSize = this.limit;
            this.tableCollection.state.currentPage = Math.floor(this.offset / this.limit);
            this.tableCollection.reset(result.rows);
            this.tableCollection.trigger('sync');
            this.ui.downloadButton.attr('disabled', this.totalCount === 0 || this.isDownloading);
            this.hideFetchLoader();
            if (this.tableLayout) {
                this.tableLayout.trigger('grid:refresh');
                if (this.tableLayout.renderAtlasPagination) {
                    this.tableLayout.renderAtlasPagination(options || {});
                }
            }
        },

        failFetch: function(xhr, fetchId) {
            if (fetchId !== this.fetchSequence) {
                return;
            }
            this.totalCount = 0;
            this.tableCollection.state.totalRecords = 0;
            this.tableCollection.reset([]);
            this.tableCollection.trigger('error');
            this.ui.downloadButton.attr('disabled', true);
            this.hideFetchLoader();
            var message = 'Failed to load glossary terms';
            if (xhr && xhr.responseJSON && xhr.responseJSON.errorMessage) {
                message = xhr.responseJSON.errorMessage;
            }
            Utils.notifyError({ content: message });
        },

        onDownloadCsv: function(e) {
            if (e) {
                e.preventDefault();
            }
            this.onDownload('CSV');
        },

        onDownloadXlsx: function(e) {
            if (e) {
                e.preventDefault();
            }
            this.onDownload('XLSX');
        },

        onDownload: function(format) {
            var that = this;
            if (this.isDownloading || this.totalCount === 0) {
                return;
            }
            this.isDownloading = true;
            this.ui.downloadButton.attr('disabled', true);
            GlossaryExport.requestExportDownload(this.filters, this.getGlossaryGuidByName(), format || 'CSV').done(function() {
                Utils.notifySuccess({
                    content: 'The current glossary export has been enqueued for download. You can access the file by clicking the download icon at the top of the page.'
                });
            }).fail(function(xhr) {
                var message = 'Glossary export failed';
                if (xhr && xhr.responseJSON && xhr.responseJSON.errorMessage) {
                    message = xhr.responseJSON.errorMessage;
                }
                Utils.notifyError({ content: message });
            }).always(function() {
                that.isDownloading = false;
                that.ui.downloadButton.attr('disabled', that.totalCount === 0);
            });
        },

        getTagList: function(guid) {
            var model = this.tableCollection.find(function(item) {
                return item.get('guid') === guid;
            });
            if (!model) {
                return [];
            }
            var obj = model.toJSON();
            return _.compact(_.map(obj.classifications, function(val) {
                if (val.entityGuid === guid) {
                    return val.typeName;
                }
            }));
        },

        addTagModalView: function(guid) {
            var that = this;
            require(['views/tag/AddTagModalView'], function(AddTagModalView) {
                var view = new AddTagModalView({
                    guid: guid,
                    callback: function() {
                        that.fetchCollection({ fromUrl: true, refresh: true });
                        if (that.searchVent) {
                            that.searchVent.trigger('entityList:refresh');
                        }
                    },
                    tagList: that.getTagList(guid),
                    showLoader: that.showFetchLoader.bind(that),
                    hideLoader: that.hideFetchLoader.bind(that),
                    collection: that.classificationDefCollection,
                    enumDefCollection: that.enumDefCollection
                });
            });
        },

        onClickAddTag: function(e) {
            var guid = this.$(e.currentTarget).data('guid');
            if (guid) {
                this.addTagModalView(guid);
            }
        },

        onClickTagCross: function(e) {
            var that = this,
                tagName = $(e.target).data('name'),
                guid = $(e.target).data('guid'),
                entityGuid = $(e.target).data('entityguid'),
                assetName = $(e.target).data('assetname');
            CommonViewFunction.deleteTag({
                tagName: tagName,
                guid: guid,
                associatedGuid: guid !== entityGuid ? entityGuid : null,
                msg: "<div class='ellipsis-with-margin'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from <b>" + _.escape(assetName) + " ?</b></div>",
                titleMessage: Messages.removeTag,
                okText: 'Remove',
                showLoader: that.showFetchLoader.bind(that),
                hideLoader: that.hideFetchLoader.bind(that),
                callback: function() {
                    that.fetchCollection({ fromUrl: true, refresh: true });
                    if (that.searchVent) {
                        that.searchVent.trigger('entityList:refresh');
                    }
                }
            });
        },

        formatClassificationsCell: function(model) {
            var row = model.toJSON();
            if (row.recordType !== 'Term' || !row.guid || !this.classificationDefCollection) {
                return GlossaryExport.formatClassificationsHtml(row.classifications);
            }
            var obj = {
                guid: row.guid,
                status: row.status,
                classifications: row.classifications || []
            };
            if (obj.status && Enums.entityStateReadOnly[obj.status]) {
                return '<div class="readOnly">' + CommonViewFunction.tagForTable(obj, this.classificationDefCollection) + '</div>';
            }
            return CommonViewFunction.tagForTable(obj, this.classificationDefCollection);
        },

        fetchCollection: function(options) {
            var that = this;
            options = options || {};
            this.fetchSequence += 1;
            var fetchId = this.fetchSequence;
            this.showFetchLoader(options);
            this.readFiltersFromUi();
            if (_.isFinite(options.offset)) {
                this.offset = options.offset;
            } else if (this.tableLayout) {
                this.offset = this.tableLayout.offset || this.offset;
                this.limit = this.tableLayout.limit || this.limit;
            }
            if (options.filterChange || options.sortChange || options.refresh) {
                this.offset = options.offset || 0;
                if (this.tableLayout) {
                    this.tableLayout.offset = this.offset;
                }
            }
            var pageIndex = Math.floor(this.offset / this.limit);
            var request = GlossaryExport.buildSearchRequest(
                this.filters,
                pageIndex,
                this.limit,
                this.getGlossaryGuidByName(),
                {
                    sortBy: this.sortBy,
                    sortOrder: this.sortOrder
                }
            );
            this.tableCollection.trigger('request');
            GlossaryExport.fetchSearchPage(request).done(function(result) {
                that.completeFetch(result, options, fetchId);
            }).fail(function(xhr) {
                that.failFetch(xhr, fetchId);
            });
        },

        renderTableLayoutView: function() {
            var that = this;
            if (this._tableLayoutLoading) {
                return;
            }
            this._tableLayoutLoading = true;
            require(['utils/TableLayout'], function(TableLayout) {
                var columns = new Backgrid.Columns(that.getColumns());
                that.tableLayout = new TableLayout(_.extend({}, that.commonTableOptions, {
                    columns: columns
                }));
                that.RGlossaryTermsTableLayoutView.show(that.tableLayout);
                _.extend(that.tableCollection.queryParams, {
                    limit: that.limit,
                    offset: that.offset
                });
                that._tableLayoutLoading = false;
                that.fetchCollection({ fromUrl: true });
            });
        },

        getColumns: function() {
            var that = this;
            var sortableHeaderCell = this.createSortableHeaderCell();
            var colDefs = {
                glossaryName: {
                    label: 'Glossary Name',
                    cell: 'string',
                    editable: false,
                    sortable: true,
                    headerCell: sortableHeaderCell,
                    className: 'searchTableName'
                },
                name: {
                    label: 'Name',
                    cell: 'html',
                    editable: false,
                    sortable: true,
                    headerCell: sortableHeaderCell,
                    className: 'searchTableName',
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            return GlossaryExport.formatNameHtml(model.toJSON());
                        }
                    })
                },
                recordType: {
                    label: 'Record Type',
                    cell: 'string',
                    editable: false,
                    sortable: true,
                    headerCell: sortableHeaderCell
                },
                shortDescription: {
                    label: 'Short Description',
                    cell: 'string',
                    editable: false,
                    sortable: false,
                    className: 'searchTableName'
                },
                longDescription: {
                    label: 'Long Description',
                    cell: 'string',
                    editable: false,
                    sortable: false,
                    className: 'searchTableName'
                },
                classifications: {
                    label: 'Classifications',
                    cell: 'html',
                    editable: false,
                    sortable: false,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            return that.formatClassificationsCell(model);
                        }
                    })
                },
                customAttributes: {
                    label: 'Custom Attributes',
                    cell: 'html',
                    editable: false,
                    sortable: false,
                    className: 'searchTableName',
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue, model) {
                            var attrs = model.get('customAttributes');
                            var display = GlossaryExport.formatCustomAttributesForDisplay(attrs);
                            if (!display) {
                                return '';
                            }
                            var tooltip = _.escape(GlossaryExport.formatCustomAttributesForTooltip(attrs));
                            return '<span class="entity-name" title="' + tooltip + '">' + _.escape(display) + '</span>';
                        }
                    })
                },
                status: {
                    label: 'Status',
                    cell: 'html',
                    editable: false,
                    sortable: true,
                    headerCell: sortableHeaderCell,
                    formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                        fromRaw: function(rawValue) {
                            return GlossaryExport.formatStatusHtml(rawValue);
                        }
                    })
                }
            };
            return _.map(colDefs, function(def, key) {
                return _.extend({ name: key }, def);
            });
        }
    });

    return GlossaryTermsListLayoutView;
});
