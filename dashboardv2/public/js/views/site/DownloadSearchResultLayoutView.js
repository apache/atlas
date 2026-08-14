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
    'hbs!tmpl/site/DownloadSearchResultLayoutView_tmpl',
    'utils/Utils',
    'utils/UrlLinks',
    'utils/Globals',
    'collection/VDownloadList'
], function(require, Backbone, DownloadSearchResultLayoutViewTmpl, Utils, UrlLinks, Globals, VDownloadList) {
    'use strict';

    var DownloadSearchResultLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends SearchLayoutView */
        {
            _viewName: 'DownloadSearchResultLayoutView',

            template: DownloadSearchResultLayoutViewTmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                downloadsPanel: ".downloads-panel",
                closeDownloadsButton: "[data-id='closeDownloads']",
                downloadListContainer: "[data-id='downloadListContainer']",
                downloadTitle: "[data-id='downloadtitle']",
                refreshDownloadsButton: "[data-id='refreshDownloads']",
                loader: "[data-id='downloadListLoader']",
                toggleDownloads: "[data-id='toggleDownloads']"
            },

            /** ui events hash */
            events: function() {
                var events = {},
                    that = this;
                events['click ' + this.ui.closeDownloadsButton] = "onHideDownloads";
                events['click ' + this.ui.refreshDownloadsButton] = "onRefreshDownloads";
                events['change ' + this.ui.toggleDownloads] = function(e) {
                    this.showAllDownloads = e.currentTarget.checked;
                    this.updateDownloadToggleTooltip();
                    this.generateDownloadList();
                }
                return events;
            },
            /**
             * intialize a new DownloadSearchResultLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                this.options = options;
                this.showDownloads = new VDownloadList();
                this.showAllDownloads = true;
                this.downloadsData = [];
                this.isDownloadsPanelOpen = false;
                this.onDocumentClickBound = _.bind(this.onDocumentClick, this);
                this.bindEvents();
            },
            bindEvents: function() {
                this.listenTo(this.options.exportVent, "downloads:showDownloads", function() {
                    this.onShowDownloads();
                });
            },
            onRender: function() {
                this.ui.toggleDownloads.prop('checked', this.showAllDownloads);
                this.updateDownloadToggleTooltip();
            },
            updateDownloadToggleTooltip: function() {
                var tooltip = this.showAllDownloads
                    ? "Showing all files (including in progress)"
                    : "Showing completed files only";
                this.ui.toggleDownloads.attr("data-original-title", tooltip);
            },
            initializeValues: function() {},
            fetchDownloadsData: function() {
                var that = this,
                    merged = [],
                    pending = 2,
                    hadFailure = false;

                var finishIfDone = function() {
                    pending -= 1;
                    if (pending === 0) {
                        that.downloadsData = merged;
                        that.generateDownloadList();
                        that.hideLoader();
                        if (hadFailure && merged.length === 0) {
                            Utils.notifyError({ content: 'Failed to fetch download records' });
                        }
                    }
                };

                this.showDownloads.getDownloadsList({
                    success: function(data) {
                        var records = (data && data.searchDownloadRecords) || [];
                        _.each(records, function(record) {
                            merged.push(_.extend({}, record, { source: 'search' }));
                        });
                    },
                    error: function() {
                        hadFailure = true;
                    },
                    complete: finishIfDone,
                    reset: true
                });

                this.showDownloads.getGlossaryDownloadsList({
                    success: function(data) {
                        // Backend reuses searchDownloadRecords for glossary export status.
                        var records = (data && (data.glossaryDownloadRecords || data.searchDownloadRecords)) || [];
                        _.each(records, function(record) {
                            merged.push(_.extend({}, record, { source: 'glossary' }));
                        });
                    },
                    error: function() {
                        hadFailure = true;
                    },
                    complete: finishIfDone
                });
            },
            // Keep timestamp parsing/sorting aligned with dashboard/src/utils/downloadRecords.ts
            generateDownloadList: function() {
                var that = this,
                    stateIconEl = "",
                    completedDownloads = "",
                    allDownloads = "",
                    downloadList = "",
                    sortedData = that.sortDownloadRecordsByLatest(this.downloadsData);
                if (sortedData.length) {
                    _.each(sortedData, function(obj) {
                        var downloadUrl = obj.source === 'glossary'
                            ? UrlLinks.glossaryDownloadFileUrl(obj.fileName)
                            : UrlLinks.downloadSearchResultsFileUrl(obj.fileName);
                        if (obj.status === "PENDING") {
                            stateIconEl = "<span class='download-state'><i class='fa fa-refresh fa-spin-custom' aria-hidden='true'></i></span>";
                        } else {
                            stateIconEl = "<span class='download-state'><a href='" + downloadUrl + "'><i class='fa fa-arrow-circle-o-down fa-lg' aria-hidden='true'></i></a></span>";
                            completedDownloads += "<li><i class='fa fa-file-excel-o fa-lg' aria-hidden='true'></i><span class='file-name'>" + obj.fileName + "</span>" + stateIconEl + "</li>";
                        }
                        allDownloads += "<li><i class='fa fa-file-excel-o fa-lg' aria-hidden='true'></i><span class='file-name'>" + obj.fileName + "</span>" + stateIconEl + "</li>";
                    });
                } else {
                    completedDownloads = allDownloads = "<li class='text-center download-list-empty'>No Data Found</li>";
                }

                if (this.downloadsData.length && completedDownloads === "") {
                    completedDownloads = "<li class='text-center download-list-empty'>No Data Found</li>";
                }

                downloadList = this.showAllDownloads ? allDownloads : completedDownloads;
                this.ui.downloadListContainer.empty();
                this.ui.downloadListContainer.html(downloadList);
            },
            onRefreshDownloads: function() {
                var that = this;
                Utils.disableRefreshButton(this.ui.refreshDownloadsButton, this);
                this.showLoader();
                that.fetchDownloadsData();
            },
            onShowDownloads: function() {
                this.fetchDownloadsData();
                this.showLoader();
                this.ui.downloadsPanel.css("right", "20px");
                if (!this.isDownloadsPanelOpen) {
                    this.isDownloadsPanelOpen = true;
                    var that = this;
                    setTimeout(function() {
                        $(document).on('click.downloadsPanel', that.onDocumentClickBound);
                    }, 0);
                }
            },
            onHideDownloads: function() {
                this.ui.downloadsPanel.css("right", "-700px");
                if (this.isDownloadsPanelOpen) {
                    this.isDownloadsPanelOpen = false;
                    $(document).off('click.downloadsPanel');
                }
            },
            onDocumentClick: function(e) {
                var $target = $(e.target);
                if ($target.closest('.downloads-panel').length) {
                    return;
                }
                if ($target.closest('[data-id="showDownloads"]').length) {
                    return;
                }
                this.onHideDownloads();
            },
            onBeforeDestroy: function() {
                $(document).off('click.downloadsPanel');
            },
            showLoader: function() {
                this.$('.downloadListLoader').show();
            },
            hideLoader: function(options) {
                this.$('.downloadListLoader').hide();
            },
            parseCreatedTime: function(createdTime) {
                if (createdTime === undefined || createdTime === null || createdTime === '') {
                    return null;
                }

                if (typeof createdTime === 'number' && isFinite(createdTime)) {
                    return createdTime;
                }

                var parsed = Date.parse(String(createdTime));
                return isNaN(parsed) ? null : parsed;
            },
            parseTimestampFromFileName: function(fileName) {
                if (!fileName) {
                    return null;
                }

                var match = fileName.match(/(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2}(?:\.\d{3})?)/);
                if (!match) {
                    return null;
                }

                var normalized = match[1] + 'T' + match[2] + ':' + match[3] + ':' + match[4];
                var parsed = Date.parse(normalized);
                return isNaN(parsed) ? null : parsed;
            },
            resolveDownloadRecordTime: function(record) {
                var fromApi = this.parseCreatedTime(record.createdTime);
                if (fromApi !== null) {
                    return fromApi;
                }

                var fromFileName = this.parseTimestampFromFileName(record.fileName);
                return fromFileName !== null ? fromFileName : 0;
            },
            sortDownloadRecordsByLatest: function(records) {
                var that = this;
                return _.sortBy(records, function(record) {
                    return -that.resolveDownloadRecordTime(record);
                });
            }
        });
    return DownloadSearchResultLayoutView;
});
