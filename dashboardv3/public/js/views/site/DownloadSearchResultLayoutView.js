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
                    this.showCompletedDownloads = !this.showCompletedDownloads;
                    if(this.showCompletedDownloads){
                        this.ui.toggleDownloads.attr("data-original-title", "Display All Files");
                    } else {
                        this.ui.toggleDownloads.attr("data-original-title", "Display Available Files");
                    }
                    this.genrateDownloadList();
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
                this.showCompletedDownloads = true;
                this.downloadsData = [];
                this.bindEvents();
            },
            bindEvents: function() {
                this.listenTo(this.options.exportVent, "downloads:showDownloads", function() {
                    this.onShowDownloads();
                });
            },
            onRender: function() {
                this.ui.toggleDownloads.attr("data-original-title", "Display All Files");
            },
            initializeValues: function() {},
            fetchDownloadsData: function() {
                var that = this;
                var apiObj = {
                    success: function(data, response) {
                        that.downloadsData = data.searchDownloadRecords;
                        that.genrateDownloadList();
                    },
                    complete: function() {
                        that.hideLoader();
                    },
                    reset: true
                }
                this.showDownloads.getDownloadsList(apiObj);
            },
            genrateDownloadList: function() {
                var that = this,
                    stateIconEl = "",
                    completedDownloads = "",
                    allDownloads = "",
                    downloadList = "",
                    sortedData = _.sortBy(this.downloadsData, function(obj){
                        return obj.createdTime;
                    }).reverse();
                if (sortedData.length) {
                    _.each(sortedData, function(obj) {
                        if (obj.status === "PENDING") {
                            stateIconEl = "<span class='download-state'><i class='fa fa-refresh fa-spin-custom' aria-hidden='true'></i></span>";
                        } else {
                            stateIconEl = "<span class='download-state'><a href=" + UrlLinks.downloadSearchResultsFileUrl(obj.fileName) + "><i class='fa fa-arrow-circle-o-down fa-lg' aria-hidden='true'></i></a></span>";
                            completedDownloads += "<li><i class='fa fa-file-excel-o fa-lg' aria-hidden='true'></i><span class='file-name'>" + obj.fileName + "</span>" + stateIconEl + "</li>"
                        }
                        allDownloads += "<li><i class='fa fa-file-excel-o fa-lg' aria-hidden='true'></i><span class='file-name'>" + obj.fileName + "</span>" + stateIconEl + "</li>";
                    });
                } else {
                    completedDownloads = allDownloads = "<li class='text-center' style='border-bottom:none'>No Data Found</li>"
                }

                if (this.downloadsData.length && completedDownloads === "") {
                    completedDownloads = "<li class='text-center' style='border-bottom:none'>No Data Found</li>";
                }

                downloadList = this.showCompletedDownloads ? completedDownloads : allDownloads;
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
            },
            onHideDownloads: function() {
                this.ui.downloadsPanel.css("right", "-400px")
            },
            showLoader: function() {
                this.$('.downloadListLoader').show();
                this.$('.downloadListOverlay').show();
            },
            hideLoader: function(options) {
                this.$('.downloadListLoader').hide();
                this.$('.downloadListOverlay').hide();
            }
        });
    return DownloadSearchResultLayoutView;
});