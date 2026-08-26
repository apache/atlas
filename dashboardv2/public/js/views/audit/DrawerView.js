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
    'hbs!tmpl/audit/DrawerView_tmpl',
    'utils/Utils'
], function (require, Backbone, DrawerView_tmpl, Utils) {
    'use strict';

    var DrawerView = Backbone.Marionette.LayoutView.extend({
        _viewName: 'DrawerView',
        template: DrawerView_tmpl,

        ui: {
            overlay: ".drawer-overlay",
            panel: ".drawer-panel",
            closeBtn: "[data-id='closeDrawer']",
            searchInput: "[data-id='searchInput']",
            drawerList: "[data-id='drawerList']",
            drawerItemsList: "[data-id='drawerList']",
            drawerScrollRegion: "[data-id='drawerScrollRegion']",
            drawerEmpty: "[data-id='drawerEmpty']",
            limitInput: "[data-id='limitInput']",
            showingText: "[data-id='showingText']",
            copyRunId: "[data-id='copyRunId']",
            drawerLoading: "[data-id='drawerLoading']",
            drawerObserver: "[data-id='drawerObserver']",
            btnFirstPage: "[data-id='btnFirstPage']",
            btnPrevPage: "[data-id='btnPrevPage']",
            btnCurrentPage: "[data-id='btnCurrentPage']",
            btnNextPage: "[data-id='btnNextPage']",
            btnLastPage: "[data-id='btnLastPage']"
        },

        events: function () {
            var events = {};
            events["click " + this.ui.closeBtn] = "closeDrawer";
            events["click " + this.ui.overlay] = "closeDrawer";
            events["keyup " + this.ui.searchInput] = "onSearch";
            events["click .blue-link"] = "onItemClick";
            events["click " + this.ui.copyRunId] = "onCopyRunId";
            events["click " + this.ui.btnFirstPage] = "goToFirstPage";
            events["click " + this.ui.btnPrevPage] = "goToPrevPage";
            events["click " + this.ui.btnNextPage] = "goToNextPage";
            events["click " + this.ui.btnLastPage] = "goToLastPage";
            events["keyup " + this.ui.limitInput] = "onLimitInput";
            return events;
        },

        initialize: function (options) {
            _.extend(this, _.pick(options, 'title', 'items', 'fetchData', 'onItemClickCb', 'actionType', 'runId', 'totalCount'));
            this.drawerPageSize = 25;
            this.limit = 25; // fallback
            this.searchText = "";
            this.displayItems = [];
            this.offset = 0;
            this.page = 1;
            this.hasMore = true;
            this.isLoading = false;
            this.scrollTop = 0;
            this.totalFilteredCount = 0;
        },

        onRender: function () {
            var that = this;
            $('body').append(this.$el);

            setTimeout(function () {
                that.ui.overlay.addClass('open');
                that.ui.panel.addClass('open');
                $('body').addClass('drawer-open-lock');
            }, 10);

            // Scroll events don't bubble, so we must bind directly to the UI element
            this.ui.drawerScrollRegion.on('scroll', function () {
                var el = this;
                that.scrollTop = el.scrollTop;
                that.renderList(); // re-render the visible chunk
            });

            this.loadData();
        },

        onBeforeDestroy: function () {
            if (this.ui && this.ui.drawerScrollRegion) {
                this.ui.drawerScrollRegion.off('scroll');
            }
        },

        loadData: function () {
            this.page = 1;
            this.displayItems = [];

            if (this.fetchData) {
                // If there's an API, load it all or handle server pagination. 
                // For this UI port, we assume we load what's needed.
                var that = this;
                this.isLoading = true;
                this.ui.drawerLoading.show();
                this.fetchData(10000, 0, function (data) {
                    that.isLoading = false;
                    that.ui.drawerLoading.hide();
                    if (data && data.length > 0) {
                        that.items = data;
                    }
                    that.renderList();
                });
            } else {
                this.renderList();
            }
        },

        goToFirstPage: function () {
            this.page = 1;
            this.scrollTop = 0;
            this.renderList();
        },

        goToPrevPage: function () {
            if (this.page > 1) {
                this.page -= 1;
                this.scrollTop = 0;
                this.renderList();
            }
        },

        goToNextPage: function () {
            var maxPage = Math.ceil(this.totalFilteredCount / this.drawerPageSize) || 1;
            if (this.page < maxPage) {
                this.page += 1;
                this.scrollTop = 0;
                this.renderList();
            }
        },

        goToLastPage: function () {
            var maxPage = Math.ceil(this.totalFilteredCount / this.drawerPageSize) || 1;
            this.page = maxPage;
            this.scrollTop = 0;
            this.renderList();
        },

        onLimitInput: function (e) {
            if (e.keyCode === 13) {
                var parsed = parseInt($(e.currentTarget).val(), 10);
                if (Number.isFinite(parsed) && parsed > 0) {
                    this.drawerPageSize = parsed;
                    this.page = 1;
                    this.scrollTop = 0;
                    this.renderList();
                }
            }
        },

        renderList: function () {
            var fullList = this.items || [];
            if (this.searchText) {
                var lowerSearch = this.searchText.toLowerCase();
                fullList = fullList.filter(function (item) {
                    var nameStr = (typeof item === 'object' && item.attributes && item.attributes.name) ? item.attributes.name : "";
                    var guidStr = (typeof item === 'object' && item.guid) ? item.guid : item;
                    return (nameStr.toLowerCase().indexOf(lowerSearch) !== -1) || 
                           (guidStr.toLowerCase().indexOf(lowerSearch) !== -1);
                });
            }

            this.totalFilteredCount = fullList.length;

            var startIndex = (this.page - 1) * this.drawerPageSize;
            var endIndex = Math.min(startIndex + this.drawerPageSize, this.totalFilteredCount);
            this.displayItems = fullList.slice(startIndex, endIndex);

            var listHtml = "";

            var virtualizedData = Utils.virtualizeList({
                items: this.displayItems,
                scrollTop: this.scrollTop,
                itemHeight: 24,
                overscan: 10,
                visibleCount: 40
            });
            
            if (virtualizedData.paddingTop > 0) {
                listHtml += '<div style="height: ' + virtualizedData.paddingTop + 'px"></div>';
            }

            _.each(virtualizedData.visibleItems, function (item, index) {
                var globalIndex = startIndex + virtualizedData.startIndex + index + 1;
                var isObj = typeof item === 'object' && item !== null;
                var guidStr = isObj ? item.guid : item;
                var typeName = isObj && item.typeName ? '[' + _.escape(item.typeName) + '] ' : '';
                var entityName = isObj && item.attributes && item.attributes.name ? _.escape(item.attributes.name) : _.escape(guidStr);
                var displayName = isObj && item.attributes && item.attributes.name ? typeName + entityName : _.escape(guidStr);
                
                listHtml += '<li class="drawer-list-item" ><span class="item-index" >' + globalIndex + '.</span> <a class="blue-link" title="' + _.escape(guidStr) + '" data-guid="' + _.escape(guidStr) + '">' + displayName + '</a></li>';
            });
            
            if (virtualizedData.paddingBottom > 0) {
                listHtml += '<div style="height: ' + virtualizedData.paddingBottom + 'px"></div>';
            }

            this.ui.drawerItemsList.html(listHtml);
            this.ui.drawerLoading.hide();

            if (this.displayItems.length === 0) {
                this.ui.drawerEmpty.html("No matching GUIDs found");
                this.ui.drawerEmpty.show();
            } else {
                this.ui.drawerEmpty.hide();
            }

            // Update Pagination UI
            var showingStart = Math.min(startIndex + 1, this.totalFilteredCount);
            var showingEnd = endIndex;
            this.ui.showingText.text(showingStart + "-" + showingEnd + " of " + this.totalFilteredCount);
            this.ui.btnCurrentPage.text(this.page);
            
            var maxPage = Math.ceil(this.totalFilteredCount / this.drawerPageSize) || 1;
            
            if (this.page <= 1) {
                this.ui.btnFirstPage.prop("disabled", true);
                this.ui.btnPrevPage.prop("disabled", true);
            } else {
                this.ui.btnFirstPage.prop("disabled", false);
                this.ui.btnPrevPage.prop("disabled", false);
            }
            
            if (this.page >= maxPage) {
                this.ui.btnNextPage.prop("disabled", true);
                this.ui.btnLastPage.prop("disabled", true);
            } else {
                this.ui.btnNextPage.prop("disabled", false);
                this.ui.btnLastPage.prop("disabled", false);
            }
        },

        onSearch: function (e) {
            this.searchText = $(e.currentTarget).val().trim();
            this.scrollTop = 0;
            this.loadData();
        },

        onItemClick: function (e) {
            var guid = $(e.currentTarget).data('guid');
            if (this.onItemClickCb) {
                this.onItemClickCb(guid, this.actionType);
            }
        },

        onCopyRunId: function () {
            var $temp = $("<input>");
            $("body").append($temp);
            $temp.val(this.runId).select();
            document.execCommand("copy");
            $temp.remove();
            Utils.notifySuccess({
                content: "Run Id copied to clipboard"
            });
        },

        closeDrawer: function () {
            var that = this;
            this.ui.overlay.removeClass('open');
            this.ui.panel.removeClass('open');
            $('body').removeClass('drawer-open-lock');

            setTimeout(function () {
                that.destroy();
            }, 300);
        },

        templateHelpers: function () {
            return {
                title: this.title || 'Entities',
                runId: this.runId,
                fetchData: this.fetchData
            };
        }
    });

    return DrawerView;
});
