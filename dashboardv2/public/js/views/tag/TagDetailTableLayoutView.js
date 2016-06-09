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
    'hbs!tmpl/tag/TagDetailTableLayoutView_tmpl',
    'utils/CommonViewFunction',
    'utils/Utils',
    'collection/VTagList'
], function(require, Backbone, TagDetailTableLayoutView_tmpl, CommonViewFunction, Utils, VTagList) {
    'use strict';

    var TagDetailTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends TagDetailTableLayoutView */
        {
            _viewName: 'TagDetailTableLayoutView',

            template: TagDetailTableLayoutView_tmpl,

            /** Layout sub regions */
            regions: {
                RTagTermTableLayoutView: "#r_tagTermTableLayoutView"
            },

            /** ui selector cache */
            ui: {
                detailValue: "[data-id='detailValue']",
                addTag: "[data-id='addTag']",
                deleteTag: "[data-id='delete']",
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.addTag] = function(e) {
                    this.addModalView(e);
                };
                events["click " + this.ui.deleteTag] = function(e) {
                    this.deleteTagDataModal(e);
                };
                return events;
            },
            /**
             * intialize a new TagDetailTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'globalVent', 'collection', 'guid', 'term'));
                this.collectionObject = this.collection.toJSON();
                this.tagTermCollection = new VTagList();
                var tagorterm = _.toArray(this.collectionObject[0].traits),
                    tagTermList = [],
                    that = this;
                _.each(tagorterm, function(object) {
                    if (that.term) {
                        var checkTagOrTerm = Utils.checkTagOrTerm(object.typeName);
                        if (checkTagOrTerm.term) {
                            tagTermList.push(object);
                        }
                    } else {
                        var checkTagOrTerm = Utils.checkTagOrTerm(object.typeName);
                        if (!checkTagOrTerm.term) {
                            tagTermList.push(object);
                        }
                    }
                });
                this.tagTermCollection.set(tagTermList);
                this.commonTableOptions = {
                    collection: this.tagTermCollection,
                    includeFilter: false,
                    includePagination: true,
                    includePageSize: false,
                    includeFooterRecords: true,
                    gridOpts: {
                        className: "table table-hover backgrid table-quickMenu",
                        emptyText: 'No records found!'
                    },
                    filterOpts: {},
                    paginatorOpts: {}
                };
            },
            bindEvents: function() {},
            onRender: function() {
                this.renderTableLayoutView();
            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var cols = new Backgrid.Columns(that.getSchemaTableColumns());
                    that.RTagTermTableLayoutView.show(new TableLayout(_.extend({}, that.commonTableOptions, {
                        globalVent: that.globalVent,
                        columns: cols
                    })));
                });
            },
            getSchemaTableColumns: function() {
                var that = this;
                var col = {};

                return this.tagTermCollection.constructor.getTableCols({
                        TagorTerm: {
                            label: (this.term) ? "Terms" : "Tags",
                            cell: "String",
                            editable: false,
                            sortable: false,
                            formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                fromRaw: function(rawValue, model) {
                                    return model.get('typeName');
                                }
                            })
                        },
                        Attributes: {
                            label: "Attributes",
                            cell: "html",
                            editable: false,
                            sortable: false,
                            formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                fromRaw: function(rawValue, model) {
                                    var values = model.get('values'),
                                        tagValue = 'NA';
                                    if (!_.isEmpty(values)) {
                                        var stringArr = [];
                                        tagValue = "";
                                        _.each(values, function(val, key) {
                                            var attrName = "<span>" + key + ":" + val + "</span>";
                                            stringArr.push(attrName);
                                        });
                                        tagValue += stringArr.join(", ");
                                    }
                                    return tagValue;
                                }
                            })
                        },
                        tool: {
                            label: "Tool",
                            cell: "html",
                            editable: false,
                            sortable: false,
                            formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                                fromRaw: function(rawValue, model) {
                                    return '<a href="javascript:void(0)"><i class="fa fa-trash" data-id="delete" data-name="' + model.get('typeName') + '"></i></a>';
                                }
                            })
                        },

                    },
                    this.tagTermCollection);
            },
            addModalView: function(e) {
                var that = this;
                require(['views/tag/addTagModalView'], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        vent: that.vent,
                        guid: that.guid,
                        modalCollection: that.collection
                    });
                    // view.saveTagData = function() {
                    //override saveTagData function
                    // }
                });
            },
            deleteTagDataModal: function(e) {
                var tagName = $(e.currentTarget).data("name"),
                    that = this,
                    modal = CommonViewFunction.deleteTagModel(tagName);
                modal.on('ok', function() {
                    that.deleteTagData(e);
                });
                modal.on('closeModal', function() {
                    modal.trigger('cancel');
                });
            },
            deleteTagData: function(e) {
                var that = this,
                    tagName = $(e.currentTarget).data("name");
                CommonViewFunction.deleteTag({
                    'tagName': tagName,
                    'guid': that.guid,
                    callback: function() {
                        that.$('.fontLoader').show();
                        that.collection.fetch({ reset: true });
                    }
                });
            }
        });
    return TagDetailTableLayoutView;
});
