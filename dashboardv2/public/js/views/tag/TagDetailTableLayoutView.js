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
    'collection/VTagList',
    'utils/Messages'
], function(require, Backbone, TagDetailTableLayoutView_tmpl, CommonViewFunction, Utils, VTagList, Messages) {
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
                editTag: "[data-id='edit']",
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
                events["click " + this.ui.editTag] = function(e) {
                    this.editTagDataModal(e);
                };
                return events;
            },
            /**
             * intialize a new TagDetailTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'entity', 'guid', 'term', 'entityName', 'fetchCollection', 'enumDefCollection'));
                this.collectionObject = this.entity;
                this.tagTermCollection = new VTagList();
                var tagorterm = _.toArray(this.collectionObject.classifications),
                    tagTermList = [],
                    that = this;
                _.each(tagorterm, function(object) {
                    var checkTagOrTerm = Utils.checkTagOrTerm(object);
                    if (that.term) {
                        if (checkTagOrTerm.term) {
                            tagTermList.push(object);
                        }
                    } else {
                        if (checkTagOrTerm.tag) {
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
                                    var values = model.get('attributes'),
                                        tagValue = 'NA';
                                    if (!_.isEmpty(values)) {
                                        var stringArr = [];
                                        tagValue = "";
                                        _.each(values, function(val, key) {
                                            var attrName = "<span>" + _.escape(key) + ":" + _.escape(val) + "</span>";
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
                                    return '<button class="btn btn-atlasAction btn-atlas no-margin-bottom typeLOV" data-id="delete" data-name="' + model.get('typeName') + '"><i class="fa fa-trash"></i></button> <button class="btn btn-atlasAction btn-atlas no-margin-bottom typeLOV" data-id="edit" data-name="' + model.get('typeName') + '"><i class="fa fa-pencil"></i></button>';
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
                        guid: that.guid,
                        modalCollection: that.collection,
                        enumDefCollection: that.enumDefCollection
                    });
                    // view.saveTagData = function() {
                    //override saveTagData function
                    // }
                });
            },
            deleteTagDataModal: function(e) {
                var tagName = $(e.currentTarget).data("name"),
                    that = this;
                if (that.term) {
                    var modal = CommonViewFunction.deleteTagModel({
                        msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from" + " " + "<b>" + this.entityName + "?</b></div>",
                        titleMessage: Messages.removeTerm,
                        buttonText: "Remove",
                    });
                } else {
                    var modal = CommonViewFunction.deleteTagModel({
                        msg: "<div class='ellipsis'>Remove: " + "<b>" + _.escape(tagName) + "</b> assignment from" + " " + "<b>" + this.entityName + "?</b></div>",
                        titleMessage: Messages.removeTag,
                        buttonText: "Remove",
                    });
                }

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
                    'tagOrTerm': (that.term ? "term" : "tag"),
                    showLoader: function() {
                        that.$('.fontLoader').show();
                        that.$('.tableOverlay').show();
                    },
                    hideLoader: function() {
                        that.$('.fontLoader').hide();
                        that.$('.tableOverlay').hide();
                    },
                    callback: function() {
                        this.hideLoader();
                        if (that.fetchCollection) {
                            that.fetchCollection();
                        }

                    }
                });
            },
            editTagDataModal: function(e) {
                var that = this,
                    tagName = $(e.currentTarget).data('name'),
                    tagModel = _.findWhere(that.collectionObject.classifications, { typeName: tagName });
                require([
                    'views/tag/addTagModalView'
                ], function(AddTagModalView) {
                    var view = new AddTagModalView({
                        'tagModel': tagModel,
                        callback: function() {
                            that.fetchCollection();
                        },
                        guid: that.guid,
                        'enumDefCollection': that.enumDefCollection
                    });
                });
            }
        });
    return TagDetailTableLayoutView;
});
