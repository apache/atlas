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
    'hbs!tmpl/glossary/GlossaryLayoutView_tmpl',
    'utils/Utils',
    'utils/Messages',
    'utils/Globals',
    'utils/CommonViewFunction',
    'jstree'
], function(require, Backbone, GlossaryLayoutViewTmpl, Utils, Messages, Globals, CommonViewFunction) {
    'use strict';

    var GlossaryLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends GlossaryLayoutView */
        {
            _viewName: 'GlossaryLayoutView',

            template: GlossaryLayoutViewTmpl,

            /** Layout sub regions */
            regions: {},
            templateHelpers: function() {
                return {
                    isAssignView: this.isAssignView
                };
            },

            /** ui selector cache */
            ui: {
                createGlossary: "[data-id='createGlossary']",
                refreshGlossary: "[data-id='refreshGlossary']",
                searchTerm: "[data-id='searchTerm']",
                searchCategory: "[data-id='searchCategory']",
                glossaryView: 'input[name="glossaryView"]',
                termTree: "[data-id='termTree']",
                categoryTree: "[data-id='categoryTree']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["change " + this.ui.glossaryView] = 'glossaryViewToggle';
                events["click " + this.ui.createGlossary] = function(e) {
                    var that = this;
                    if (e) {
                        $(e.currentTarget).attr("disabled", "true");
                    }
                    CommonViewFunction.createEditGlossaryCategoryTerm({
                        isGlossaryView: true,
                        collection: this.glossaryCollection,
                        callback: function() {
                            that.ui.createGlossary.removeAttr("disabled");
                            that.getGlossary();
                        },
                        onModalClose: function() {
                            that.ui.createGlossary.removeAttr("disabled");
                        }
                    })
                };
                events["click " + this.ui.refreshGlossary] = 'getGlossary';
                events["keyup " + this.ui.searchTerm] = function() {
                    this.ui.termTree.jstree("search", this.ui.searchTerm.val());
                };
                events["keyup " + this.ui.searchCategory] = function() {
                    this.ui.categoryTree.jstree("search", this.ui.searchCategory.val());
                };
                return events;
            },
            /**
             * intialize a new GlossaryLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'value', 'glossaryCollection', 'glossary', 'isAssignTermView', 'isAssignCategoryView', 'isAssignEntityView'));
                this.viewType = "term";
                this.isAssignView = this.isAssignTermView || this.isAssignCategoryView || this.isAssignEntityView;
                this.bindEvents();
                this.query = {
                    term: {
                        url: null
                    },
                    category: {
                        url: null
                    }
                };
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.glossaryCollection.fullCollection, "reset add remove change", function() {
                    this.generateTree();
                }, this);
                this.listenTo(this.glossaryCollection, "update:details", function() {
                    this.getGlossary();
                }, this);
                $('body').on('click', '.termPopoverOptions li, .categoryPopoverOptions li', function(e) {
                    that.$('.termPopover,.categoryPopover').popover('hide');
                    that[$(this).find('a').data('fn')](e)
                });
            },
            onRender: function() {
                if (this.isAssignCategoryView) {
                    this.$('.category-view').show();
                    this.$('.term-view').hide();
                }
                if (this.isAssignView && this.glossaryCollection.fullCollection.length) {
                    this.generateTree();
                } else {
                    this.getGlossary();
                }
            },
            glossaryViewToggle: function(e) {
                if (e.currentTarget.checked) {
                    this.$('.category-view').show();
                    this.$('.term-view').hide();
                    this.viewType = "category";
                } else {
                    this.$('.term-view').show();
                    this.$('.category-view').hide();
                    this.viewType = "term";
                }
                if (Utils.getUrlState.isGlossaryTab()) {
                    // var name = this.query[this.viewType].url;
                    // var guid = this.glossary.selectedItem.guid;
                    // Utils.setUrl({
                    //     url: '#!/glossary/' + guid,
                    //     urlParams: {
                    //         viewType: this.viewType,
                    //     },
                    //     mergeBrowserUrl: false,
                    //     trigger: true,
                    //     updateTabState: true
                    // });
                }
            },
            getGlossary: function() {
                this.glossaryCollection.fetch({ reset: true });
            },
            generateCategoryData: function(options) {
                return _.map(options.data, function(obj) {
                    return {
                        "text": obj.displayText,
                        "icon": "fa fa-files-o",
                        "guid": obj.categoryGuid,
                        "id": obj.categoryGuid,
                        "glossaryId": options.node.glossaryId,
                        "glossaryName": options.node.glossaryName,
                        "model": obj,
                        "type": "GlossaryCategory",
                        "children": true
                    }
                });
            },
            getCategory: function(options) {
                var that = this;
                this.glossaryCollection.getCategory({
                    "guid": options.node.guid,
                    "related": true,
                    "ajaxOptions": {
                        success: function(data) {
                            if (data && data.children) {
                                options.callback(that.generateCategoryData(_.extend({}, { "data": data.children }, options)));
                            } else {
                                options.callback([]);
                            }
                        },
                        cust_error: function() {
                            options.callback([]);
                        }
                    }
                });
            },
            generateData: function(opt) {
                var that = this,
                    type = opt.type;
                var getSelectedState = function(options) {
                    var objGuid = options.objGuid,
                        node = options.node,
                        index = options.index;
                    if (!that.guid) {
                        var selectedItem = {
                            "type": "Glossary",
                            "model": that.glossaryCollection.first().toJSON()
                        };
                        selectedItem.text = selectedItem.model.displayName;
                        selectedItem.guid = selectedItem.model.guid;
                        if (index == 0 && selectedItem.guid == objGuid) {
                            that.glossary.selectedItem = selectedItem;
                            return {
                                'opened': true,
                                'selected': true
                            }
                        }
                    } else {
                        if (that.guid == objGuid) {
                            that.glossary.selectedItem = node
                            return {
                                'opened': true,
                                'selected': true
                            }
                        }
                    }
                }
                return this.glossaryCollection.fullCollection.map(function(model, i) {
                    var obj = model.toJSON(),
                        parent = {
                            "text": obj.displayName,
                            "icon": "fa fa-folder-o",
                            "guid": obj.guid,
                            "id": obj.guid,
                            "model": obj,
                            "type": obj.typeName ? obj.typeName : "Glossary",
                            "children": []
                        }
                    parent.state = getSelectedState({
                        index: i,
                        node: parent,
                        objGuid: obj.guid
                    });

                    if (type == "category" && obj.categories) {
                        _.each(obj.categories, function(category) {
                            if (category.parentCategoryGuid) {
                                return;
                            }
                            var type = category.typeName || "GlossaryCategory",
                                guid = category.categoryGuid,
                                categoryObj = {
                                    "text": category.displayText,
                                    "type": type,
                                    "guid": guid,
                                    "id": guid,
                                    "parent": obj,
                                    "glossaryId": obj.guid,
                                    "glossaryName": obj.displayName,
                                    "model": category,
                                    "children": true,
                                    "icon": "fa fa-files-o",
                                };
                            categoryObj.state = getSelectedState({
                                index: i,
                                node: categoryObj,
                                objGuid: guid
                            })
                            parent.children.push(categoryObj)
                        });
                    }
                    if (type == "term" && obj.terms) {
                        _.each(obj.terms, function(term) {
                            var type = term.typeName || "GlossaryTerm",
                                guid = term.termGuid,
                                termObj = {
                                    "text": term.displayText,
                                    "type": type,
                                    "guid": guid,
                                    "id": guid,
                                    "parent": obj,
                                    "glossaryName": obj.displayName,
                                    "glossaryId": obj.guid,
                                    "model": term,
                                    "icon": "fa fa-file-o"
                                }
                            termObj.state = getSelectedState({
                                index: i,
                                node: termObj,
                                objGuid: guid
                            })
                            parent.children.push(termObj);
                        });
                    }
                    return parent;
                });
            },
            manualRender: function(options) {
                _.extend(this, options);
                this.triggerUrl();
            },
            generateTree: function() {
                var $termTree = this.ui.termTree,
                    $categoryTree = this.ui.categoryTree,
                    that = this,
                    getTreeConfig = function(options) {
                        return {
                            "plugins": ["search", "themes", "core", "wholerow", "sort", "conditionalselect"],
                            "conditionalselect": function(node) {
                                if (that.isAssignView) {
                                    return node.original.type != "Glossary" ? true : false;
                                } else {
                                    return node.original.type != "NoAction" ? true : false;
                                }
                            },
                            "core": {
                                "data": function(node, cb) {
                                    if (node.id === "#") {
                                        cb(that.generateData(options));
                                    } else {
                                        that.getCategory({ "node": node.original, "callback": cb });
                                    }
                                },
                                "themes": {
                                    "name": that.isAssignView ? "default" : "default-dark",
                                    "dots": true
                                },
                            }
                        }
                    },
                    treeLoaded = function() {
                        if (that.selectFirstNodeManually) {
                            that.selectFirstNodeManually = false;
                            var id = that.glossary.selectedItem.guid;
                            $treeEl.jstree('select_node', '#' + id + '_anchor');
                            $treeEl.jstree('open_node', '#' + id + '_anchor');
                        }
                    },
                    createAction = function(options) {
                        var $el = options.el,
                            type = options.type,
                            popoverClassName = type == "term" ? "termPopover" : "categoryPopover";
                        if (!that.isAssignView) {
                            var wholerowEl = $el.find("li[role='treeitem'] > .jstree-wholerow:not(:has(>div.tools))")
                            wholerowEl.append('<div class="tools"><i class="fa fa-ellipsis-h ' + popoverClassName + '"></i></div>');

                            if (type == "term") {
                                that.createTermAction();
                            } else if (type == "category") {
                                that.createCategoryAction();
                            }
                        }
                    },
                    initializeTree = function(options) {
                        var $el = options.el,
                            type = options.type;

                        $el.jstree(getTreeConfig({
                                type: type
                            })).on("load_node.jstree", function(e, data) {
                                createAction(_.extend({}, options, data));
                            }).on("open_node.jstree", function(e, data) {
                                createAction(_.extend({}, options, data));
                            })
                            .on("select_node.jstree", function(e, data) {
                                that.glossary.selectedItem = data.node.original;
                                //$("." + popoverClassName).popover('hide');
                                that.triggerUrl();
                            }).bind('loaded.jstree', function(e, data) {
                                treeLoaded();
                            });
                    },
                    initializeTermTree = function() {
                        if ($termTree.data('jstree')) {
                            $('.termPopover').popover('destroy');
                            $termTree.jstree(true).refresh();
                        } else {
                            initializeTree({
                                el: $termTree,
                                type: "term"
                            });
                        }
                    },
                    initializeCategoryTree = function() {
                        if ($categoryTree.data('jstree')) {
                            $categoryTree.jstree(true).refresh();
                        } else {
                            initializeTree({
                                el: $categoryTree,
                                type: "category"
                            })
                        }
                    }
                if (this.isAssignView) {
                    if (this.isAssignTermView || this.isAssignEntityView) {
                        initializeTermTree();
                    } else if (this.isAssignCategoryView) {
                        initializeCategoryTree();
                    }
                } else {
                    initializeTermTree();
                    initializeCategoryTree();
                }


                if (Utils.getUrlState.isGlossaryTab()) {
                    this.triggerUrl();
                }
                this.glossaryCollection.trigger("render:done");
            },
            createTermAction: function() {
                var that = this;
                Utils.generatePopover({
                    el: this.$('.termPopover'),
                    contentClass: 'termPopoverOptions',
                    popoverOptions: {
                        content: function() {
                            var node = that.glossary.selectedItem,
                                liString = "";
                            if (node.type == "Glossary") {
                                liString = "<li data-type=" + node.type + " class='listTerm'><i class='fa fa-plus'></i> <a href='javascript:void(0)' data-fn='createSubNode'>Create Sub-Term</a></li>" +
                                    "<li data-type=" + node.type + " class='listTerm'><i class='fa fa-trash-o'></i><a href='javascript:void(0)' data-fn='deleteNode'>Delete Glossary</a></li>"
                            } else {
                                liString = "<li data-type=" + node.type + " class='listTerm'><i class='fa fa-trash-o'></i><a href='javascript:void(0)' data-fn='deleteNode'>Delete Term</a></li>"
                            }
                            return "<ul>" + liString + "</ul>";
                        }
                    }
                });
            },
            createCategoryAction: function() {
                var that = this;
                Utils.generatePopover({
                    el: this.$('.categoryPopover'),
                    contentClass: 'categoryPopoverOptions',
                    popoverOptions: {
                        content: function() {
                            var node = that.glossary.selectedItem,
                                liString = "";
                            if (node.type == "Glossary") {
                                liString = "<li data-type=" + node.type + " class='listTerm'><i class='fa fa-plus'></i> <a href='javascript:void(0)' data-fn='createSubNode'>Create Sub-Category</a></li>" +
                                    "<li data-type=" + node.type + " class='listTerm'><i class='fa fa-trash-o'></i><a href='javascript:void(0)' data-fn='deleteNode'>Delete Glossary</a></li>"
                            } else {
                                liString = "<li data-type=" + node.type + " class='listTerm'><i class='fa fa-plus'></i> <a href='javascript:void(0)' data-fn='createSubNode'>Create Sub-Category</a></li>" +
                                    "<li data-type=" + node.type + " class='listTerm'><i class='fa fa-trash-o'></i><a href='javascript:void(0)' data-fn='deleteNode'>Delete Category</a></li>"
                            }
                            return "<ul>" + liString + "</ul>";
                        }
                    }
                });
            },
            createSubNode: function(opt) {
                var that = this,
                    type = this.glossary.selectedItem.type;
                if ((type == "Glossary" || type == "GlossaryCategory") && this.viewType == "category") {
                    CommonViewFunction.createEditGlossaryCategoryTerm({
                        "isCategoryView": true,
                        "collection": that.glossaryCollection,
                        "callback": function() {
                            that.getGlossary();
                        },
                        "node": this.glossary.selectedItem
                    })
                } else {
                    CommonViewFunction.createEditGlossaryCategoryTerm({
                        "isTermView": true,
                        "callback": function() {
                            that.getGlossary();
                        },
                        "collection": that.glossaryCollection,
                        "node": this.glossary.selectedItem
                    })
                }
            },
            deleteNode: function(opt) {
                var that = this,
                    messageType = "",
                    options = {
                        success: function(rModel, response) {
                            Utils.notifySuccess({
                                content: messageType + Messages.deleteSuccessMessage
                            });
                            that.getGlossary();
                        }
                    },
                    type = this.glossary.selectedItem.type,
                    guid = this.glossary.selectedItem.guid
                if (type == "Glossary") {
                    messageType = "Glossary";
                    this.glossaryCollection.fullCollection.get(guid).destroy(options);
                } else if (type == "GlossaryCategory") {
                    messageType = "Category"
                    new this.glossaryCollection.model().deleteCategory(guid, options);
                } else if (type == "GlossaryTerm") {
                    messageType = "Term";
                    new this.glossaryCollection.model().deleteTerm(guid, options);
                }
            },
            triggerUrl: function() {
                if (this.isAssignView) {
                    return;
                }
                var selectedItem = this.glossary.selectedItem;
                if (this.glossaryCollection.length && _.isEmpty(selectedItem)) {
                    selectedItem = { "model": this.glossaryCollection.first().toJSON() };
                    selectedItem.guid = selectedItem.model.guid;
                    selectedItem.type = "glossary";
                    this.glossary.selectedItem = selectedItem;
                    this.selectFirstNodeManually = true;
                }
                if (_.isEmpty(selectedItem)) {
                    return;
                }
                var type = selectedItem.type;
                if (Utils.getUrlState.isGlossaryTab() || Utils.getUrlState.isDetailPage()) {
                    var urlParams = { gType: "glossary" },
                        guid = selectedItem.guid;
                    if (type === "GlossaryTerm") {
                        urlParams.gType = "term";
                    } else if (type === "GlossaryCategory") {
                        urlParams.gType = "category";
                    }
                    if (selectedItem.glossaryId) {
                        urlParams["gId"] = selectedItem.glossaryId;
                    }
                    Utils.setUrl({
                        url: '#!/glossary/' + guid,
                        mergeBrowserUrl: false,
                        trigger: true,
                        urlParams: urlParams,
                        updateTabState: true
                    });
                }
            }
        });
    return GlossaryLayoutView;
});