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
    'hbs!tmpl/business_metadata/CreateBusinessMetadataLayoutView_tmpl',
    'utils/Utils',
    'utils/Messages',
    'views/business_metadata/BusinessMetadataAttributeItemView',
    'collection/VTagList',
    'models/VEntity',
    'utils/UrlLinks',
    'platform'
], function(require, Backbone, CreateBusinessMetadataLayoutViewTmpl, Utils, Messages, BusinessMetadataAttributeItemView, VTagList, VEntity, UrlLinks, platform) {

    var CreateBusinessMetadataLayoutView = Backbone.Marionette.CompositeView.extend(
        /** @lends CreateBusinessMetadataLayoutView */
        {
            _viewName: 'CreateBusinessMetadataLayoutView',

            template: CreateBusinessMetadataLayoutViewTmpl,

            templateHelpers: function() {
                return {
                    create: this.create,
                    description: this.description,
                    fromTable: this.fromTable,
                    isEditAttr: this.isEditAttr
                };
            },

            /** Layout sub regions */
            regions: {},

            childView: BusinessMetadataAttributeItemView,

            childViewContainer: "[data-id='addAttributeDiv']",

            childViewOptions: function() {
                return {
                    // saveButton: this.ui.saveButton,
                    parentView: this
                };
            },
            /** ui selector cache */
            ui: {
                name: "[data-id='name']",
                description: "[data-id='description']",
                title: "[data-id='title']",
                attributeData: "[data-id='attributeData']",
                addAttributeDiv: "[data-id='addAttributeDiv']",
                createForm: '[data-id="createForm"]',
                businessMetadataAttrPageCancle: '[data-id="businessMetadataAttrPageCancle"]',
                businessMetadataAttrPageOk: '[data-id="businessMetadataAttrPageOk"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.attributeData] = "onClickAddAttriBtn";
                events["click " + this.ui.businessMetadataAttrPageOk] = function(e) {
                    var that = this,
                        modal = that.$el;
                    if (e.target.dataset.action == "attributeEdit" || e.target.dataset.action == "addAttribute") {
                        // var selectedBusinessMetadata = that.businessMetadataDefCollection.fullCollection.findWhere({ guid: that.guid });
                        that.onUpdateAttr();
                    } else {
                        if (that.$el.find('.form-control.businessMetadata-name')[0].value === "") {
                            $(that.$el.find('.form-control.businessMetadata-name')[0]).css("borderColor", "red");
                            Utils.notifyInfo({
                                content: "Business Metadata name is empty."
                            });

                        } else {
                            that.onCreateBusinessMetadata();
                        }
                    }

                };
                events["click " + this.ui.businessMetadataAttrPageCancle] = function(e) {
                    this.options.onUpdateBusinessMetadata();
                };
                return events;
            },
            /**
             * intialize a new CreateBusinessMetadataLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'businessMetadataDefCollection', 'enumDefCollection', 'model', 'descriptionData', 'isNewBusinessMetadata', 'isAttrEdit', 'entityDefCollection', 'typeHeaders', 'attrDetails'));
                this.fromTable = this.isNewBusinessMetadata ? true : false;
                this.isEditAttr = this.isAttrEdit ? false : true;
                this.businessMetadataModel = new VEntity();
                if (this.model) {
                    this.description = this.model.get('description');
                } else {
                    this.create = true;
                }
                if (!this.isNewBusinessMetadata) {
                    this.collection = this.isAttrEdit ? new Backbone.Collection([{
                        "name": this.attrDetails.name,
                        "typeName": this.attrDetails.attrTypeName,
                        "isOptional": true,
                        "cardinality": "SINGLE",
                        "valuesMinCount": 0,
                        "valuesMaxCount": 1,
                        "isUnique": false,
                        "isIndexable": false
                    }]) : new Backbone.Collection([{
                        "name": "",
                        "typeName": "string",
                        "isOptional": true,
                        "cardinality": "SINGLE",
                        "valuesMinCount": 0,
                        "valuesMaxCount": 1,
                        "isUnique": false,
                        "isIndexable": false
                    }]);
                } else {
                    this.collection = new Backbone.Collection();
                }

            },
            bindEvents: function() {},
            onRender: function() {
                var that = this;
                this.$('.fontLoader').show();
                if (!('placeholder' in HTMLInputElement.prototype)) {
                    this.ui.createForm.find('input,textarea').placeholder();
                }
                if (this.isNewBusinessMetadata == true) {
                    that.ui.businessMetadataAttrPageOk.text("Create");
                    that.ui.businessMetadataAttrPageOk.attr('data-action', 'newBusinessMetadata');
                } else {
                    that.ui.businessMetadataAttrPageOk.text("Save");
                    that.ui.businessMetadataAttrPageOk.attr('data-action', 'attributeEdit');
                }
                this.hideLoader();
            },
            hideLoader: function() {
                this.$('.fontLoader').hide();
                this.$('.hide').removeClass('hide');
            },
            collectionAttribute: function() {
                this.collection.add(new Backbone.Model({
                    "name": "",
                    "typeName": "string",
                    "isOptional": true,
                    "cardinality": "SINGLE",
                    "valuesMinCount": 0,
                    "valuesMaxCount": 1,
                    "isUnique": false,
                    "isIndexable": false
                }));
            },
            onClickAddAttriBtn: function() {
                this.collectionAttribute();
                if (!('placeholder' in HTMLInputElement.prototype)) {
                    this.ui.addAttributeDiv.find('input,textarea').placeholder();
                }
            },
            loaderStatus: function(isActive) {
                var that = this;
                if (isActive) {
                    parent.$('.business-metadata-attr-tableOverlay').show();
                    parent.$('.business-metadata-attr-fontLoader').show();
                } else {
                    parent.$('.business-metadata-attr-tableOverlay').hide();
                    parent.$('.business-metadata-attr-fontLoader').hide();
                }
            },
            validateValues: function() {
                var attrNameValidate = true,
                    enumValue = true,
                    stringValidate = true,
                    enumType = true;
                if (this.$el.find(".attributeInput").length > 0) {
                    this.$el.find(".attributeInput").each(function() {
                        if ($(this).val() === "") {
                            $(this).css("borderColor", "red");
                            attrNameValidate = false;
                        }
                    });
                }
                if (this.$el.find(".enumvalue-container").length > 0 && this.$el.find(".enumvalue-container")[0].style.display != 'none') {
                    this.$el.find(".enumvalue-container").each(function(index) {
                        if (this.style.display != 'none') {
                            if ($(this).find(".enumValueSelector").length > 0) {
                                $(this).find(".enumValueSelector").each(function(index) {
                                    if ($(this).val().length === 0) {
                                        $(this).css("borderColor", "red");
                                        enumValue = false;
                                    }
                                });
                            }
                        }
                    })
                }
                if (this.$el.find(".enumtype-container").length > 0 && this.$el.find(".enumtype-container")[0].style.display != 'none') {
                    this.$el.find(".enumtype-container").each(function(index) {
                        if (this.style.display != 'none') {
                            if ($(this).find(".enumTypeSelector").length > 0) {
                                $(this).find(".enumTypeSelector").each(function(index) {
                                    if ($(this).val() == null || $(this).val() == '' || $(this).val().length === 0) {
                                        $(this).css("borderColor", "red");
                                        enumType = false;
                                    }
                                });
                            }
                        }
                    })
                }
                if (this.$el.find(".stringlength-container").length > 0 && this.$el.find(".stringlength-container")[0].style.display != 'none') {
                    this.$el.find(".stringlength-container").each(function(index) {
                        if (this.style.display != 'none') {
                            if ($(this).find(".stringLengthVal").length > 0) {
                                $(this).find(".stringLengthVal").each(function(index) {
                                    if ($(this).val().length === 0) {
                                        $(this).css("borderColor", "red");
                                        stringValidate = false;
                                    }
                                });
                            }
                        };
                    })
                }

                this.$el.find(".attributeInput").keyup(function() {
                    $(this).css("borderColor", "#e8e9ee");
                });
                if (!attrNameValidate) {
                    Utils.notifyInfo({
                        content: "Please fill the attributes details"
                    });
                    return true;
                }
                if (!enumType) {
                    Utils.notifyInfo({
                        content: "Please enter the Enumeration Name or select another type"
                    });
                    return true;
                }
                if (!enumValue) {
                    Utils.notifyInfo({
                        content: "Please enter the Enum values or select another type"
                    });
                    return true;
                }
                if (!stringValidate) {
                    Utils.notifyInfo({
                        content: "Please enter the Max Length for string or select another type"
                    });
                    return true;
                }
            },
            businessMetadataAttributes: function(modelEl, attrObj) {
                var obj = {
                    options: {
                        "applicableEntityTypes": JSON.stringify(modelEl.find(".entityTypeSelector").val()),
                        "maxStrLength": modelEl.find(".stringLengthVal").val() ? modelEl.find(".stringLengthVal").val() : "0"
                    }
                };
                // var types = ["string","boolean"];
                // if (attrObj.typeName != "string" && attrObj.typeName != "boolean" && attrObj.typeName != "byte" && attrObj.typeName != "short" && attrObj.typeName != "int" && attrObj.typeName != "float" && attrObj.typeName != "double" && attrObj.typeName != "long" && attrObj.typeName != "date") {
                //     var enumName = enumDefCollection.fullCollection.findWhere({ name: attrObj.typeName });
                //     if (enumName) {
                //         return 
                //     }
                // }
                if (obj.multiValueSelect) {
                    obj.multiValued = true;
                    obj.typeName = "array<" + obj.typeName + ">";
                }
                return obj;
            },
            highlightAttrinuteName: function(modelEl, obj) {
                Utils.notifyInfo({
                    content: "Attribute " + obj.name + " already exist"
                });
                modelEl.find(".attributeInput").css("borderColor", "red");
                this.loaderStatus(false);
            },
            createEnumObject: function(arrayObj, obj, enumVal) {
                return arrayObj.push({
                    "name": obj.typeName,
                    "elementDefs": enumVal
                });
            },
            onCreateBusinessMetadata: function() {
                var that = this,
                    attrNames = [],
                    isvalidName = true;

                if (this.validateValues()) {
                    return;
                };
                this.loaderStatus(true);
                var name = this.ui.name.val(),
                    description = _.escape(this.ui.description.val());
                var attributeObj = this.collection.toJSON();
                if (this.collection.length === 1 && this.collection.first().get("name") === "") {
                    attributeObj = [];
                }
                if (attributeObj.length) {
                    // _.each(attributeObj, function(obj) {
                    //     var modelEl = this.$('#' + obj.modalID);
                    //     modelEl.find(".attributeInput").css("borderColor", "transparent");;
                    //     if (attrNames.indexOf(obj.name) > -1) {
                    //         that.highlightAttrinuteName(modelEl, obj);
                    //         isvalidName = false;
                    //         return true;
                    //     } else {
                    //         attrNames.push(obj.name);
                    //     }
                    //     obj = that.businessMetadataAttributes(modelEl, obj);
                    //     // if (that.isPostCallEnum) {
                    //     //     that.createEnumObject(enumDefs, obj, elementValues);
                    //     // }
                    //     // if (that.isPutCall) {
                    //     //     that.createEnumObject(putEnumDef, obj, elementValues);
                    //     // }
                    // });
                    var notifyObj = {
                        modal: true,
                        confirm: {
                            confirm: true,
                            buttons: [{
                                    text: "Ok",
                                    addClass: "btn-atlas btn-md",
                                    click: function(notice) {
                                        notice.remove();
                                    }
                                },
                                null
                            ]
                        }
                    };
                }
                if (isvalidName) {
                    this.json = {
                        "enumDefs": [],
                        "structDefs": [],
                        "classificationDefs": [],
                        "entityDefs": [],
                        "businessMetadataDefs": [{
                            "category": "BUSINESS_METADATA",
                            "createdBy": "admin",
                            "updatedBy": "admin",
                            "version": 1,
                            "typeVersion": "1.1",
                            "name": name.trim(),
                            "description": description.trim(),
                            "attributeDefs": attributeObj
                        }]
                    };
                    var apiObj = {
                        sort: false,
                        data: this.json,
                        success: function(model, response) {
                            var nameSpaveDef = model.businessMetadataDefs;
                            if (nameSpaveDef) {
                                that.options.businessMetadataDefCollection.fullCollection.add(nameSpaveDef);
                                Utils.notifySuccess({
                                    content: "Business Metadata " + name + Messages.getAbbreviationMsg(false, 'addSuccessMessage')
                                });
                            }
                            that.options.onUpdateBusinessMetadata();
                        },
                        silent: true,
                        reset: true,
                        complete: function(model, status) {
                            attrNames = [];
                            that.loaderStatus(false);
                        }
                    }
                    apiObj.type = "POST";
                    that.businessMetadataModel.saveBusinessMetadata(apiObj);
                } else {
                    attrNames = [];
                }
            },
            onUpdateAttr: function() {
                var that = this,
                    selectedBusinessMetadata = $.extend(true, {}, that.options.selectedBusinessMetadata.toJSON()),
                    attributeDefs = selectedBusinessMetadata['attributeDefs'],
                    isvalidName = true;
                if (this.validateValues()) {
                    return;
                };
                if (this.collection.length > 0) {
                    this.loaderStatus(true);
                    if (selectedBusinessMetadata.attributeDefs === undefined) {
                        selectedBusinessMetadata.attributeDefs = [];
                    }
                    selectedBusinessMetadata.attributeDefs = selectedBusinessMetadata.attributeDefs.concat(this.collection.toJSON());
                    // this.collection.each(function(model) {
                    //     var obj = model.toJSON(),
                    //         modelEl = this.$('#' + obj.modalID);
                    //     modelEl.find(".attributeInput").css("borderColor", "transparent");
                    //     // if (that.options.isNewAttr == true && _.find(attributeDefs, { name: obj.name })) {
                    //     //     that.highlightAttrinuteName(modelEl, obj);
                    //     //     isvalidName = false;
                    //     //     return true;
                    //     // }
                    //     obj = that.businessMetadataAttributes(modelEl, obj);
                    //     // if (that.isPostCallEnum) {
                    //     //     that.createEnumObject(postEnumDef, obj, elementValues);
                    //     // } else if (that.isPutCall) {
                    //     //     that.createEnumObject(enumDefs, obj, elementValues);
                    //     // }

                    //     // if (that.options.isNewAttr == true) {
                    //     //     selectedBusinessMetadata.attributeDefs.push(obj);
                    //     // } else {
                    //     //     var attrDef = selectedBusinessMetadata.attributeDefs;
                    //     //     _.each(attrDef, function(attrObj) {
                    //     //         if (attrObj.name === that.$el.find(".attributeInput")[0].value) {
                    //     //             attrObj.name = obj.name;
                    //     //             attrObj.typeName = obj.typeName;
                    //     //             attrObj.multiValued = obj.multiValueSelect || false;
                    //     //             attrObj.options.applicableEntityTypes = obj.options.applicableEntityTypes;
                    //     //             attrObj.enumValues = obj.enumValues;
                    //     //             attrObj.options.maxStrLength = obj.options.maxStrLength;
                    //     //         }
                    //     //     });
                    //     // }
                    // });
                    if (isvalidName) {
                        this.json = {
                            "enumDefs": [],
                            "structDefs": [],
                            "classificationDefs": [],
                            "entityDefs": [],
                            "businessMetadataDefs": [selectedBusinessMetadata]
                        };
                        var apiObj = {
                            sort: false,
                            data: this.json,
                            success: function(model, response) {
                                var selectedBusinessMetadata = that.options.businessMetadataDefCollection.fullCollection.findWhere({ guid: that.options.guid });
                                Utils.notifySuccess({
                                    content: "One or more Business Metadada attribute" + Messages.getAbbreviationMsg(false, 'editSuccessMessage')
                                });
                                if (model.businessMetadataDefs && model.businessMetadataDefs.length) {
                                    that.options.selectedBusinessMetadata.set(model.businessMetadataDefs[0]);
                                }
                                that.options.onEditCallback();
                                that.options.onUpdateBusinessMetadata();
                            },
                            silent: true,
                            reset: true,
                            complete: function(model, status) {
                                that.loaderStatus(false);
                            }
                        }
                        apiObj.type = "PUT";
                        that.businessMetadataModel.saveBusinessMetadata(apiObj);
                    }

                } else {
                    Utils.notifySuccess({
                        content: "No attribute updated"
                    });
                    this.loaderStatus(false);
                    that.options.onUpdateBusinessMetadata();
                }
            }
        });
    return CreateBusinessMetadataLayoutView;
});