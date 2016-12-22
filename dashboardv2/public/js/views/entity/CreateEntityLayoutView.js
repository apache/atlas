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
    'hbs!tmpl/entity/CreateEntityLayoutView_tmpl',
    'utils/Utils',
    'collection/VTagList',
    'collection/VCommonList',
    'collection/VEntityList',
    'models/VEntity',
    'modules/Modal',
    'utils/Messages',
    'datetimepicker',
    'moment',
    'utils/UrlLinks',
    'collection/VSearchList',
    'utils/Enums'
], function(require, Backbone, CreateEntityLayoutViewTmpl, Utils, VTagList, VCommonList, VEntityList, VEntity, Modal, Messages, datepicker, moment, UrlLinks, VSearchList, Enums) {

    var CreateEntityLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends CreateEntityLayoutView */
        {
            _viewName: 'CreateEntityLayoutView',

            template: CreateEntityLayoutViewTmpl,

            templateHelpers: function() {
                return {
                    guid: this.guid
                };
            },

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                entityName: "[data-id='entityName']",
                entityList: "[data-id='entityList']",
                description: "[data-id='description']",
                entityInputData: "[data-id='entityInputData']",
                entityLegend: "[data-id='entityLegend']",
                toggleRequired: 'input[name="toggleRequired"]',
                assetName: "[data-id='assetName']",
                entityInput: "[data-id='entityInput']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["change " + this.ui.entityList] = "onEntityChange";
                events["change " + this.ui.toggleRequired] = function(e) {
                    this.requiredAllToggle(e.currentTarget.checked)
                };
                return events;
            },
            /**
             * intialize a new CreateEntityLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'callback', 'showLoader'));
                var that = this,
                    entityTitle, okLabel;
                this.entityDetailCollection = new VCommonList();
                this.searchCollection = new VSearchList();
                this.searchCollection.url = UrlLinks.searchApiUrl(Enums.searchUrlType.DSL);
                this.selectStoreCollection = new Backbone.Collection();
                this.entityModel = new VEntity();
                if (this.guid) {
                    this.collection = new VEntityList();
                    this.collection.modelAttrName = "createEntity"
                } else {
                    this.collection = new VTagList();
                }
                this.asyncFetchCounter = 0;
                this.required = true;
                if (this.guid) {
                    entityTitle = 'Edit entity';
                    okLabel = 'Update';
                } else {
                    entityTitle = 'Create entity';
                    okLabel = 'Create';
                }
                this.modal = new Modal({
                    title: entityTitle,
                    content: this,
                    cancelText: "Cancel",
                    okText: okLabel,
                    allowCancel: true,
                    okCloses: false,
                    resizable: true,
                    resizableOpts: {
                        minWidth: 600,
                        minHeight: 284,
                        handles: "n, e, s, w",
                        resize: function(event, ui) {
                            that.modal.$el.find('.modal-body').css('min-height', ui.size.height - 134 + 'px');
                            that.modal.$el.find('.modal-body').css('max-height', ui.size.height - 134 + 'px');
                        }
                    }
                }).open();
                this.modal.$el.find('button.ok').attr("disabled", true);
                this.ui.entityList.val("");
                $(this.ui.entityInputData).on('keyup change dp.change', that.modal.$el.find('input select textarea'), function(e) {
                    that.ui.entityInputData.find("input,select,textarea").each(function() {
                        if (this.value !== "") {
                            if ($(this).data('select2')) {
                                $(this).data('select2').$container.removeClass("errorClass")
                            } else {
                                $(this).removeClass('errorClass');
                            }
                        }
                    });
                });
                this.modal.on('ok', function(e) {
                    that.okButton();
                });
                this.modal.on('closeModal', function() {
                    that.modal.trigger('cancel');
                });
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.collection, "reset", function() {
                    --this.asyncFetchCounter;
                    this.entityCollectionList();
                }, this);
                this.listenTo(this.collection, 'error', function() {
                    --this.asyncFetchCounter
                    this.hideLoader();
                }, this);
                this.listenTo(this.searchCollection, "reset", function() {
                    this.addJsonSearchData();
                }, this);
                this.listenTo(this.searchCollection, 'error', function(data, key) {
                    this.addJsonSearchData(key);
                    this.hideLoader();
                }, this);
            },
            onRender: function() {
                this.bindEvents();
                this.fetchCollections();
            },
            fetchCollections: function() {
                this.asyncFetchCounter++;
                if (this.guid) {
                    this.collection.url = UrlLinks.entitiesApiUrl(this.guid);
                    this.collection.fetch({ reset: true });
                } else {
                    this.collection.url = UrlLinks.entitiesDefApiUrl()
                    this.collection.modelAttrName = 'list';
                    this.collection.fetch({ reset: true });
                }

            },
            entityCollectionList: function() {
                this.ui.entityList.empty();
                var that = this,
                    name = "",
                    value;
                if (this.guid) {
                    this.collection.each(function(val) {
                        name += _.escape(val.get("attributes").name) || _.escape(val.get("attributes").qualifiedName) || _.escape(val.get("attributes").id);
                        that.entityData = val;
                    });
                    this.ui.assetName.html(name);
                    this.onEntityChange(null, this.entityData);
                } else {
                    var str = '<option selected="selected" disabled="disabled">--Select entity-type--</option>';
                    this.collection.fullCollection.comparator = function(model) {
                        return model.get('name');
                    }
                    this.collection.fullCollection.sort().each(function(val) {
                        str += '<option>' + _.escape(val.get("name")) + '</option>';
                    });
                    this.ui.entityList.html(str);
                }
            },
            capitalize: function(string) {
                return string.charAt(0).toUpperCase() + string.slice(1);
            },
            requiredAllToggle: function(checked) {
                if (checked) {
                    this.ui.entityInputData.find('div.true').show();
                    this.ui.entityInputData.find('fieldset div.true').show();
                    this.required = false;
                } else {
                    this.ui.entityInputData.find('div.true').hide();
                    this.ui.entityInputData.find('fieldset div.true').hide();
                    this.required = true;
                }

            },
            onEntityChange: function(e, value) {
                this.modal.$el.find('button.ok').prop("disabled", false);
                var that = this,
                    typeName;
                this.showLoader();
                this.ui.entityInputData.empty();
                if (value) {
                    typeName = value.get("typeName");
                }
                if (typeName) {
                    this.collection.url = UrlLinks.entitiesDefApiUrl(typeName);
                } else {
                    this.collection.url = UrlLinks.entitiesDefApiUrl(e.target.value);
                    this.collection.modelAttrName = 'attributeDefs';
                }
                this.collection.fetch({
                    success: function(model, data) {
                        that.subAttributeData(data)
                    },
                    complete: function() {
                        var _self = that;
                        that.$('input[data-type="date"]').each(function() {
                            if (!$(this).data('datepicker')) {
                                $(this).datetimepicker({
                                    format: 'DD MMMM YYYY'
                                });
                            }
                        });
                        that.$('input[data-type="long"]').each(function() {
                            if (!$(this).data('datepicker')) {
                                $(this).datetimepicker({
                                    format: 'DD MMMM YYYY, HH:mm',
                                    showTodayButton: true,
                                    showClose: true
                                });
                            }
                        });
                        // IE9 allow input type number
                        that.$('input[data-type="int"]').on('keydown', function(e) {
                            var regex = /^[0-9]*([.](?=[^.]|$))*(?:\.\d{1,2})?$/; // allow only numbers [0-9] 
                            if (!regex.test(e.currentTarget.value)) {
                                return false;
                            }
                        });
                        if (that.ui.entityInputData.find('select.true,input.true').length === 0) {
                            that.requiredAllToggle(that.ui.entityInputData.find('select.true,input.true').length === 0);
                            that.ui.toggleRequired.prop('checked', true);

                        }
                        // IE9 allow input type number
                        that.$('input[data-type="int"]').on('keyup click', function(e) {
                            e.currentTarget.value = e.currentTarget.value;
                            var regex = /^[0-9]*([.](?=[^.]|$))*(?:\.\d{1,2})?$/; // allow only numbers [0-9] 
                            if (!regex.test(e.currentTarget.value)) {
                                var txtfld = e.currentTarget;
                                var newtxt = txtfld.value.slice(0, txtfld.value.length - 1);
                                txtfld.value = newtxt;
                            }
                        });
                    },
                    silent: true
                });
            },
            subAttributeData: function(data) {
                var that = this,
                    attributeInput = "",
                    alloptional = false;
                _.each(data.attributeDefs, function(value) {
                    if (value.isOptional == true) {
                        alloptional = true;
                    }

                    attributeInput += that.getContainer(value);
                });
                if (attributeInput !== "") {
                    entityTitle = that.getFieldSet(data, alloptional, attributeInput);
                    that.ui.entityInputData.prepend(entityTitle);
                }
                if (data.superTypes && data.superTypes.length > 0) {
                    for (var j = 0; j < data.superTypes.length; j++) {
                        var superTypeAttr = data.superTypes[j];
                        that.fetchTagSubData(superTypeAttr);
                    }
                } else {
                    this.hideLoader();
                }
                if (this.required) {
                    this.ui.entityInputData.find('fieldset div.true').hide()
                    this.ui.entityInputData.find('div.true').hide();
                }
                if (!('placeholder' in HTMLInputElement.prototype)) {
                    this.ui.entityInputData.find('input,select,textarea').placeholder();
                }
            },
            getContainer: function(value) {
                var entityLabel = this.capitalize(value.name);
                return '<div class="row row-margin-bottom ' + value.isOptional + '"><span class="col-md-3">' +
                    '<label class="' + value.isOptional + '">' + entityLabel + (value.isOptional == true ? '' : ' <span class="requiredInput">*</span>') + '</label></span>' +
                    '<span class="col-md-9 position-relative">' +
                    (value.typeName === "boolean" ? this.getSelect(value) : this.getInput(value)) +
                    '<span class="spanEntityType" title="Data Type : ' + value.typeName + '">' + '(' + Utils.escapeHtml(value.typeName) + ')' + '</span></input></span></div>';
            },
            getFieldSet: function(data, alloptional, attributeInput) {
                return '<fieldset class="scheduler-border' + (alloptional ? " alloptional" : "") + '"><legend class="scheduler-border">' + data.name + '</legend>' + attributeInput + '</fieldset>';
            },
            getInput: function(value) {
                var that = this;
                var entityValue = "";
                if (this.guid) {
                    var dataValue = this.entityData.get("attributes")[value.name];
                    if (_.isObject(dataValue)) {
                        entityValue = JSON.stringify(dataValue);
                    } else {
                        if (dataValue) {
                            entityValue = dataValue;
                        }
                        if (value.typeName === "date" && dataValue) {
                            entityValue = moment(dataValue).format("DD MMMM YYYY");
                        }
                        if (value.typeName === "long") {
                            entityValue = moment(dataValue).format("DD MMMM YYYY, HH:mm");
                        }
                    }
                }
                if (value.typeName === "string" || value.typeName === "long" || value.typeName === "int" || value.typeName === "boolean" || value.typeName === "date") {
                    return '<input class="form-control entityInputBox ' + (value.isOptional === true ? "false" : "true") + '"' +
                        ' data-type="' + value.typeName + '"' +
                        ' value="' + entityValue + '"' +
                        ' data-key="' + value.name + '"' +
                        ' placeholder="' + value.name + '"' +
                        ' data-id="entityInput">';
                } else if (value.typeName === "map<string,string>") {
                    return '<textarea class="form-control entityInputBox ' + (value.isOptional === true ? "false" : "true") + '"' +
                        ' data-type="' + value.typeName + '"' +
                        ' data-key="' + value.name + '"' +
                        ' placeholder="' + value.name + '"' +
                        ' data-id="entityInput">' + entityValue + '</textarea>';
                } else {
                    var changeDatatype;
                    if (value.typeName.indexOf("array") == -1) {
                        changeDatatype = value.typeName;
                    } else {
                        if (value.typeName === "array<string>") {
                            changeDatatype = value.typeName;
                        } else {
                            changeDatatype = value.typeName.split('<')[1].split('>')[0];
                        }
                    }
                    $.extend(that.searchCollection.queryParams, { query: changeDatatype });
                    that.searchCollection.fetch({ reset: true });
                    return '<select class="form-control row-margin-bottom entityInputBox ' + (value.isOptional === true ? "false" : "true") + '" data-type="' + value.typeName +
                        '" data-key="' + value.name + '"data-id="entitySelectData" data-queryData="' + changeDatatype + '">' + (this.guid ? entityValue : "") + '</select>';
                }
            },
            getSelect: function(value) {
                return '<select class="form-control row-margin-bottom ' + (value.isOptional === true ? "false" : "true") + '" data-type="' + value.typeName + '" data-key="' + value.name + '" data-id="entityInput">' +
                    '<option disabled="disabled">--Select true or false--</option><option>true</option>' +
                    '<option>false</option></select>';
            },
            fetchTagSubData: function(entityName) {
                var that = this;
                this.collection.url = UrlLinks.entitiesDefApiUrl(entityName);
                this.collection.modelAttrName = 'attributeDefs';
                this.asyncFetchCounter++;
                this.collection.fetch({
                    success: function(model, data) {
                        that.subAttributeData(data);
                    },
                    complete: function() {
                        --that.asyncFetchCounter;
                        if (that.asyncFetchCounter === 0) {
                            that.$('input[data-type="date"]').each(function() {
                                if (!$(this).data('datepicker')) {
                                    $(this).datetimepicker({
                                        format: 'DD MMMM YYYY'
                                    });
                                }
                            });
                            that.$('input[data-type="long"]').each(function() {
                                if (!$(this).data('datepicker')) {
                                    $(this).datetimepicker({
                                        format: 'DD MMMM YYYY, HH:mm',
                                        showTodayButton: true,
                                        showClose: true
                                    });
                                }
                            });
                            that.hideLoader();
                        }
                        that.$('select[data-type="boolean"]').each(function(value, key) {
                            var dataKey = $(key).data('key');
                            if (that.entityData) {
                                var setValue = that.entityData.get("attributes")[dataKey];
                                this.value = setValue;
                            }
                        });

                    },
                    silent: true
                });
            },

            okButton: function() {
                var that = this;
                this.showLoader();
                this.parentEntity = this.ui.entityList.val();
                var entityAttribute = {};
                that.validateError = false;
                that.validateMessage = false;
                this.ui.entityInputData.find("input,select,textarea").each(function() {
                    var value = $(this).val();
                    if ($(this).val() && $(this).val().trim) {
                        value = $(this).val().trim();
                    }
                    if ($(this).hasClass("true")) {
                        if (value == "" || value == undefined) {
                            if ($(this).data('select2')) {
                                $(this).data('select2').$container.addClass("errorClass")
                            } else {
                                $(this).addClass('errorClass');
                            }
                            that.hideLoader();
                            that.validateError = true;
                            that.validateMessage = true;
                            return;
                        }
                    }
                    var dataTypeEnitity = $(this).data('type');
                    var datakeyEntity = $(this).data('key');
                    var selectDataType = $(this).data('querydata');
                    var pickKey = $(this).data('pickkey');
                    if (typeof datakeyEntity === 'string' && datakeyEntity.indexOf("Time") > -1) {
                        entityAttribute[datakeyEntity] = Date.parse($(this).val());
                    } else if (dataTypeEnitity == "string" || dataTypeEnitity === "long" || dataTypeEnitity === "int" || dataTypeEnitity === "boolean" || dataTypeEnitity == "date") {
                        entityAttribute[datakeyEntity] = $(this).val();
                    } else {
                        try {
                            if (value !== undefined && value !== null && value !== "") {
                                if (_.isArray(value)) {
                                    var arrayEmptyValueCheck = value.join("")
                                    if (arrayEmptyValueCheck === "") {
                                        return;
                                    }
                                    if (dataTypeEnitity === "array<string>" || dataTypeEnitity === "map<string,string>") {
                                        parseData = value;
                                    } else {
                                        if (that.selectStoreCollection.length) {
                                            var parseData = value.map(function(val) {
                                                var temp = {} // I9 support;
                                                temp[pickKey] = val;
                                                var valueData = that.selectStoreCollection.findWhere(temp).toJSON();
                                                valueData['guid'] = valueData.id;
                                                return valueData;
                                            })
                                        }
                                    }
                                } else {
                                    if (that.selectStoreCollection.length && pickKey) {

                                        var temp = {} // I9 support;
                                        temp[pickKey] = $(this).val();
                                        var parseData = that.selectStoreCollection.findWhere(temp).toJSON();
                                        parseData['guid'] = parseData.id || parseData['$id$'].id;
                                    }
                                    // Object but maptype
                                    if (!pickKey) {
                                        parseData = JSON.parse($(this).val());
                                    }
                                }
                                entityAttribute[datakeyEntity] = parseData
                                $(this).removeClass('errorClass');
                            }
                        } catch (e) {
                            $(this).addClass('errorClass');
                            that.validateError = e;
                            that.hideLoader();
                        }
                    }
                });
                var entityJson = {
                    "typeName": this.guid ? this.entityData.get("typeName") : this.parentEntity,
                    "attributes": entityAttribute
                };
                if (this.guid) {
                    entityJson["guid"] = this.entityData.get("guid");
                };
                if (that.validateError) {
                    if (that.validateMessage) {
                        Utils.notifyError({
                            content: "Please fill the required fields"
                        });
                    } else {
                        Utils.notifyError({
                            content: that.validateError.message
                        });
                    }
                    that.validateError = null;
                    that.hideLoader();
                } else {
                    this.entityModel.createOreditEntity(this.guid, {
                        data: JSON.stringify(entityJson),
                        type: this.guid ? "PUT" : "POST",
                        success: function(model, response) {
                            that.callback();
                            that.modal.close();
                            Utils.notifySuccess({
                                content: "entity " + Messages[that.guid ? 'editSuccessMessage' : 'addSuccessMessage']
                            });
                        },
                        error: function(response) {
                            if (response.responseJSON) {
                                Utils.notifyError({
                                    content: response.responseJSON.error || response.responseJSON.errorMessage
                                });
                            }
                        },
                        complete: function() {
                            that.hideLoader();
                        }
                    });
                }
            },
            showLoader: function() {
                this.$('.entityLoader').show();
                this.$('.entityInputData').hide();
            },
            hideLoader: function() {
                this.$('.entityLoader').hide();
                this.$('.entityInputData').show();
            },
            addJsonSearchData: function(isError) {
                var that = this,
                    typename,
                    str = '';
                if (isError) {
                    typename = isError.responseJSON.error.split(": ")[1];
                } else {
                    if (this.searchCollection.length) {
                        typename = this.searchCollection.first().get("$typeName$");
                        this.selectStoreCollection.push(this.searchCollection.fullCollection.models);
                        var labelName = "";
                        _.each(this.searchCollection.fullCollection.models, function(value, key) {
                            if (value.get("qualifiedName")) {
                                labelName = "qualifiedName";
                            } else if (value.get("name")) {
                                labelName = "name";
                            } else if (value.get("id")) {
                                labelName = "id";
                            }
                            str += '<option>' + _.escape(value.get(labelName)) + '</option>';
                        });
                    }
                }
                this.$('select[data-queryData="' + typename + '"]').html(str);
                this.$('select[data-queryData="' + typename + '"]').attr('data-pickkey', labelName);
                this.$('select[data-queryData="' + typename + '"]').each(function(value, key) {
                    var keyData = $(this).data("key");
                    var typeData = $(this).data("type");
                    var placeholderName = "Select a " + typename + " from the dropdown list";
                    var $this = $(this);
                    $this.attr("multiple", ($this.data('type').indexOf("array") === -1 ? false : true))
                    if (that.guid) {
                        if (that.selectStoreCollection.length) {
                            var selectedValue = [];
                        }
                        var dataValue = that.entityData.get("attributes")[keyData];
                        that.selectStoreCollection.each(function(value) {
                            if (dataValue !== null && _.isArray(dataValue)) {
                                _.each(dataValue, function(obj) {
                                    if (obj.guid === value.get("id")) {
                                        selectedValue.push(value.get("qualifiedName") || value.get("name") || value.get("id"));
                                    }
                                });
                            } else if (dataValue !== null) {
                                if (dataValue.guid === value.get("id")) {
                                    selectedValue.push(value.get("qualifiedName") || value.get("name") || value.get("id"));
                                }
                            }
                        });
                        if (selectedValue) {
                            $this.val(selectedValue);
                        } else {
                            if (that.guid) {
                                var dataValue = that.entityData.get("attributes")[keyData];
                                if (dataValue !== null) {
                                    _.each(dataValue, function(obj) {
                                        str += '<option>' + _.escape(obj) + '</option>';
                                    });
                                    $this.html(str);
                                }
                            }
                            $this.val(dataValue);
                        }
                    } else {
                        $this.val("");
                    }
                    $this.select2({
                        placeholder: placeholderName,
                        allowClear: true,
                        tags: true
                    });
                });
            }
        });
    return CreateEntityLayoutView;
});
