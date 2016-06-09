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

define(['require', 'utils/Utils', 'modules/Modal', 'utils/Messages'], function(require, Utils, Modal, Messages) {
    'use strict';

    var CommonViewFunction = {};
    CommonViewFunction.deleteTagModel = function(tagName) {
        var msg = "<b>Tag:</b>";
        if (tagName) {
            var tagOrTerm = Utils.checkTagOrTerm(tagName);
            if (tagOrTerm.term) {
                msg = "<b>Term: " + tagName + "</b>";
            } else {
                msg = "<b>Tag: " + tagName + "</b>";
            }
        }
        var modal = new Modal({
            title: Messages.deleteTitle,
            okText: 'Delete',
            htmlContent: msg,
            cancelText: "Cancel",
            allowCancel: true,
            okCloses: true,
            showFooter: true,
        }).open();
        return modal;
    };
    CommonViewFunction.deleteTag = function(options) {
        require(['models/VTag'], function(VTag) {
            var tagModel = new VTag();
            if (options && options.guid && options.tagName) {

                tagModel.deleteTag(options.guid, options.tagName, {
                    beforeSend: function() {},
                    success: function(data) {
                        var msg = "Tag " + name.name + Messages.deleteSuccessMessage;
                        if (data.traitName) {
                            var tagOrTerm = Utils.checkTagOrTerm(data.traitName);
                            if (tagOrTerm.term) {
                                msg = "Term " + data.traitName + Messages.deleteSuccessMessage;
                            } else {
                                msg = "Tag " + data.traitName + Messages.deleteSuccessMessage;
                            }
                        } else {
                            var tagOrTerm = Utils.checkTagOrTerm(options.tagName);
                            if (tagOrTerm.term) {
                                msg = "Term " + data.traitName + Messages.deleteSuccessMessage;
                            } else {
                                msg = "Tag " + data.traitName + Messages.deleteSuccessMessage;
                            }
                        }
                        Utils.notifySuccess({
                            content: msg
                        });
                        if (options.callback) {
                            options.callback();
                        }
                        if (options.collection) {
                            options.collection.fetch({ reset: true });
                        }

                    },
                    error: function(error, data, status) {
                        var message = options.tagName + Messages.deleteErrorMessage;
                        if (data.error) {
                            message = data.error;
                        }
                        Utils.notifyError({
                            content: message
                        });
                    },
                    complete: function() {}
                });
            }
        });
    };
    CommonViewFunction.propertyTable = function(valueObject, scope) {
        var table = "",
            fetchInputOutputValue = function(id) {
                var that = this;
                scope.model.getEntity(id, {
                    beforeSend: function() {},
                    success: function(data) {
                        var value = "";
                        if (data.definition.values.name) {
                            value = data.definition.values.name;
                        }
                        var id = "";
                        if (data.definition.id) {
                            if (_.isObject(data.definition.id) && data.definition.id.id) {
                                id = data.definition.id.id;
                            } else {
                                id = data.definition.id;
                            }
                        }
                        if (value.length > 1) {
                            scope.$('td div[data-id="' + id + '"]').html('<a href="#!/detailPage/' + id + '">' + value + '</a>');
                        } else {
                            scope.$('td div[data-id="' + id + '"]').html('<a href="#!/detailPage/' + id + '">' + id + '</a>');
                        }

                    },
                    error: function(error, data, status) {},
                    complete: function() {}
                });
            }
        _.keys(valueObject).map(function(key) {
            var keyValue = valueObject[key];
            if (_.isArray(keyValue)) {
                var subLink = "";
                for (var i = 0; i < keyValue.length; i++) {
                    var inputOutputField = keyValue[i],
                        id = undefined;
                    if (_.isObject(inputOutputField.id)) {
                        id = inputOutputField.id.id;
                    } else {
                        id = inputOutputField.id;
                    }
                    if (id) {
                        if (inputOutputField.values) {
                            if (inputOutputField.values.name) {
                                subLink += '<div><a href="#!/detailPage/' + id + '">' + inputOutputField.values.name + '</a><div>'
                            } else {
                                subLink += '<a href="#!/detailPage/' + id + '">' + id + '</a>'
                            }
                        } else {
                            fetchInputOutputValue(id);
                            subLink += '<div data-id="' + id + '"></div>';
                        }

                    } else {
                        subLink += '<div></div>';
                    }
                }
                table += '<tr><td>' + key + '</td><td>' + subLink + '</td></tr>';
            } else if (_.isObject(keyValue)) {
                var id = undefined;
                if (_.isObject(keyValue.id)) {
                    id = keyValue.id.id;
                } else {
                    id = keyValue.id;
                }
                if (id) {
                    if (keyValue.values) {
                        if (keyValue.values.name) {
                            table += '<tr><td>' + key + '</td><td><div><a href="#!/detailPage/' + id + '">' + keyValue.values.name + '</a><div></td></tr>';
                        } else {
                            table += '<tr><td>' + key + '</td><td><div><a href="#!/detailPage/' + id + '">' + id + '</a><div></td></tr>';
                        }
                    } else {
                        fetchInputOutputValue(id);
                        table += '<tr><td>' + key + '</td><td><div data-id="' + id + '"></div></td></tr>';
                    }
                } else {
                    var stringArr = [];
                    _.each(keyValue, function(val, key) {
                        var value = "";
                        if (_.isObject(val)) {
                            value = JSON.stringify(val);
                        } else {
                            value = val;
                        }
                        var attrName = "<span>" + key + " : " + value + "</span>";
                        stringArr.push(attrName);
                    });
                    var jointValues = stringArr.join(", ");
                    if (jointValues.length) {
                        table += '<tr><td>' + key + '</td><td><div>' + jointValues + '</div></td></tr>';
                    } else {
                        table += '<tr><td>' + key + '</td><td></td></tr>';
                    }
                }
            } else {
                if (key == "createTime" || key == "lastAccessTime" || key == "retention") {
                    table += '<tr><td>' + key + '</td><td>' + new Date(valueObject[key]) + '</td></tr>';
                } else {
                    table += '<tr><td>' + key + '</td><td>' + valueObject[key] + '</td></tr>';
                }

            }
        });
        return table;
    }
    CommonViewFunction.userDataFetch = function(options) {
        if (options.url) {
            $.ajax({
                url: options.url,
                success: function(response) {
                    if (options.callback) {
                        options.callback(response);
                    }
                }
            });
        }
    }
    return CommonViewFunction;
});
