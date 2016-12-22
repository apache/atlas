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

define(['require', 'utils/Utils', 'modules/Modal', 'utils/Messages', 'utils/Enums'], function(require, Utils, Modal, Messages, Enums) {
    'use strict';

    var CommonViewFunction = {};
    CommonViewFunction.deleteTagModel = function(options) {
        var modal = new Modal({
            title: options.titleMessage,
            okText: options.buttonText,
            htmlContent: options.msg,
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
                    success: function(data) {
                        var msg = "Tag " + name.name + Messages.removeSuccessMessage;
                        if (options.tagOrTerm === "term") {
                            msg = "Term " + options.tagName + Messages.removeSuccessMessage;
                        } else if (options.tagOrTerm === "tag") {
                            msg = "Tag " + options.tagName + Messages.removeSuccessMessage;
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
    CommonViewFunction.propertyTable = function(valueObject, scope, searchTable) {
        var table = "",
            fetchInputOutputValue = function(id) {
                var that = this;
                if (searchTable) {
                    ++scope.fetchList
                }
                scope.entityModel.getEntity(id, {
                    success: function(data) {
                        var value = "",
                            deleteButton = "";

                        if (data && data.attributes) {
                            if (data.attributes.name) {
                                value = data.attributes.name;
                            } else if (data.attributes.qualifiedName) {
                                value = data.attributes.qualifiedName;
                            } else if (data.typeName) {
                                value = data.typeName;
                            }
                        }
                        var id = "";
                        if (data.guid) {
                            if (Enums.entityStateReadOnly[data.attributes.state]) {
                                deleteButton += '<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                            }
                            id = data.guid;

                        }
                        if (value.length > 1) {
                            scope.$('td div[data-id="' + id + '"]').html('<a href="#!/detailPage/' + id + '">' + _.escape(value) + '</a>');
                        } else {
                            scope.$('td div[data-id="' + id + '"]').html('<a href="#!/detailPage/' + id + '">' + _.escape(id) + '</a>');
                        }
                        if (deleteButton.length) {
                            scope.$('td div[data-id="' + id + '"]').addClass('block readOnlyLink');
                            scope.$('td div[data-id="' + id + '"]').append(deleteButton);
                        }
                    },
                    error: function(error, data, status) {},
                    complete: function() {
                        if (searchTable) {
                            --scope.fetchList;
                            scope.checkTableFetch();
                        }
                    }
                });
            }
        _.keys(valueObject).map(function(key) {
            key = _.escape(key)
            var keyValue = valueObject[key],
                valueOfArray = [];
            if (_.isArray(keyValue) || _.isObject(keyValue)) {
                if (!_.isArray(keyValue) && _.isObject(keyValue)) {
                    keyValue = [keyValue];
                }
                var subLink = "";
                for (var i = 0; i < keyValue.length; i++) {
                    var inputOutputField = keyValue[i],
                        id = inputOutputField.guid,
                        tempLink = "",
                        readOnly = false;
                    if (_.isString(inputOutputField) || _.isBoolean(inputOutputField) || _.isNumber(inputOutputField)) {
                        if (inputOutputField.indexOf("$") == -1) {
                            valueOfArray.push('<span>' + _.escape(inputOutputField) + '</span>');
                        }
                    } else if (_.isObject(inputOutputField) && !inputOutputField.attributes && !id) {
                        _.each(inputOutputField, function(objValue, objKey) {
                            var value = objValue;
                            if (objKey.indexOf("$") == -1) {
                                if (_.isObject(value)) {
                                    value = JSON.stringify(value);
                                }
                                valueOfArray.push('<span>' + _.escape(objKey) + ':' + _.escape(value) + '</span>');
                            }
                        });
                    }

                    if (id && inputOutputField) {
                        if (inputOutputField.attributes) {
                            if (inputOutputField.attributes.name) {
                                tempLink += '<a href="#!/detailPage/' + id + '">' + _.escape(inputOutputField.attributes.name) + '</a>'
                            } else if (inputOutputField.attributes.qualifiedName) {
                                tempLink += '<a href="#!/detailPage/' + id + '">' + _.escape(inputOutputField.attributes.qualifiedName) + '</a>'
                            } else if (inputOutputField.typeName) {
                                tempLink += '<a href="#!/detailPage/' + id + '">' + _.escape(inputOutputField.typeName) + '</a>'
                            } else {
                                tempLink += '<a href="#!/detailPage/' + id + '">' + id + '</a>'
                            }
                        } else if (inputOutputField.name) {
                            tempLink += '<a href="#!/detailPage/' + id + '">' + _.escape(inputOutputField.name) + '</a>';
                        } else if (inputOutputField.qualifiedName) {
                            tempLink += '<a href="#!/detailPage/' + id + '">' + _.escape(inputOutputField.qualifiedName) + '</a>'
                        } else {
                            var fetch = true;
                            fetchInputOutputValue(id);
                            tempLink += '<div data-id="' + id + '"></div>';
                        }
                    }

                    if (readOnly) {
                        if (!fetch) {
                            tempLink += '<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                            subLink += '<div class="block readOnlyLink">' + tempLink + '</div>';
                        } else {
                            fetch = false;
                            subLink += tempLink;
                        }

                    } else {
                        if (tempLink.search('href') != -1) {
                            subLink += '<div>' + tempLink + '</div>'
                        } else if (tempLink.length) {
                            subLink += tempLink
                        }
                    }
                }
                if (valueOfArray.length) {
                    subLink = valueOfArray.join(', ');
                }
                if (searchTable) {
                    table = subLink;
                } else {
                    table += '<tr><td>' + key + '</td><td>' + _.escape(subLink) + '</td></tr>';
                }
            } else {
                if (key.indexOf("Time") !== -1 || key == "retention") {
                    if (searchTable) {
                        table = new Date(valueObject[key]);
                    } else {
                        table += '<tr><td>' + key + '</td><td>' + new Date(valueObject[key]) + '</td></tr>';
                    }
                } else {
                    if (searchTable) {
                        if (_.isBoolean(valueObject[key])) {
                            table = valueObject[key].toString();
                        } else {
                            table = valueObject[key];
                        }
                    } else {
                        table += '<tr><td>' + key + '</td><td>' + _.escape(valueObject[key]) + '</td></tr>';
                    }
                }
            }
        });
        return table;
    }
    CommonViewFunction.breadcrumbUrlMaker = function(url) {
        if (url) {
            var urlList = [];
            var splitURL = url.split("api/atlas/v1/taxonomies/");
            if (splitURL.length > 1) {
                var splitUrlWithoutTerm = splitURL[1].split("/terms/");
                if (splitUrlWithoutTerm.length == 1) {
                    splitUrlWithoutTerm = splitUrlWithoutTerm[0].split("/");
                }
            } else {
                var splitUrlWithoutTerm = splitURL[0].split("/terms/");
                if (splitUrlWithoutTerm.length == 1) {
                    splitUrlWithoutTerm = splitUrlWithoutTerm[0].split("/");
                }
            }

            var href = "";
            for (var i in splitUrlWithoutTerm) {
                if (i == 0) {
                    href = splitUrlWithoutTerm[i];
                    urlList.push({
                        value: _.escape(splitUrlWithoutTerm[i]),
                        href: href
                    });
                } else {
                    href += "/terms/" + splitUrlWithoutTerm[i];
                    urlList.push({
                        value: _.escape(splitUrlWithoutTerm[i]),
                        href: href
                    });
                };
            }
            return urlList;
        }
    }
    CommonViewFunction.breadcrumbMaker = function(options) {
        var li = "";
        if (options.urlList) {
            _.each(options.urlList, function(object) {
                li += '<li><a class="link" href="#!/taxonomy/detailCatalog/api/atlas/v1/taxonomies/' + object.href + '?load=true">' + _.escape(object.value) + '</a></li>';
            });
        }
        if (options.scope) {
            options.scope.html(li);
            options.scope.asBreadcrumbs("destroy");
            options.scope.asBreadcrumbs({
                namespace: 'breadcrumb',
                overflow: "left",
                responsive: false,
                toggleIconClass: 'fa fa-ellipsis-h',
                dropdown: function(classes) {
                    var dropdownClass = 'dropdown';
                    var dropdownMenuClass = 'dropdown-menu popover popoverTerm bottom arrowPosition';
                    if (this.options.overflow === 'right') {
                        dropdownMenuClass += ' dropdown-menu-right';
                    }

                    return '<li class="' + dropdownClass + ' ' + classes.dropdownClass + '">' +
                        '<a href="javascript:void(0);" class="' + classes.toggleClass + '" data-toggle="dropdown">' +
                        '<i class="' + classes.toggleIconClass + '"></i>' +
                        '</a>' +
                        '<ul class="' + dropdownMenuClass + ' ' + classes.dropdownMenuClass + '">' +
                        '<div class="arrow"></div>' +
                        '</ul>' +
                        '</li>';
                }
            });
        }
    }
    CommonViewFunction.termTableBreadcrumbMaker = function(model) {
        if (!model) {
            return "";
        }
        var traits = model.get('$traits$'),
            url = "",
            deleteHtml = "",
            html = "",
            id = model.get('$id$').id || model.get('$id$'),
            terms = [];
        _.keys(traits).map(function(key) {
            if (traits[key]) {
                var tagName = Utils.checkTagOrTerm(traits[key]);
            }
            if (tagName.term) {
                terms.push({
                    deleteHtml: '<a class="pull-left" title="Remove Term"><i class="fa fa-trash" data-id="tagClick" data-type="term" data-assetname="' + _.escape(model.get("name")) + '" data-name="' + tagName.fullName + '" data-guid="' + (model.get('$id$').id || model.get('$id$')) + '" ></i></a>',
                    url: _.unescape(tagName.fullName).split(".").join("/"),
                    name: tagName.fullName
                });
            }
        });
        _.each(terms, function(obj, i) {
            var className = "";
            if (i >= 1) {
                className += "showHideDiv hide";
            }
            obj['valueUrl'] = CommonViewFunction.breadcrumbUrlMaker(obj.url);
            html += '<div class="' + className + '" dataterm-name="' + _.escape(obj.name) + '"><div class="liContent"></div>' + obj.deleteHtml + '</div>';
        })
        if (terms.length > 1) {
            html += '<div><a  href="javascript:void(0)" data-id="showMoreLessTerm" class="inputTag inputTagGreen"><span>Show More </span><i class="fa fa-angle-right"></i></a></div>'
        }
        if (model.get('$id$')) {
            html += '<div><a href="javascript:void(0)" class="inputAssignTag" data-id="addTerm" data-guid="' + (model.get('$id$').id || model.get('$id$')) + '"><i class="fa fa-folder-o"></i>' + " " + 'Assign Term</a></div>'
        } else {
            html += '<div><a href="javascript:void(0)" class="inputAssignTag" data-id="addTerm"><i class="fa fa-folder-o"></i>' + " " + 'Assign Term</a></div>'
        }
        return {
            html: '<div class="termTableBreadcrumb" dataterm-id="' + id + '">' + html + '</div>',
            object: { scopeId: id, value: terms }
        }

    }
    CommonViewFunction.tagForTable = function(model) {
        var traits = model.get('$traits$'),
            atags = "",
            addTag = "",
            popTag = "",
            count = 0;
        _.keys(model.get('$traits$')).map(function(key) {
            if (traits[key]) {
                var tagName = Utils.checkTagOrTerm(traits[key]);
            }
            var className = "inputTag";
            if (tagName.tag) {
                if (count >= 1) {
                    popTag += '<a class="' + className + '" data-id="tagClick"><span class="inputValue">' + tagName.fullName + '</span><i class="fa fa-times" data-id="delete"  data-assetname="' + model.get("name") + '"data-name="' + tagName.fullName + '" data-type="tag" data-guid="' + (model.get('$id$').id || model.get('$id$')) + '" ></i></a>';
                } else {
                    atags += '<a class="' + className + '" data-id="tagClick"><span class="inputValue">' + tagName.fullName + '</span><i class="fa fa-times" data-id="delete" data-assetname="' + model.get("name") + '" data-name="' + tagName.fullName + '"  data-type="tag" data-guid="' + (model.get('$id$').id || model.get('$id$')) + '" ></i></a>';
                }
                ++count;
            }
        });
        if (model.get('$id$')) {
            addTag += '<a href="javascript:void(0)" data-id="addTag" class="inputTagAdd assignTag" data-guid="' + (model.get('$id$').id || model.get('$id$')) + '" ><i class="fa fa-plus"></i></a>';
        } else {
            addTag += '<a href="javascript:void(0)" data-id="addTag" class="inputTagAdd assignTag"><i style="right:0" class="fa fa-plus"></i></a>';
        }
        if (count > 1) {
            addTag += '<div data-id="showMoreLess" class="inputTagAdd assignTag tagDetailPopover"><i class="fa fa-ellipsis-h" aria-hidden="true"></i></div>'
        }
        return '<div class="tagList">' + atags + addTag + '<div class="popover popoverTag bottom" style="display:none"><div class="arrow"></div><div class="popover-content popoverContainer">' + popTag + '</div></div></div>';
    }
    CommonViewFunction.saveTermToAsset = function(options, that) {
        require(['models/VCatalog'], function(Vcatalog) {
            var VCatalog = new Vcatalog();
            var name = options.termName;
            VCatalog.url = function() {
                return "api/atlas/v1/entities/" + options.guid + "/tags/" + name;
            };
            ++that.asyncFetchCounter;
            VCatalog.save(null, {
                success: function(data) {
                    Utils.notifySuccess({
                        content: "Term " + name + Messages.addTermToEntitySuccessMessage
                    });
                    if (options.collection) {
                        options.collection.fetch({ reset: true });
                    }
                },
                error: function(error, data, status) {
                    if (data && data.responseText) {
                        var data = JSON.parse(data.responseText);
                        Utils.notifyError({
                            content: data.message || data.msgDesc
                        });
                    }
                },
                complete: function() {
                    --that.asyncFetchCounter
                    if (that.callback && that.asyncFetchCounter === 0) {
                        that.callback(); // It will call to parent of parent Callback i.e callback of searchLayoutView
                    }
                }
            });
        })
    }
    CommonViewFunction.addRestCsrfCustomHeader = function(xhr, settings) {
        //    if (settings.url == null || !settings.url.startsWith('/webhdfs/')) {
        if (settings.url == null) {
            return;
        }
        var method = settings.type;
        if (CommonViewFunction.restCsrfCustomHeader != null && !CommonViewFunction.restCsrfMethodsToIgnore[method]) {
            // The value of the header is unimportant.  Only its presence matters.
            xhr.setRequestHeader(CommonViewFunction.restCsrfCustomHeader, '""');
        }
    }
    CommonViewFunction.restCsrfCustomHeader = null;
    CommonViewFunction.restCsrfMethodsToIgnore = null;
    CommonViewFunction.userDataFetch = function(options) {
        var csrfEnabled = false,
            header = null,
            methods = [];

        function getTrimmedStringArrayValue(string) {
            var str = string,
                array = [];
            if (str) {
                var splitStr = str.split(',');
                for (var i = 0; i < splitStr.length; i++) {
                    array.push(splitStr[i].trim());
                }
            }
            return array;
        }
        if (options.url) {
            $.ajax({
                url: options.url,
                success: function(response) {
                    if (response) {
                        if (response['atlas.rest-csrf.enabled']) {
                            var str = "" + response['atlas.rest-csrf.enabled'];
                            csrfEnabled = (str.toLowerCase() == 'true');
                        }
                        if (response['atlas.rest-csrf.custom-header']) {
                            header = response['atlas.rest-csrf.custom-header'].trim();
                        }
                        if (response['atlas.rest-csrf.methods-to-ignore']) {
                            methods = getTrimmedStringArrayValue(response['atlas.rest-csrf.methods-to-ignore']);
                        }
                        if (csrfEnabled) {
                            CommonViewFunction.restCsrfCustomHeader = header;
                            CommonViewFunction.restCsrfMethodsToIgnore = {};
                            methods.map(function(method) { CommonViewFunction.restCsrfMethodsToIgnore[method] = true; });
                            Backbone.$.ajaxSetup({
                                beforeSend: CommonViewFunction.addRestCsrfCustomHeader
                            });
                        }
                    }
                },
                complete: function(response) {
                    if (options.callback) {
                        options.callback(response.responseJSON);
                    }
                }
            });
        }
    }
    return CommonViewFunction;
});
