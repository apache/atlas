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

define(['require', 'utils/Utils', 'modules/Modal', 'utils/Messages', 'utils/Globals'], function(require, Utils, Modal, Messages, Globals) {
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
                        if (data.traitName) {
                            var tagOrTerm = Utils.checkTagOrTerm(data.traitName);
                            if (tagOrTerm.term) {
                                msg = "Term " + data.traitName + Messages.removeSuccessMessage;
                            } else {
                                msg = "Tag " + data.traitName + Messages.removeSuccessMessage;
                            }
                        } else {
                            var tagOrTerm = Utils.checkTagOrTerm(options.tagName);
                            if (tagOrTerm.term) {
                                msg = "Term " + data.traitName + Messages.removeSuccessMessage;
                            } else {
                                msg = "Tag " + data.traitName + Messages.removeSuccessMessage;
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
                    success: function(data) {
                        var value = "",
                            deleteButton = "";
                        if (data.definition.values.name) {
                            value = data.definition.values.name;
                        } else if (data.definition.values.qualifiedName) {
                            value = data.definition.values.qualifiedName;
                        } else if (data.definition.typeName) {
                            value = data.definition.typeName;
                        }
                        var id = "";
                        if (data.definition.id) {
                            if (_.isObject(data.definition.id) && data.definition.id.id) {
                                id = data.definition.id.id;
                                if (Globals.entityStateReadOnly[data.definition.id.state]) {
                                    deleteButton += '<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                                }
                            } else {
                                id = data.definition.id;
                            }
                        }
                        if (value.length > 1) {
                            scope.$('td div[data-id="' + id + '"]').html('<a href="#!/detailPage/' + id + '">' + value + '</a>');
                        } else {
                            scope.$('td div[data-id="' + id + '"]').html('<a href="#!/detailPage/' + id + '">' + id + '</a>');
                        }
                        if (deleteButton.length) {
                            scope.$('td div[data-id="' + id + '"]').addClass('block readOnlyLink');
                            scope.$('td div[data-id="' + id + '"]').append(deleteButton);
                        }
                    },
                    error: function(error, data, status) {},
                    complete: function() {}
                });
            }
        _.keys(valueObject).map(function(key) {
            var keyValue = valueObject[key],
                valueOfArray = [];
            if (_.isArray(keyValue)) {
                var subLink = "";
                for (var i = 0; i < keyValue.length; i++) {
                    var inputOutputField = keyValue[i],
                        id = undefined,
                        tempLink = "",
                        readOnly = false;
                    if (inputOutputField) {
                        if (_.isObject(inputOutputField.id)) {
                            id = inputOutputField.id.id;
                            if (Globals.entityStateReadOnly[inputOutputField.id.state]) {
                                readOnly = inputOutputField.id.state
                            }
                        } else if (inputOutputField.id) {
                            id = inputOutputField.id;
                        } else if (_.isString(inputOutputField) || _.isBoolean(inputOutputField) || _.isNumber(inputOutputField)) {
                            valueOfArray.push('<span>' + inputOutputField + '</span>');
                        } else if (_.isObject(inputOutputField)) {
                            _.each(inputOutputField, function(objValue, objKey) {
                                var value = objValue;
                                if (_.isObject(value)) {
                                    value = JSON.stringify(value);
                                }
                                valueOfArray.push('<span>' + objKey + ':' + value + '</span>');
                            });
                        }
                    }

                    if (id) {
                        if (inputOutputField.values) {
                            if (inputOutputField.values.name) {
                                tempLink += '<a href="#!/detailPage/' + id + '">' + inputOutputField.values.name + '</a>'
                            } else if (inputOutputField.values.qualifiedName) {
                                tempLink += '<a href="#!/detailPage/' + id + '">' + inputOutputField.values.qualifiedName + '</a>'
                            } else if (inputOutputField.typeName) {
                                tempLink += '<a href="#!/detailPage/' + id + '">' + inputOutputField.typeName + '</a>'
                            } else {
                                tempLink += '<a href="#!/detailPage/' + id + '">' + id + '</a>'
                            }
                        } else {
                            fetchInputOutputValue(id);
                            tempLink += '<div data-id="' + id + '"></div>';
                        }
                    }
                    if (readOnly) {
                        tempLink += '<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                        subLink += '<div class="block readOnlyLink">' + tempLink + '</div>';
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
                table += '<tr><td>' + key + '</td><td>' + subLink + '</td></tr>';
            } else if (_.isObject(keyValue)) {
                var id = undefined,
                    tempLink = "",
                    readOnly = false;
                if (_.isObject(keyValue.id)) {
                    id = keyValue.id.id;
                    if (Globals.entityStateReadOnly[keyValue.id.state]) {
                        readOnly = keyValue.id.state
                    }
                } else {
                    id = keyValue.id;
                }
                if (id) {
                    if (keyValue.values) {
                        if (keyValue.values.name) {
                            tempLink += '<a href="#!/detailPage/' + id + '">' + keyValue.values.name + '</a>';
                        } else if (keyValue.values.qualifiedName) {
                            tempLink += '<a href="#!/detailPage/' + id + '">' + keyValue.values.qualifiedName + '</a>'
                        } else if (keyValue.typeName) {
                            tempLink += '<a href="#!/detailPage/' + id + '">' + keyValue.typeName + '</a>'
                        } else {
                            tempLink += '<a href="#!/detailPage/' + id + '">' + id + '</a>';
                        }
                    } else {
                        fetchInputOutputValue(id);
                        tempLink += '<div data-id="' + id + '"></div>';
                    }
                    if (readOnly) {
                        tempLink += '<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                        table += '<tr><td>' + key + '</td><td><div class="block readOnlyLink">' + tempLink + '</div></td></tr>';
                    } else {
                        table += '<tr><td>' + key + '</td><td>' + tempLink + '</td></tr>';
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
                        tempLink += '<div>' + jointValues + '</div>';
                    }
                    if (readOnly) {
                        tempLink += '<button title="Deleted" class="btn btn-atlasAction btn-atlas deleteBtn"><i class="fa fa-trash"></i></button>';
                        table += '<tr><td>' + key + '</td><td><div class="block readOnlyLink">' + tempLink + '</div></td></tr>';
                    } else {
                        table += '<tr><td>' + key + '</td><td>' + tempLink + '</td></tr>';
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
                        value: splitUrlWithoutTerm[i],
                        href: href
                    });
                } else {
                    href += "/terms/" + splitUrlWithoutTerm[i];
                    urlList.push({
                        value: splitUrlWithoutTerm[i],
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
                li += '<li><a href="javascript:void(0)" class="link" data-href="/api/atlas/v1/taxonomies/' + object.href + '">' + object.value + '</a></li>';
            });
        }
        if (options.scope) {
            options.scope.html(li);
            options.scope.asBreadcrumbs("destroy");
            options.scope.asBreadcrumbs({
                namespace: 'breadcrumb',
                overflow: "left",
                dropicon: "fa fa-ellipsis-h",
                responsive: false,
                dropdown: function() {
                    return '<div class=\"dropdown\">' +
                        '<a href=\"javascript:void(0);\" class=\"' + this.namespace + '-toggle\" data-toggle=\"dropdown\"><i class=\"' + this.dropicon + '\"</i></a>' +
                        '<ul class=\"' + this.namespace + '-menu dropdown-menu popover popoverTerm bottom arrowPosition \" ><div class="arrow"></div></ul>' +
                        '</div>';
                },
                dropdownContent: function(a) {
                    return '<li><a class="link" href="javascript:void(0)" data-href="' + a.find('a').data('href') + '" class="dropdown-item">' + a.text() + "</a></li>";
                }
            });
        }
        options.scope.find('li a.link').click(function() {
            Utils.setUrl({
                url: "#!/taxonomy/detailCatalog" + $(this).data('href') + "?load=true",
                mergeBrowserUrl: false,
                trigger: true,
                updateTabState: function() {
                    return { taxonomyUrl: this.url, stateChanged: false };
                }
            });
        });
    }
    CommonViewFunction.termTableBreadcrumbMaker = function(model) {
        var traits = model.get('$traits$'),
            url = "",
            deleteHtml = "",
            html = "",
            id = model.get('$id$').id,
            terms = [];
        _.keys(traits).map(function(key) {
            var tagName = Utils.checkTagOrTerm(traits[key].$typeName$);
            if (tagName.term) {
                terms.push({
                    deleteHtml: '<a class="pull-left" title="Remove Term"><i class="fa fa-trash" data-id="tagClick" data-assetname="' + model.get("name") + '" data-name="' + traits[key].$typeName$ + '" data-guid="' + model.get('$id$').id + '" ></i></a>',
                    url: traits[key].$typeName$.split(".").join("/"),
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
            html += '<div class="' + className + '" dataterm-name="' + obj.name + '"><div class="liContent"></div>' + obj.deleteHtml + '</div>';
        })
        if (terms.length > 1) {
            html += '<div><a  href="javascript:void(0)" data-id="showMoreLessTerm" class="inputTag inputTagGreen"><span>Show More </span><i class="fa fa-angle-right"></i></a></div>'
        }
        if (model.get('$id$')) {
            html += '<div><a href="javascript:void(0)" class="inputAssignTag" data-id="addTerm" data-guid="' + model.get('$id$').id + '"><i class="fa fa-folder-o"></i>' + " " + 'Assign Term</a></div>'
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
            var tagName = Utils.checkTagOrTerm(traits[key].$typeName$),
                className = "inputTag";
            if (!tagName.term) {
                if (count >= 1) {
                    popTag += '<a class="' + className + '" data-id="tagClick"><span class="inputValue">' + traits[key].$typeName$ + '</span><i class="fa fa-times" data-id="delete"  data-assetname="' + model.get("name") + '"data-name="' + tagName.name + '" data-guid="' + model.get('$id$').id + '" ></i></a>';
                } else {
                    atags += '<a class="' + className + '" data-id="tagClick"><span class="inputValue">' + traits[key].$typeName$ + '</span><i class="fa fa-times" data-id="delete" data-assetname="' + model.get("name") + '" data-name="' + tagName.name + '" data-guid="' + model.get('$id$').id + '" ></i></a>';
                }
                ++count;
            }
        });
        if (model.get('$id$')) {
            addTag += '<a href="javascript:void(0)" data-id="addTag" class="inputTagAdd" data-guid="' + model.get('$id$').id + '" ><i class="fa fa-plus"></i></a>';
        } else {
            addTag += '<a href="javascript:void(0)" data-id="addTag" class="inputTagAdd"><i style="right:0" class="fa fa-plus"></i></a>';
        }
        if (count > 1) {
            addTag += '<div data-id="showMoreLess" class="inputTagAdd tagDetailPopover"><i class="fa fa-ellipsis-h" aria-hidden="true"></i></div>'
        }
        return '<div class="tagList">' + atags + addTag + '<div class="popover popoverTag bottom" style="display:none"><div class="arrow"></div><div class="popover-content popoverContainer">' + popTag + '</div></div></div>';
    }
    CommonViewFunction.saveTermToAsset = function(options) {
        require(['models/VCatalog'], function(Vcatalog) {
            var VCatalog = new Vcatalog();
            var name = options.termName;
            VCatalog.url = function() {
                return "api/atlas/v1/entities/" + options.guid + "/tags/" + name;
            };
            VCatalog.save(null, {
                success: function(data) {
                    Utils.notifySuccess({
                        content: "Term " + name + Messages.addTermToEntitySuccessMessage
                    });
                    if (options.callback) {
                        options.callback();
                    }
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
                        if (options.callback) {
                            options.callback();
                        }
                    }
                },
                complete: function() {}
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
