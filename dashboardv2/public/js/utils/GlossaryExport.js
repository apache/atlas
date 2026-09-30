/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

define(['require', 'jquery', 'underscore', 'utils/UrlLinks', 'utils/CommonViewFunction'], function(require, $, _, UrlLinks, CommonViewFunction) {
    'use strict';

    var SEARCH_DEFAULTS = {
        limit: 25,
        offset: 0,
        sortBy: 'glossaryName',
        sortOrder: 'DESCENDING'
    };

    var STATUS_FILTER_OPTIONS = [
        { label: 'All', value: '' },
        { label: 'Draft', value: 'DRAFT' },
        { label: 'Active', value: 'ACTIVE' },
        { label: 'Deprecated', value: 'DEPRECATED' }
    ];

    var STATUS_META = {
        ACTIVE: { label: 'Active', shortLabel: 'Active', cssClass: 'glossary-status-badge glossary-status-active' },
        DRAFT: { label: 'Draft', shortLabel: 'Draft', cssClass: 'glossary-status-badge glossary-status-draft' },
        DEPRECATED: { label: 'Deprecated', shortLabel: 'Depr.', cssClass: 'glossary-status-badge glossary-status-deprecated' },
        UNKNOWN: { label: 'Unknown', shortLabel: '—', cssClass: 'glossary-status-badge glossary-status-unknown' }
    };

    var empty = function(v) {
        return v === null || v === undefined ? '' : String(v);
    };

    var normalizeStatus = function(status) {
        if (!status) {
            return 'UNKNOWN';
        }
        var normalized = String(status).trim().toUpperCase();
        if (normalized === 'ACTIVE' || normalized === 'DRAFT' || normalized === 'DEPRECATED') {
            return normalized;
        }
        return 'UNKNOWN';
    };

    var getStatusMeta = function(status) {
        return STATUS_META[normalizeStatus(status)];
    };

    var createDefaultFilters = function() {
        return {
            recordType: 'all',
            glossaryName: '',
            statusFilter: '',
            searchText: ''
        };
    };

    var mapRecordTypeToGlossaryType = function(recordType) {
        if (recordType === 'Term') {
            return 'TERM';
        }
        if (recordType === 'Category') {
            return 'CATEGORY';
        }
        return undefined;
    };

    var buildGlossaryGuidByName = function(glossaryCollection) {
        var map = {};
        if (!glossaryCollection || !glossaryCollection.fullCollection) {
            return map;
        }
        glossaryCollection.fullCollection.each(function(model) {
            var name = model.get('name');
            var guid = model.get('guid');
            if (name && guid) {
                map[name] = guid;
            }
        });
        return map;
    };

    var buildSearchRequest = function(filters, pageIndex, rowsPerPage, glossaryGuidByName, sortOptions) {
        glossaryGuidByName = glossaryGuidByName || {};
        sortOptions = sortOptions || {};
        var request = {
            limit: rowsPerPage,
            offset: pageIndex * rowsPerPage,
            sortBy: sortOptions.sortBy || SEARCH_DEFAULTS.sortBy,
            sortOrder: sortOptions.sortOrder || SEARCH_DEFAULTS.sortOrder,
            excludeDeleted: true
        };
        var glossaryType = mapRecordTypeToGlossaryType(filters.recordType);
        if (glossaryType) {
            request.glossaryType = glossaryType;
        }
        if (filters.statusFilter && String(filters.statusFilter).trim()) {
            request.status = String(filters.statusFilter).trim();
        }
        if (filters.searchText && String(filters.searchText).trim()) {
            request.searchQuery = String(filters.searchText).trim();
        }
        if (filters.glossaryName && String(filters.glossaryName).trim()) {
            var glossaryName = String(filters.glossaryName).trim();
            request.glossary = {
                name: glossaryName,
                guid: glossaryGuidByName[glossaryName]
            };
        }
        return request;
    };

    var formatCustomAttributesForDisplay = function(attrs) {
        if (!attrs || typeof attrs !== 'object' || _.isEmpty(attrs)) {
            return '';
        }
        return _.map(attrs, function(value, key) {
            return key + ': ' + empty(value);
        }).join(', ');
    };

    var formatCustomAttributesForTooltip = function(attrs) {
        if (!attrs || typeof attrs !== 'object' || _.isEmpty(attrs)) {
            return '';
        }
        try {
            return JSON.stringify(attrs, null, 2);
        } catch (e) {
            return formatCustomAttributesForDisplay(attrs);
        }
    };

    var mapDetailToRow = function(detail, recordType, glossaryName, glossaryGuid) {
        return {
            id: recordType.toLowerCase() + '-' + (empty(detail.guid) || empty(detail.name)),
            recordType: recordType,
            name: empty(detail.name),
            guid: empty(detail.guid),
            qualifiedName: empty(detail.qualifiedName),
            glossaryName: glossaryName,
            glossaryGuid: glossaryGuid,
            shortDescription: empty(detail.shortDescription),
            longDescription: empty(detail.longDescription),
            status: empty(detail.status),
            classifications: detail.classifications || [],
            customAttributes: detail.customAttributes || {}
        };
    };

    var mapSearchResponseToRows = function(response) {
        var glossaries = response && response.glossary;
        if (!_.isArray(glossaries)) {
            return [];
        }
        var rows = [];
        _.each(glossaries, function(glossary) {
            var glossaryName = empty(glossary && glossary.name);
            var glossaryGuid = empty(glossary && glossary.guid);
            _.each(glossary.terms || [], function(term) {
                rows.push(mapDetailToRow(term, 'Term', glossaryName, glossaryGuid));
            });
            _.each(glossary.categories || [], function(category) {
                rows.push(mapDetailToRow(category, 'Category', glossaryName, glossaryGuid));
            });
        });
        return rows;
    };

    var unwrapSearchResponse = function(response) {
        if (response && response.glossary) {
            return response;
        }
        if (response && response.data && response.data.glossary) {
            return response.data;
        }
        return response || {};
    };

    var parseApproximateCount = function(value, fallback) {
        var parsed = Number(value);
        if (_.isFinite(parsed) && parsed >= 0) {
            return parsed;
        }
        return fallback;
    };

    var postJson = function(url, data) {
        return $.ajax({
            url: url,
            type: 'POST',
            contentType: 'application/json',
            dataType: 'json',
            data: JSON.stringify(data),
            beforeSend: CommonViewFunction.addRestCsrfCustomHeader
        });
    };

    var fetchSearchPage = function(request) {
        var deferred = $.Deferred();
        postJson(UrlLinks.glossarySearchUrl(), request).done(function(response) {
            var body = unwrapSearchResponse(response);
            var rows = mapSearchResponseToRows(body);
            var totalCount = parseApproximateCount(body.approximateCount, rows.length);
            deferred.resolve({
                rows: rows,
                totalCount: totalCount
            });
        }).fail(function(xhr) {
            deferred.reject(xhr);
        });
        return deferred.promise();
    };

    var buildSearchExportRequest = function(filters, glossaryGuidByName, format) {
        var request = buildSearchRequest(filters, 0, 0, glossaryGuidByName, {});
        request.limit = 0;
        request.offset = 0;
        if (format) {
            request.format = format;
        }
        return request;
    };

    var requestExportDownload = function(filters, glossaryGuidByName, format) {
        var deferred = $.Deferred();
        postJson(
            UrlLinks.glossaryCreateFileUrl(),
            buildSearchExportRequest(filters, glossaryGuidByName, format || 'CSV')
        ).done(function() {
            deferred.resolve();
        }).fail(function(xhr) {
            deferred.reject(xhr);
        });
        return deferred.promise();
    };

    var formatStatusHtml = function(status) {
        var meta = getStatusMeta(status);
        return '<span class="' + meta.cssClass + '" title="Status: ' + _.escape(meta.label) + '">' +
            _.escape(meta.shortLabel) + '</span>';
    };

    var formatClassificationsHtml = function(classifications) {
        if (!_.isArray(classifications) || classifications.length === 0) {
            return '';
        }
        return _.map(classifications, function(classification) {
            if (!classification || !classification.typeName) {
                return '';
            }
            var typeName = _.escape(classification.typeName);
            return '<a href="#!/tag/tagAttribute/' + typeName + '" class="btn btn-action btn-sm btn-blue">' +
                typeName + '</a>';
        }).join(' ');
    };

    var buildGlossaryDetailUrl = function(row) {
        var gType = row.recordType === 'Term' ? 'term' : 'category';
        var params = {
            gId: row.glossaryGuid,
            gType: gType,
            viewType: gType,
            fromView: 'entity'
        };
        if (row.recordType === 'Term' && row.qualifiedName) {
            params.term = row.qualifiedName;
        }
        var query = _.map(params, function(value, key) {
            return encodeURIComponent(key) + '=' + encodeURIComponent(value);
        }).join('&');
        return '#!/glossary/' + row.guid + (query ? '?' + query : '');
    };

    var formatNameHtml = function(row) {
        var name = _.escape(row.name);
        if (!row.guid) {
            return name;
        }
        return '<a href="' + buildGlossaryDetailUrl(row) + '" class="entity-name nav-link">' + name + '</a>';
    };

    return {
        SEARCH_DEFAULTS: SEARCH_DEFAULTS,
        STATUS_FILTER_OPTIONS: STATUS_FILTER_OPTIONS,
        createDefaultFilters: createDefaultFilters,
        buildGlossaryGuidByName: buildGlossaryGuidByName,
        buildSearchRequest: buildSearchRequest,
        buildSearchExportRequest: buildSearchExportRequest,
        formatCustomAttributesForDisplay: formatCustomAttributesForDisplay,
        formatCustomAttributesForTooltip: formatCustomAttributesForTooltip,
        mapSearchResponseToRows: mapSearchResponseToRows,
        fetchSearchPage: fetchSearchPage,
        requestExportDownload: requestExportDownload,
        formatStatusHtml: formatStatusHtml,
        formatClassificationsHtml: formatClassificationsHtml,
        formatNameHtml: formatNameHtml,
        getStatusMeta: getStatusMeta
    };
});
