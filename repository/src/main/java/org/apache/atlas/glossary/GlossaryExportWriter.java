/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.glossary;

import com.opencsv.CSVWriter;
import org.apache.atlas.model.glossary.GlossaryExportParameters;
import org.apache.atlas.model.glossary.GlossaryExportRow;
import org.apache.commons.collections.CollectionUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GlossaryExportWriter {
    private static final int MAX_CELL_LENGTH = 32767;

    private static final String[] AUDIT_HEADERS = {
            "Record Type", "Name", "Glossary Name", "Short Description", "Long Description",
            "Status", "Classifications", "Custom Attributes", "Related Categories / Parent",
            "Qualified Name", "GUID"
    };

    public void write(OutputStream outputStream, List<GlossaryExportRow> rows,
                      GlossaryExportParameters.ExportFormat format,
                      GlossaryExportParameters.ExportMode mode) throws IOException {
        if (format == GlossaryExportParameters.ExportFormat.XLSX) {
            writeXlsx(outputStream, rows, mode);
        } else {
            writeCsv(outputStream, rows, mode);
        }
    }

    public void writeCsv(OutputStream outputStream, List<GlossaryExportRow> rows,
                         GlossaryExportParameters.ExportMode mode) throws IOException {
        try (OutputStreamWriter osw = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
                CSVWriter writer = new CSVWriter(osw)) {
            osw.write('\ufeff');

            if (mode == GlossaryExportParameters.ExportMode.IMPORT_COMPATIBLE) {
                writer.writeNext(getImportHeaders());
                if (CollectionUtils.isNotEmpty(rows)) {
                    for (GlossaryExportRow row : rows) {
                        if (row.getRecordType() == GlossaryExportRow.RecordTypeKind.TERM) {
                            writer.writeNext(toImportRow(row));
                        }
                    }
                }
            } else {
                writer.writeNext(AUDIT_HEADERS);
                if (CollectionUtils.isNotEmpty(rows)) {
                    for (GlossaryExportRow row : rows) {
                        writer.writeNext(toAuditRow(row));
                    }
                }
            }
        }
    }

    public void writeXlsx(OutputStream outputStream, List<GlossaryExportRow> rows,
                          GlossaryExportParameters.ExportMode mode) throws IOException {
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Glossary Export");
            String[] headers;
            List<String[]> dataRows = new ArrayList<>();

            if (mode == GlossaryExportParameters.ExportMode.IMPORT_COMPATIBLE) {
                headers = getImportHeaders();
                if (CollectionUtils.isNotEmpty(rows)) {
                    for (GlossaryExportRow row : rows) {
                        if (row.getRecordType() == GlossaryExportRow.RecordTypeKind.TERM) {
                            dataRows.add(toImportRow(row));
                        }
                    }
                }
            } else {
                headers = AUDIT_HEADERS;
                if (CollectionUtils.isNotEmpty(rows)) {
                    for (GlossaryExportRow row : rows) {
                        dataRows.add(toAuditRow(row));
                    }
                }
            }

            Row headerRow = sheet.createRow(0);
            for (int i = 0; i < headers.length; i++) {
                headerRow.createCell(i).setCellValue(headers[i]);
            }

            int rowIdx = 1;
            for (String[] data : dataRows) {
                Row excelRow = sheet.createRow(rowIdx++);
                for (int i = 0; i < data.length; i++) {
                    excelRow.createCell(i).setCellValue(truncate(data[i]));
                }
            }

            workbook.write(outputStream);
        }
    }

    private String[] getImportHeaders() {
        return GlossaryTermUtils.getGlossaryTermHeaders().split(", ");
    }

    private String[] toAuditRow(GlossaryExportRow row) {
        return new String[] {
                row.getRecordType() != null ? row.getRecordType().name() : "",
                safe(row.getName()),
                safe(row.getGlossaryName()),
                safe(row.getShortDescription()),
                safe(row.getLongDescription()),
                safe(row.getStatus()),
                safe(row.getClassifications()),
                safe(row.getCustomAttributes()),
                safe(row.getRelatedCategoriesOrParent()),
                safe(row.getQualifiedName()),
                safe(row.getGuid())
        };
    }

    private String[] toImportRow(GlossaryExportRow row) {
        return new String[] {
                safe(row.getGlossaryName()),
                safe(row.getName()),
                safe(row.getShortDescription()),
                safe(row.getLongDescription()),
                safe(row.getExamples()),
                safe(row.getAbbreviation()),
                safe(row.getUsage()),
                safe(row.getCustomAttributes()),
                safe(row.getTranslationTerms()),
                safe(row.getValidValuesFor()),
                safe(row.getSynonyms()),
                safe(row.getReplacedBy()),
                safe(row.getValidValues()),
                safe(row.getReplacementTerms()),
                safe(row.getSeeAlso()),
                safe(row.getTranslatedTerms()),
                safe(row.getIsA()),
                safe(row.getAntonyms()),
                safe(row.getClassifies()),
                safe(row.getPreferredToTerms()),
                safe(row.getPreferredTerms())
        };
    }

    private String safe(String value) {
        return value != null ? value : "";
    }

    private String truncate(String value) {
        if (value == null) {
            return "";
        }

        return value.length() > MAX_CELL_LENGTH ? value.substring(0, MAX_CELL_LENGTH) : value;
    }
}
