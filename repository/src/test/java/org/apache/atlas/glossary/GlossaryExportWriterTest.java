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

import org.apache.atlas.model.glossary.GlossaryExportParameters;
import org.apache.atlas.model.glossary.GlossaryExportRow;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class GlossaryExportWriterTest {
    private final GlossaryExportWriter writer = new GlossaryExportWriter();

    @Test
    public void testWriteAuditCsv() throws Exception {
        GlossaryExportRow row = new GlossaryExportRow();
        row.setRecordType(GlossaryExportRow.RecordTypeKind.TERM);
        row.setName("BankBranch");
        row.setGlossaryName("testBankingGlossary");
        row.setShortDescription("SD4");
        row.setLongDescription("LD4");
        row.setStatus("ACTIVE");
        row.setGuid("term-guid-1");

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writer.writeCsv(output, Collections.singletonList(row), GlossaryExportParameters.ExportMode.AUDIT);

        String csv = output.toString(StandardCharsets.UTF_8.name());
        assertTrue(csv.contains("BankBranch"));
        assertTrue(csv.contains("testBankingGlossary"));
        assertTrue(csv.contains("Record Type"));
    }

    @Test
    public void testWriteAuditXlsx() throws Exception {
        GlossaryExportRow row = new GlossaryExportRow();
        row.setRecordType(GlossaryExportRow.RecordTypeKind.TERM);
        row.setName("BankBranch");
        row.setGlossaryName("testBankingGlossary");

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writer.writeXlsx(output, Collections.singletonList(row), GlossaryExportParameters.ExportMode.AUDIT);

        try (XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(output.toByteArray()))) {
            Sheet sheet = workbook.getSheetAt(0);
            Row header  = sheet.getRow(0);
            Row data    = sheet.getRow(1);

            assertEquals(header.getCell(0).getStringCellValue(), "Record Type");
            assertEquals(data.getCell(1).getStringCellValue(), "BankBranch");
            assertEquals(data.getCell(2).getStringCellValue(), "testBankingGlossary");
        }
    }
}
