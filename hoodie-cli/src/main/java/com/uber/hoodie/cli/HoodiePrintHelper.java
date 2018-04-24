/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.cli;

import dnl.utils.text.table.TextTable;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;

/**
 * Helper class to render table for hoodie-cli
 */
public class HoodiePrintHelper {

  /**
   * Print header and raw rows
   * @param header Header
   * @param rows Raw Rows
   * @return output
   */
  public static String print(String[] header, String[][] rows) {
    TextTable textTable = new TextTable(header, rows);
    return printTextTable(textTable);
  }

  /**
   * Render rows in Table Buffer
   * @param buffer Table Buffer
   * @return output
   */
  public static String print(TableBuffer buffer) {
    String[] header = new String[buffer.getFieldNames().size()];
    buffer.getFieldNames().toArray(header);

    String[][] rows = buffer.getRenderRows().stream()
        .map(l -> l.stream().toArray(String[]::new))
        .toArray(String[][]::new);
    TextTable textTable = new TextTable(header, rows);
    return printTextTable(textTable);
  }

  /**
   * Render only header of the table
   * @param header Table Header
   * @return output
   */
  public static String print(TableHeader header) {
    String[] head = new String[header.getFieldNames().size()];
    header.getFieldNames().toArray(head);
    TextTable textTable = new TextTable(head, new String[][]{});
    return printTextTable(textTable);
  }

  /**
   * Print Text table
   * @param textTable Text table to be printed
   * @return
   */
  private static String printTextTable(TextTable textTable) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    textTable.printTable(ps, 4);
    return new String(baos.toByteArray(), Charset.forName("utf-8"));
  }
}
