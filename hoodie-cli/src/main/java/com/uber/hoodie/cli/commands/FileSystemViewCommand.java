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

package com.uber.hoodie.cli.commands;

import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.cli.HoodiePrintHelper;
import com.uber.hoodie.cli.TableHeader;
import com.uber.hoodie.cli.commands.HoodieLogFileCommand.LogFileProcessResult;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieDefaultTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.NumericUtils;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class FileSystemViewCommand implements CommandMarker {

  /**
   * Show file system file-slices
   *
   * @param globRegex         Regex to filter files before generating view
   * @param readOptimizedOnly Use Commit Timeline only
   * @param maxInstant        Max Instant to be used for filtering file slices
   * @param includeMaxInstant Max Instant Inclusive
   * @param includeInflight   Include file-slices belonging to inflight instants
   * @param excludeCompaction Exclude Compaction Instants
   * @param limit             Max Number of file-slices to be included
   * @param sortByField       Column for sorting
   * @param descending        Descending order while sorting
   * @param headerOnly        Print only headers
   */
  @CliCommand(value = "fsview show all", help = "Show entire file-system view")
  public String showAllFileSlices(
      @CliOption(key = {"pathRegex"},
          help = "regex to select files, eg: 2016/08/02", unspecifiedDefaultValue = "*/*/*") String globRegex,
      @CliOption(key = {
          "readOptimizedOnly"}, help = "Max Instant", unspecifiedDefaultValue = "false") boolean readOptimizedOnly,
      @CliOption(key = {"maxInstant"}, help = "Max Instant", unspecifiedDefaultValue = "") String maxInstant,
      @CliOption(key = {
          "includeMax"}, help = "Include Max Instant", unspecifiedDefaultValue = "false") boolean includeMaxInstant,
      @CliOption(key = {
          "includeInflight"}, help = "Include Inflight Instants", unspecifiedDefaultValue = "false")
          boolean includeInflight,
      @CliOption(key = {"excludeCompaction"}, help = "Exclude compaction Instants", unspecifiedDefaultValue = "false")
          boolean excludeCompaction,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {
          "headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    HoodieTableFileSystemView fsView = buildFileSystemView(globRegex, maxInstant, readOptimizedOnly, includeMaxInstant,
        includeInflight, excludeCompaction);
    List<Comparable[]> rows = new ArrayList<>();
    for (HoodieFileGroup fg : fsView.getFileGroupMap().values()) {

      fg.getAllFileSlices().forEach(fs -> {
        int idx = 0;
        Comparable[] row = new Comparable[11];
        row[idx++] = fg.getPartitionPath();
        row[idx++] = fg.getId();
        row[idx++] = fs.getBaseInstantTime();
        row[idx++] = fs.getDataFile().isPresent() ? fs.getDataFile().get().getPath() : "";
        row[idx++] = fs.getDataFile().isPresent() ? fs.getDataFile().get().getFileSize() : -1;
        row[idx++] = fs.getLogFiles().count();
        row[idx++] = fs.getLogFiles().filter(lf -> lf.getFileSize().isPresent())
            .mapToLong(lf -> lf.getFileSize().get()).sum();
        row[idx++] = fs.getLogFiles().filter(lf -> lf.getFileSize().isPresent())
            .collect(Collectors.toList()).toString();
        rows.add(row);
      });
    }
    Function<Object, String> converterFunction = entry -> {
      return NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    };
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Total Log File Size", converterFunction);
    fieldNameToConverterMap.put("Data-File Size", converterFunction);

    TableHeader header = new TableHeader()
        .addTableHeaderField("Partition")
        .addTableHeaderField("FileId")
        .addTableHeaderField("Base-Instant")
        .addTableHeaderField("Data-File")
        .addTableHeaderField("Data-File Size")
        .addTableHeaderField("Num Log Files")
        .addTableHeaderField("Total Log File Size")
        .addTableHeaderField("Log Files");
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  /**
   * Show file system's latest file-slices
   *
   * @param partition         Partition Path
   * @param readOptimizedOnly Use Commit Timeline only
   * @param maxInstant        Max Instant to be used for filtering file slices
   * @param includeMaxInstant Max Instant Inclusive
   * @param includeInflight   Include file-slices belonging to inflight instants
   * @param excludeCompaction Exclude Compaction Instants
   * @param limit             Max Number of file-slices to be included
   * @param sortByField       Column for sorting
   * @param descending        Descending order while sorting
   * @param headerOnly        Print only headers
   */
  @CliCommand(value = "fsview show latest", help = "Show latest file-system view")
  public String showLatestFileSlices(
      @CliOption(key = {"partitionPath"},
          help = "valid paritition path", mandatory = true) String partition,
      @CliOption(key = {
          "readOptimizedOnly"}, help = "Max Instant", unspecifiedDefaultValue = "false") boolean readOptimizedOnly,
      @CliOption(key = {"maxInstant"}, help = "Max Instant", unspecifiedDefaultValue = "") String maxInstant,
      @CliOption(key = {"merge"}, help = "Merge File Slices due to pending compaction",
          unspecifiedDefaultValue = "true") final boolean merge,
      @CliOption(key = {"includeMax"}, help = "Include Max Instant", unspecifiedDefaultValue = "false")
          boolean includeMaxInstant,
      @CliOption(key = {"includeInflight"}, help = "Include Inflight Instants", unspecifiedDefaultValue = "false")
          boolean includeInflight,
      @CliOption(key = {"excludeCompaction"}, help = "Exclude compaction Instants", unspecifiedDefaultValue = "false")
          boolean excludeCompaction,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {
          "headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    HoodieTableFileSystemView fsView = buildFileSystemView(partition, maxInstant, readOptimizedOnly, includeMaxInstant,
        includeInflight, excludeCompaction);
    List<Comparable[]> rows = new ArrayList<>();

    final Stream<FileSlice> fileSliceStream;
    if (!merge) {
      fileSliceStream = fsView.getLatestFileSlices(partition);
    } else {
      if (maxInstant.isEmpty()) {
        maxInstant = HoodieCLI.tableMetadata.getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant()
            .get().getTimestamp();
      }
      fileSliceStream = fsView.getLatestMergedFileSlicesBeforeOrOn(partition, maxInstant);
    }

    fileSliceStream.forEach(fs -> {
      int idx = 0;
      Comparable[] row = new Comparable[11];
      row[idx++] = partition;
      row[idx++] = fs.getFileId();
      row[idx++] = fs.getBaseInstantTime();
      row[idx++] = fs.getDataFile().isPresent() ? fs.getDataFile().get().getPath() : "";
      row[idx++] = fs.getDataFile().isPresent() ? fs.getDataFile().get().getFileSize() : -1;
      row[idx++] = fs.getLogFiles().count();
      row[idx++] = fs.getLogFiles().filter(lf -> lf.getFileSize().isPresent())
          .mapToLong(lf -> lf.getFileSize().get()).sum();
      row[idx++] = fs.getLogFiles().filter(lf -> lf.getFileSize().isPresent())
          .filter(lf -> lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
          .mapToLong(lf -> lf.getFileSize().get()).sum();
      row[idx++] = fs.getLogFiles().filter(lf -> lf.getFileSize().isPresent())
          .filter(lf -> !lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
          .mapToLong(lf -> lf.getFileSize().get()).sum();
      row[idx++] = fs.getLogFiles().filter(lf -> lf.getFileSize().isPresent())
          .filter(lf -> lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
          .collect(Collectors.toList()).toString();
      row[idx++] = fs.getLogFiles().filter(lf -> lf.getFileSize().isPresent())
          .filter(lf -> !lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
          .collect(Collectors.toList()).toString();
      rows.add(row);
    });

    Function<Object, String> converterFunction = entry -> {
      return NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    };
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Total Log File Size", converterFunction);
    fieldNameToConverterMap.put("Data-File Size", converterFunction);
    fieldNameToConverterMap.put("Log File Size before Compaction", converterFunction);
    fieldNameToConverterMap.put("Log File Size after Compaction", converterFunction);

    TableHeader header = new TableHeader()
        .addTableHeaderField("Partition")
        .addTableHeaderField("FileId")
        .addTableHeaderField("Base-Instant")
        .addTableHeaderField("Data-File")
        .addTableHeaderField("Data-File Size")
        .addTableHeaderField("Num Log Files")
        .addTableHeaderField("Total Log File Size")
        .addTableHeaderField("Log File Size before Compaction")
        .addTableHeaderField("Log File Size after Compaction")
        .addTableHeaderField("Log Files before Compaction")
        .addTableHeaderField("Log Files after Compaction");
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  private HoodieTableFileSystemView buildFileSystemView(String globRegex, String maxInstant, boolean readOptimizedOnly,
      boolean includeMaxInstant, boolean includeInflight, boolean excludeCompaction) throws IOException {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(HoodieCLI.tableMetadata.getHadoopConf(),
        HoodieCLI.tableMetadata.getBasePath(), true);
    FileSystem fs = HoodieCLI.fs;
    String globPath = String.format("%s/%s/*", HoodieCLI.tableMetadata.getBasePath(), globRegex);
    FileStatus[] statuses = fs.globStatus(new Path(globPath));
    Stream<HoodieInstant> instantsStream = null;

    HoodieTimeline timeline = null;
    if (readOptimizedOnly) {
      timeline = metaClient.getActiveTimeline().getCommitTimeline();
    } else if (excludeCompaction) {
      timeline = metaClient.getActiveTimeline().getCommitsTimeline();
    } else {
      timeline = metaClient.getActiveTimeline().getCommitsAndCompactionTimeline();
    }

    if (!includeInflight) {
      timeline = timeline.filterCompletedInstants();
    }

    instantsStream = timeline.getInstants();

    if (!maxInstant.isEmpty()) {
      final BiPredicate<String, String> predicate;
      if (includeMaxInstant) {
        predicate = HoodieTimeline.GREATER_OR_EQUAL;
      } else {
        predicate = HoodieTimeline.GREATER;
      }
      instantsStream = instantsStream.filter(is -> predicate.test(maxInstant, is.getTimestamp()));
    }

    HoodieTimeline filteredTimeline = new HoodieDefaultTimeline(instantsStream,
        (Function<HoodieInstant, Optional<byte[]>> & Serializable) metaClient.getActiveTimeline()::getInstantDetails);
    return new HoodieTableFileSystemView(metaClient, filteredTimeline, statuses);
  }

  /**
   * File System View Detailed Stats
   *
   * @param partition   Partition for generating file system view
   * @param limit       Limit the number of fileIds to be displayed
   * @param sortByField Column name for sorting
   * @param descending  Descending order
   * @param headerOnly  Print header only
   */
  @CliCommand(value = "fsview detailed stats", help = "File Slice level Size stats")
  public String fileSliceStats(
      @CliOption(key = {"partitionPath"}, help = "Partition path", mandatory = true) final String partition,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {
          "headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(HoodieCLI.tableMetadata.getHadoopConf(),
        HoodieCLI.tableMetadata.getBasePath());
    HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitsAndCompactionTimeline());
    HoodieInstant latestInstant = metaClient.getActiveTimeline().getCommitsAndCompactionTimeline()
        .filterCompletedInstants().lastInstant().get();
    List<FileSlice> latest = fileSystemView.getLatestMergedFileSlicesBeforeOrOn(partition, latestInstant.getTimestamp())
        .collect(Collectors.toList());
    List<Comparable[]> rows = new ArrayList<>();
    for (FileSlice fSlice : latest) {
      Long dataFileSize = new Long(-1);
      Long numRowsInParquet = new Long(-1);
      if (fSlice.getDataFile().isPresent()) {
        dataFileSize = fSlice.getDataFile().get().getFileSize();
        numRowsInParquet = getRowCount(fSlice.getDataFile().get().getFileStatus(), metaClient.getHadoopConf());
      }

      long logFilesTotalSize = fSlice.getLogFiles().filter(f -> f.getFileSize().isPresent())
          .mapToLong(f -> f.getFileSize().get()).sum();
      List<String> logFilePaths = fSlice.getLogFiles()
          .map(lf -> lf.getPath().toString()).collect(Collectors.toList());
      LogFileProcessResult result = HoodieLogFileCommand.processLogFile(logFilePaths, metaClient.getFs());
      double logToBaseRatio = dataFileSize > 0 ? logFilesTotalSize / (dataFileSize * 1.0) : -1;
      Comparable[] row = new Comparable[10];
      int idx = 0;
      row[idx++] = partition;
      row[idx++] = fSlice.getFileId();
      row[idx++] = fSlice.getBaseInstantTime();
      row[idx++] = dataFileSize;
      row[idx++] = fSlice.getLogFiles().count();
      row[idx++] = logFilesTotalSize;
      row[idx++] = logToBaseRatio;
      row[idx++] = numRowsInParquet;
      row[idx++] = result.totalRecordsCount;
      row[idx++] = numRowsInParquet > 0 ? (result.totalRecordsCount * 1.0) / numRowsInParquet : -1;

      rows.add(row);
    }

    Function<Object, String> converterFunction = entry -> {
      return NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    };
    Function<Object, String> roundFunction = entry -> {
      return String.format("%.5f", entry);
    };
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Base File Size", converterFunction);
    fieldNameToConverterMap.put("Total Log Files Size", converterFunction);
    fieldNameToConverterMap.put("Num Rows Ratio", roundFunction);

    TableHeader header = new TableHeader()
        .addTableHeaderField("Partition")
        .addTableHeaderField("FileId")
        .addTableHeaderField("BaseInstant")
        .addTableHeaderField("Base File Size")
        .addTableHeaderField("Number of Log Files")
        .addTableHeaderField("Total Log Files Size")
        .addTableHeaderField("Log To Base Ratio")
        .addTableHeaderField("Num Rows in Base File")
        .addTableHeaderField("Num Rows in Log Files")
        .addTableHeaderField("Num Rows Ratio");

    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  /**
   * Fetch Parquet row count
   * @param fs
   * @param conf
   * @return
   * @throws IOException
   */
  private static long getRowCount(FileStatus fs, Configuration conf) throws IOException {
    long rowCount = 0;
    for (Footer f : ParquetFileReader.readFooters(conf, fs, false)) {
      for (BlockMetaData b : f.getParquetMetadata().getBlocks()) {
        rowCount += b.getRowCount();
      }
    }
    return rowCount;
  }
}
