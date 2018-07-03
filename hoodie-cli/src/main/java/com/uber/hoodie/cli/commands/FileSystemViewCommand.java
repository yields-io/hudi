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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class FileSystemViewCommand {

  @CliCommand(value = "show fsview all", help = "Show entire file-system view")
  public String showAllFileSlices(
      @CliOption(key = {"partitionPath"},
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

  @CliCommand(value = "show fsview latest", help = "Show latest file-system view")
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
}
