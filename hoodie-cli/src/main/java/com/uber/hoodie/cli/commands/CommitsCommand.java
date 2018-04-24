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
import com.uber.hoodie.cli.TableBuffer;
import com.uber.hoodie.cli.TableFieldType;
import com.uber.hoodie.cli.TableHeader;
import com.uber.hoodie.cli.utils.InputStreamConsumer;
import com.uber.hoodie.cli.utils.SparkUtil;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.NumericUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class CommitsCommand implements CommandMarker {

  @CliAvailabilityIndicator({"commits show"})
  public boolean isShowAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliAvailabilityIndicator({"commits refresh"})
  public boolean isRefreshAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliAvailabilityIndicator({"commit rollback"})
  public boolean isRollbackAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliAvailabilityIndicator({"commit show"})
  public boolean isCommitShowAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliCommand(value = "commits show", help = "Show the commits")
  public String showCommits(@CliOption(key = {
      "limit"}, mandatory = false, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false")
      final boolean headerOnly) throws IOException {

    TableHeader header = new TableHeader()
        .addTableHeaderField("CommitTime", TableFieldType.TEXT)
        .addTableHeaderField("Total Bytes Written", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Files Added", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Files Updated", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Partitions Written", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Records Written", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Update Records Written", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Errors", TableFieldType.NUMERIC);

    if (headerOnly) {
      return HoodiePrintHelper.print(header);
    }

    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> commits = timeline.getInstants().collect(Collectors.toList());
    List<String[]> rows = new ArrayList<>();
    Collections.reverse(commits);
    for (int i = 0; i < commits.size(); i++) {
      HoodieInstant commit = commits.get(i);
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(commit).get());
      rows.add(new String[] {commit.getTimestamp(),
          Long.valueOf(commitMetadata.fetchTotalBytesWritten()).toString(),
          String.valueOf(commitMetadata.fetchTotalFilesInsert()),
          String.valueOf(commitMetadata.fetchTotalFilesUpdated()),
          String.valueOf(commitMetadata.fetchTotalPartitionsWritten()),
          String.valueOf(commitMetadata.fetchTotalRecordsWritten()),
          String.valueOf(commitMetadata.fetchTotalUpdateRecordsWritten()),
          String.valueOf(commitMetadata.fetchTotalWriteErrors())});
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Total Bytes Written", entry -> {
      return NumericUtils.humanReadableByteCount((Double.valueOf(entry.toString())));
    });

    TableBuffer buffer = new TableBuffer(header, fieldNameToConverterMap,
        Optional.ofNullable(sortByField.isEmpty() ? null : sortByField),
        Optional.of(descending),
        Optional.ofNullable(limit <= 0 ? null : limit)).addAllRows(rows).flip();
    return HoodiePrintHelper.print(buffer);
  }

  @CliCommand(value = "commits refresh", help = "Refresh the commits")
  public String refreshCommits() throws IOException {
    HoodieTableMetaClient metadata = new HoodieTableMetaClient(HoodieCLI.conf, HoodieCLI.tableMetadata.getBasePath());
    HoodieCLI.setTableMetadata(metadata);
    return "Metadata for table " + metadata.getTableConfig().getTableName() + " refreshed.";
  }

  @CliCommand(value = "commit rollback", help = "Rollback a commit")
  public String rollbackCommit(@CliOption(key = {"commit"}, help = "Commit to rollback") final String commitTime,
      @CliOption(key = {"sparkProperties"}, help = "Spark Properites File Path") final String sparkPropertiesPath)
      throws Exception {
    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);

    if (!timeline.containsInstant(commitInstant)) {
      return "Commit " + commitTime + " not found in Commits " + timeline;
    }

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher
        .addAppArgs(SparkMain.SparkCommand.ROLLBACK.toString(), commitTime, HoodieCLI.tableMetadata.getBasePath());
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    // Refresh the current
    refreshCommits();
    if (exitCode != 0) {
      return "Commit " + commitTime + " failed to roll back";
    }
    return "Commit " + commitTime + " rolled back";
  }

  @CliCommand(value = "commit showpartitions", help = "Show partition level details of a commit")
  public String showCommitPartitions(
      @CliOption(key = {"commit"}, help = "Commit to show") final String commitTime,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false")
      final boolean headerOnly) throws Exception {

    TableHeader header = new TableHeader()
        .addTableHeaderField("Partition Path", TableFieldType.TEXT)
        .addTableHeaderField("Total Files Added", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Files Updated", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Records Inserted", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Records Updated", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Bytes Written", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Errors", TableFieldType.NUMERIC);

    if (headerOnly) {
      return HoodiePrintHelper.print(header);
    }

    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);

    if (!timeline.containsInstant(commitInstant)) {
      return "Commit " + commitTime + " not found in Commits " + timeline;
    }
    HoodieCommitMetadata meta = HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(commitInstant).get());
    List<String[]> rows = new ArrayList<String[]>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : meta.getPartitionToWriteStats().entrySet()) {
      String path = entry.getKey();
      List<HoodieWriteStat> stats = entry.getValue();
      long totalFilesAdded = 0;
      long totalFilesUpdated = 0;
      long totalRecordsUpdated = 0;
      long totalRecordsInserted = 0;
      long totalBytesWritten = 0;
      long totalWriteErrors = 0;
      for (HoodieWriteStat stat : stats) {
        if (stat.getPrevCommit().equals(HoodieWriteStat.NULL_COMMIT)) {
          totalFilesAdded += 1;
          totalRecordsInserted += stat.getNumWrites();
        } else {
          totalFilesUpdated += 1;
          totalRecordsUpdated += stat.getNumUpdateWrites();
        }
        totalBytesWritten += stat.getTotalWriteBytes();
        totalWriteErrors += stat.getTotalWriteErrors();
      }
      rows.add(new String[] {path, String.valueOf(totalFilesAdded), String.valueOf(totalFilesUpdated),
          String.valueOf(totalRecordsInserted), String.valueOf(totalRecordsUpdated),
          Long.valueOf(totalBytesWritten).toString(), String.valueOf(totalWriteErrors)});
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Total Bytes Written", entry -> {
      return NumericUtils.humanReadableByteCount((Long.valueOf(entry.toString())));
    });

    TableBuffer buffer = new TableBuffer(header, fieldNameToConverterMap,
        Optional.ofNullable(sortByField.isEmpty() ? null : sortByField),
        Optional.of(descending),
        Optional.ofNullable(limit <= 0 ? null : limit)).addAllRows(rows).flip();
    return HoodiePrintHelper.print(buffer);
  }

  @CliCommand(value = "commit showfiles", help = "Show file level details of a commit")
  public String showCommitFiles(
      @CliOption(key = {"commit"}, help = "Commit to show") final String commitTime,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false")
      final boolean headerOnly) throws Exception {

    TableHeader header = new TableHeader()
        .addTableHeaderField("Partition Path", TableFieldType.TEXT)
        .addTableHeaderField("File ID", TableFieldType.TEXT)
        .addTableHeaderField("Previous Commit", TableFieldType.TEXT)
        .addTableHeaderField("Total Records Updated", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Records Written", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Bytes Written", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Errors", TableFieldType.NUMERIC);

    if (headerOnly) {
      return HoodiePrintHelper.print(header);
    }

    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTime);

    if (!timeline.containsInstant(commitInstant)) {
      return "Commit " + commitTime + " not found in Commits " + timeline;
    }
    HoodieCommitMetadata meta = HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(commitInstant).get());
    List<String[]> rows = new ArrayList<String[]>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : meta.getPartitionToWriteStats().entrySet()) {
      String path = entry.getKey();
      List<HoodieWriteStat> stats = entry.getValue();
      for (HoodieWriteStat stat : stats) {
        rows.add(new String[] {path, stat.getFileId(), stat.getPrevCommit(), String.valueOf(stat.getNumUpdateWrites()),
            String.valueOf(stat.getNumWrites()), String.valueOf(stat.getTotalWriteBytes()),
            String.valueOf(stat.getTotalWriteErrors())});
      }
    }

    TableBuffer buffer = new TableBuffer(header, new HashMap<>(),
        Optional.ofNullable(sortByField.isEmpty() ? null : sortByField),
        Optional.of(descending),
        Optional.ofNullable(limit <= 0 ? null : limit)).addAllRows(rows).flip();
    return HoodiePrintHelper.print(buffer);
  }

  @CliAvailabilityIndicator({"commits compare"})
  public boolean isCompareCommitsAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliCommand(value = "commits compare", help = "Compare commits with another Hoodie dataset")
  public String compareCommits(@CliOption(key = {"path"}, help = "Path of the dataset to compare to") final String path)
      throws Exception {

    HoodieTableMetaClient target = new HoodieTableMetaClient(HoodieCLI.conf, path);
    HoodieTimeline targetTimeline = target.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieTableMetaClient source = HoodieCLI.tableMetadata;
    HoodieTimeline sourceTimeline = source.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    String targetLatestCommit =
        targetTimeline.getInstants().iterator().hasNext() ? "0" : targetTimeline.lastInstant().get().getTimestamp();
    String sourceLatestCommit =
        sourceTimeline.getInstants().iterator().hasNext() ? "0" : sourceTimeline.lastInstant().get().getTimestamp();

    if (sourceLatestCommit != null && HoodieTimeline.compareTimestamps(targetLatestCommit, sourceLatestCommit,
        HoodieTimeline.GREATER)) {
      // source is behind the target
      List<String> commitsToCatchup = targetTimeline.findInstantsAfter(sourceLatestCommit, Integer.MAX_VALUE)
          .getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
      return "Source " + source.getTableConfig().getTableName() + " is behind by " + commitsToCatchup.size()
          + " commits. Commits to catch up - " + commitsToCatchup;
    } else {
      List<String> commitsToCatchup = sourceTimeline.findInstantsAfter(targetLatestCommit, Integer.MAX_VALUE)
          .getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
      return "Source " + source.getTableConfig().getTableName() + " is ahead by " + commitsToCatchup.size()
          + " commits. Commits to catch up - " + commitsToCatchup;
    }
  }

  @CliAvailabilityIndicator({"commits sync"})
  public boolean isSyncCommitsAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliCommand(value = "commits sync", help = "Compare commits with another Hoodie dataset")
  public String syncCommits(@CliOption(key = {"path"}, help = "Path of the dataset to compare to") final String path)
      throws Exception {
    HoodieCLI.syncTableMetadata = new HoodieTableMetaClient(HoodieCLI.conf, path);
    HoodieCLI.state = HoodieCLI.CLIState.SYNC;
    return "Load sync state between " + HoodieCLI.tableMetadata.getTableConfig().getTableName() + " and "
        + HoodieCLI.syncTableMetadata.getTableConfig().getTableName();
  }

}
