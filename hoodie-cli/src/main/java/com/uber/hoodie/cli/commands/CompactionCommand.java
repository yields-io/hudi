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

import static com.uber.hoodie.common.table.HoodieTimeline.COMPACTION_ACTION;

import com.uber.hoodie.avro.model.HoodieCompactionOperation;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.cli.HoodiePrintHelper;
import com.uber.hoodie.cli.TableHeader;
import com.uber.hoodie.cli.commands.SparkMain.SparkCommand;
import com.uber.hoodie.cli.utils.InputStreamConsumer;
import com.uber.hoodie.cli.utils.SparkUtil;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.CompactionUtils;
import com.uber.hoodie.common.util.CompactionUtils.ValidationResult;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class CompactionCommand implements CommandMarker {

  private static Logger log = LogManager.getLogger(HDFSParquetImportCommand.class);

  @CliCommand(value = "compactions show all", help = "Shows all compactions that are in active timeline")
  public String compactionsAll(
      @CliOption(key = {
          "activeOnly"}, help = "Include extra metadata", unspecifiedDefaultValue = "true") final boolean activeOnly,
      @CliOption(key = {
          "includeExtraMetadata"}, help = "Include extra metadata", unspecifiedDefaultValue = "false")
      final boolean includeExtraMetadata,
      @CliOption(key = {
          "limit"}, mandatory = false, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {
          "headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {
    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsAndCompactionTimeline();
    HoodieTimeline commitTimeline = activeTimeline.getCommitTimeline().filterCompletedInstants();
    Set<String> committed = commitTimeline.getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toSet());

    if (activeOnly) {
      timeline = timeline.filterPendingCompactionTimeline();
    }

    List<HoodieInstant> instants = timeline.getInstants().collect(Collectors.toList());
    List<Comparable[]> rows = new ArrayList<>();
    Collections.reverse(instants);
    for (int i = 0; i < instants.size(); i++) {
      HoodieInstant instant = instants.get(i);
      HoodieCompactionPlan workload = null;
      if (!instant.getAction().equals(COMPACTION_ACTION)) {
        try {
          // This could be a completed compaction. Assume a compaction request file is present but skip if fails
          workload = AvroUtils.deserializeCompactionPlan(
              activeTimeline.getInstantAuxiliaryDetails(
                  HoodieTimeline.getCompactionRequestedInstant(instant.getTimestamp())).get());
        } catch (HoodieIOException ioe) {
          // SKIP
        }
      } else {
        workload = AvroUtils.deserializeCompactionPlan(activeTimeline.getInstantAuxiliaryDetails(
            HoodieTimeline.getCompactionRequestedInstant(instant.getTimestamp())).get());
      }

      if (null != workload) {
        HoodieInstant.State state = instant.getState();
        if (committed.contains(instant.getTimestamp())) {
          state = State.COMPLETED;
        }
        if (includeExtraMetadata) {
          rows.add(new Comparable[]{instant.getTimestamp(),
              state.toString(),
              workload.getOperations() == null ? 0 : workload.getOperations().size(),
              workload.getExtraMetadata().toString()});
        } else {
          rows.add(new Comparable[]{instant.getTimestamp(),
              state.toString(),
              workload.getOperations() == null ? 0 : workload.getOperations().size()});
        }
      }
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    TableHeader header = new TableHeader()
        .addTableHeaderField("Compaction Instant Time")
        .addTableHeaderField("State")
        .addTableHeaderField("Total FileIds to be Compacted");
    if (includeExtraMetadata) {
      header = header.addTableHeaderField("Extra Metadata");
    }
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "compaction show", help = "Shows compaction details for a specific compaction instant")
  public String compactionShow(
      @CliOption(key = "instant", mandatory = true, help = "Base path for the target hoodie dataset") final
      String compactionInstantTime,
      @CliOption(key = {
          "limit"}, mandatory = false, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {
          "headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws Exception {
    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieCompactionPlan workload = AvroUtils.deserializeCompactionPlan(
        activeTimeline.getInstantAuxiliaryDetails(
            HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get());

    List<Comparable[]> rows = new ArrayList<>();
    if ((null != workload) && (null != workload.getOperations())) {
      for (HoodieCompactionOperation op : workload.getOperations()) {
        rows.add(new Comparable[]{op.getPartitionPath(),
            op.getFileId(),
            op.getBaseInstantTime(),
            op.getDataFilePath(),
            op.getDeltaFilePaths().size(),
            op.getMetrics().toString()
        });
      }
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    TableHeader header = new TableHeader()
        .addTableHeaderField("Partition Path")
        .addTableHeaderField("File Id")
        .addTableHeaderField("Base Instant")
        .addTableHeaderField("Data File Path")
        .addTableHeaderField("Total Delta Files")
        .addTableHeaderField("getMetrics");
    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "compaction repair", help = "Repair Compaction")
  public String repairCompaction(
      @CliOption(key = "compactionInstant", mandatory = true, help = "Compaction Instant")
      final String compactionInstant) throws Exception {
    // Rename to
    throw new IllegalStateException("Not Implemented yet");
  }

  @CliCommand(value = "compaction validate", help = "Validate Compaction")
  public String validateCompaction(
      @CliOption(key = "compactionInstant", mandatory = true, help = "Compaction Instant")
      final String compactionInstant,
      @CliOption(key = {
          "limit"}, mandatory = false, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {
          "headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws Exception {
    HoodieTableMetaClient metaClient = HoodieCLI.tableMetadata;
    List<ValidationResult> res = CompactionUtils.validateCompactionPlan(metaClient, compactionInstant);
    List<Comparable[]> rows = new ArrayList<>();
    res.stream().forEach(r -> {
      Comparable[] row = new Comparable[] { r.getOperation().getFileId(),
          r.getOperation().getBaseInstantTime(), r.getOperation().getDataFilePath(),
          r.getOperation().getDeltaFilePaths().size(), r.isSuccess(),
          r.getErrorMessage().isPresent() ? r.getErrorMessage().get().getMessage() : "" };
      rows.add(row);
    });

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    TableHeader header = new TableHeader()
        .addTableHeaderField("File Id")
        .addTableHeaderField("Base Instant Time")
        .addTableHeaderField("Base Data File")
        .addTableHeaderField("Num Delta Files")
        .addTableHeaderField("Valid")
        .addTableHeaderField("Error");

    return HoodiePrintHelper.print(header, fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "compaction unschedule", help = "Unschedule Compaction")
  public String unscheduleCompaction(
      @CliOption(key = "compactionInstant", mandatory = true, help = "Compaction Instant")
      final String compactionInstant) throws Exception {

    HoodieTableMetaClient metaClient = HoodieCLI.tableMetadata;
    List<Pair<HoodieLogFile, HoodieLogFile>> renameActions =
        CompactionUtils.getRenamingActionsForUnschedulingCompactionPlan(metaClient, compactionInstant,
            Optional.empty());

    runRenamingOps(metaClient, renameActions);

    // Overwrite compaction request with empty compaction operations
    HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, compactionInstant);
    HoodieCompactionPlan newPlan =
        HoodieCompactionPlan.newBuilder().setOperations(new ArrayList<>()).setExtraMetadata(plan.getExtraMetadata())
            .build();
    metaClient.getActiveTimeline().saveToCompactionRequested(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, compactionInstant),
        AvroUtils.serializeCompactionPlan(newPlan));
    return "Successfully removed all file-ids from compaction instant " + compactionInstant;
  }

  @CliCommand(value = "compaction unscheduleFileId", help = "UnSchedule Compaction for a fileId")
  public String unscheduleCompactFile(
      @CliOption(key = "fileId", mandatory = true, help = "File Id") final String fileId) throws Exception {

    HoodieTableMetaClient metaClient = HoodieCLI.tableMetadata;
    List<Pair<HoodieLogFile, HoodieLogFile>> renameActions =
        CompactionUtils.getRenamingActionsForUnschedulingCompactionForFileId(metaClient, fileId, Optional.empty());

    runRenamingOps(metaClient, renameActions);

    // Ready to remove this file-Id from compaction request
    Pair<String, HoodieCompactionOperation> compactionOperationWithInstant =
        CompactionUtils.getAllPendingCompactionOperations(metaClient).get(fileId);
    HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, compactionOperationWithInstant.getKey());
    List<HoodieCompactionOperation> newOps = plan.getOperations().stream()
        .filter(op -> !op.getFileId().equals(fileId)).collect(Collectors.toList());
    HoodieCompactionPlan newPlan =
        HoodieCompactionPlan.newBuilder().setOperations(newOps).setExtraMetadata(plan.getExtraMetadata()).build();
    metaClient.getActiveTimeline().saveToCompactionRequested(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, compactionOperationWithInstant.getLeft()),
        AvroUtils.serializeCompactionPlan(newPlan));
    return "Successfully removed file " + fileId
        + " from compaction instant " + compactionOperationWithInstant.getLeft();
  }

  private void runRenamingOps(HoodieTableMetaClient metaClient,
      List<Pair<HoodieLogFile, HoodieLogFile>> renameActions) {
    if (renameActions.isEmpty()) {
      System.out.println("No renaming of log-files needed. Proceeding to removing file-id from compaction-plan");
    } else {
      System.out.println("The following compaction renaming operations needs to be performed to un-schedule");
      renameActions.stream().forEach(lfPair -> {
        System.out.println("RENAME " + lfPair.getLeft().getPath() + " => " + lfPair.getRight().getPath());
        try {
          CompactionUtils.renameLogFile(metaClient, lfPair.getLeft(), lfPair.getRight());
        } catch (IOException e) {
          log.error("Error renaming log file", e);
          log.error("\n\n\n***NOTE Compaction is in inconsistent state. Run \"compaction repair "
              + lfPair.getLeft().getBaseCommitTime() + "\" to recover from failure ***\n\n\n");
          throw new HoodieIOException(e.getMessage(), e);
        }
      });
    }
  }

  @CliCommand(value = "compaction schedule", help = "Schedule Compaction")
  public String scheduleCompact(
      @CliOption(key = "tableName", mandatory = true, help = "Table name") final String tableName,
      @CliOption(key = "rowKeyField", mandatory = true, help = "Row key field name") final String rowKeyField,
      @CliOption(key = {
          "parallelism"}, mandatory = true, help = "Parallelism for hoodie compaction") final String parallelism,
      @CliOption(key = "schemaFilePath", mandatory = true, help = "Path for Avro schema file") final String
          schemaFilePath,
      @CliOption(key = "sparkMemory", mandatory = true, help = "Spark executor memory") final String sparkMemory,
      @CliOption(key = "retry", mandatory = true, help = "Number of retries") final String retry) throws Exception {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    // First get a compaction instant time and pass it to spark launcher for scheduling compaction
    String compactionInstantTime = HoodieActiveTimeline.createNewCommitTime();

    if (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ) {
      String sparkPropertiesPath = Utils.getDefaultPropertiesFile(
          scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
      SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
      sparkLauncher.addAppArgs(SparkCommand.COMPACT_SCHEDULE.toString(), HoodieCLI.tableMetadata.getBasePath(),
          tableName, compactionInstantTime, rowKeyField, parallelism, schemaFilePath, sparkMemory, retry);
      Process process = sparkLauncher.launch();
      InputStreamConsumer.captureOutput(process);
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        return "Failed to run compaction for " + compactionInstantTime;
      }
      return "Compaction successfully completed for " + compactionInstantTime;
    } else {
      throw new Exception("Compactions can only be run for table type : MERGE_ON_READ");
    }
  }

  @CliCommand(value = "compaction run", help = "Run Compaction for given instant time")
  public String compact(
      @CliOption(key = "tableName", mandatory = true, help = "Table name") final String tableName,
      @CliOption(key = "rowKeyField", mandatory = true, help = "Row key field name") final String rowKeyField,
      @CliOption(key = {
          "parallelism"}, mandatory = true, help = "Parallelism for hoodie compaction") final String parallelism,
      @CliOption(key = "schemaFilePath", mandatory = true, help = "Path for Avro schema file") final String
          schemaFilePath,
      @CliOption(key = "sparkMemory", mandatory = true, help = "Spark executor memory") final String sparkMemory,
      @CliOption(key = "retry", mandatory = true, help = "Number of retries") final String retry,
      @CliOption(key = "compactionInstant", mandatory = true, help = "Base path for the target hoodie dataset") final
      String compactionInstantTime) throws Exception {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    if (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ) {
      String sparkPropertiesPath = Utils.getDefaultPropertiesFile(
          scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
      SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
      sparkLauncher.addAppArgs(SparkCommand.COMPACT_RUN.toString(), HoodieCLI.tableMetadata.getBasePath(),
          tableName, compactionInstantTime, rowKeyField, parallelism, schemaFilePath, sparkMemory, retry);
      Process process = sparkLauncher.launch();
      InputStreamConsumer.captureOutput(process);
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        return "Failed to run compaction for " + compactionInstantTime;
      }
      return "Compaction successfully completed for " + compactionInstantTime;
    } else {
      throw new Exception("Compactions can only be run for table type : MERGE_ON_READ");
    }
  }
}