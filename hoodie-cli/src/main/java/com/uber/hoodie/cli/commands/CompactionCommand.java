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
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.CompactionUtils;
import com.uber.hoodie.common.util.CompactionUtils.CompactionValidationResult;
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
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class CompactionCommand implements CommandMarker {

  private static Logger log = LogManager.getLogger(HDFSParquetImportCommand.class);

  /**
   * Display all compaction requests (including those that have completed) with the count of fileIds to be compacted.
   *
   * @param pendingOnly          Only display Pending compactions
   * @param includeExtraMetadata Display ExtraMetadata stored in compaction request
   * @param limit                Number of compactions to be displayed
   * @param sortByField          Column name for sorting
   * @param descending           descending order
   * @param headerOnly           Display only header
   * @return result
   */
  @CliCommand(value = "compactions show all", help = "Shows all compactions that are in active timeline")
  public String compactionsAll(
      @CliOption(key = {"pendingOnly"}, help = "Only include pending compactions", unspecifiedDefaultValue = "true")
      final boolean pendingOnly,
      @CliOption(key = {"includeExtraMetadata"}, help = "Include extra metadata", unspecifiedDefaultValue = "false")
      final boolean includeExtraMetadata,
      @CliOption(key = {"limit"}, mandatory = false, help = "Limit commits", unspecifiedDefaultValue = "-1")
      final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {
          "headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {
    HoodieActiveTimeline activeTimeline = new HoodieTableMetaClient(HoodieCLI.tableMetadata.getHadoopConf(),
        HoodieCLI.tableMetadata.getBasePath(), true).getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsAndCompactionTimeline();
    HoodieTimeline commitTimeline = activeTimeline.getCommitTimeline().filterCompletedInstants();
    Set<String> committed = commitTimeline.getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toSet());

    if (pendingOnly) {
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

  /**
   * Display file ids to be compacted
   *
   * @param compactionInstantTime Compaction Instant
   * @param limit                 Limit the number of file-ids
   * @param sortByField           Column name for sorting
   * @param descending            Descending order
   * @param headerOnly            Print headers only
   */
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

  /**
   * Validate compaction instant
   *
   * @param compactionInstant Compaction Instant
   * @param limit             Limit the number of file ids
   * @param sortByField       Column name for sorting
   * @param descending        Decreasing order
   * @param headerOnly        Header only
   */
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
    List<CompactionValidationResult> res = CompactionUtils.validateCompactionPlan(metaClient, compactionInstant);
    List<Comparable[]> rows = new ArrayList<>();
    res.stream().forEach(r -> {
      Comparable[] row = new Comparable[]{r.getOperation().getFileId(),
          r.getOperation().getBaseInstantTime(), r.getOperation().getDataFilePath().orElse(""),
          r.getOperation().getDeltaFilePaths().size(), r.isSuccess(),
          r.getErrorMessage().isPresent() ? r.getErrorMessage().get().getMessage() : ""};
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

  /**
  @CliCommand(value = "compaction validate", help = "Validate Compaction")
  public String validateCompaction2(
      @CliOption(key = "compactionInstant", mandatory = true, help = "Compaction Instant")
      final String compactionInstant,
      @CliOption(key = {
          "parallelism"}, mandatory = true, help = "Parallelism for hoodie compaction") final String parallelism,
      @CliOption(key = "schemaFilePath", mandatory = true, help = "Path for Avro schema file") final String
          schemaFilePath,
      @CliOption(key = "sparkMemory", mandatory = true, help = "Spark executor memory") final String sparkMemory)
      throws Exception {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    if (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ) {
      String sparkPropertiesPath = Utils.getDefaultPropertiesFile(
          JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());
      SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
      sparkLauncher.addAppArgs(SparkCommand.COMPACT_VALIDATE.toString(), HoodieCLI.tableMetadata.getBasePath(),
          compactionInstant,  parallelism, sparkMemory, "0");
      Process process = sparkLauncher.launch();
      InputStreamConsumer.captureOutput(process);
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        return "Failed to validate compaction for " + compactionInstant;
      }
      return "Validation completed for " + compactionInstant;
    } else {
      throw new Exception("Compaction Validations can only be run for table type : MERGE_ON_READ");
    }
  }
  **/

  /**
   * Unschedule a compaction instant. All log-files that are created because of pending compactions are adjusted
   * such that they resemble the case where compaction was not scheduled in the first place. This operation is
   * expected to be run when no ingestion or compaction is running.
   *
   * @param compactionInstant Compaction instant to be unscheduled
   * @param skipValidation Skip Validation
   * @return
   * @throws Exception
   */
  @CliCommand(value = "compaction unschedule", help = "Unschedule Compaction")
  public String unscheduleCompaction(
      @CliOption(key = "compactionInstant", mandatory = true, help = "Compaction Instant")
      final String compactionInstant,
      @CliOption(key = {
          "skipValidation"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean skipValidation)
      throws Exception {

    HoodieTableMetaClient metaClient = HoodieCLI.tableMetadata;
    List<Pair<HoodieLogFile, HoodieLogFile>> renameActions =
        CompactionUtils.getRenamingActionsForUnschedulingCompactionPlan(metaClient, compactionInstant,
            Optional.empty(), skipValidation);

    runRenamingOps(metaClient, renameActions);

    // Overwrite compaction request with empty compaction operations
    HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, compactionInstant);
    HoodieCompactionPlan newPlan =
        HoodieCompactionPlan.newBuilder().setOperations(new ArrayList<>()).setExtraMetadata(plan.getExtraMetadata())
            .build();
    HoodieInstant inflight = new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, compactionInstant);
    Path inflightPath = new Path(metaClient.getMetaPath(), inflight.getFileName());
    if (metaClient.getFs().exists(inflightPath)) {
      // revert if in inflight state
      metaClient.getActiveTimeline().revertCompactionInflightToRequested(inflight);
    }
    metaClient.getActiveTimeline().saveToCompactionRequested(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, compactionInstant),
        AvroUtils.serializeCompactionPlan(newPlan));
    return "Successfully removed all file-ids from compaction instant " + compactionInstant;
  }

  /**
   * Remove a fileId from a pending compaction request
   * @param fileId   FileId to be removed
   * @param skipValidation Skip validation
   * @return
   * @throws Exception
   */
  @CliCommand(value = "compaction unscheduleFileId", help = "UnSchedule Compaction for a fileId")
  public String unscheduleCompactFile(
      @CliOption(key = "fileId", mandatory = true, help = "File Id") final String fileId,
      @CliOption(key = {
          "skipValidation"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean skipValidation)
      throws Exception {

    HoodieTableMetaClient metaClient = HoodieCLI.tableMetadata;
    List<Pair<HoodieLogFile, HoodieLogFile>> renameActions =
        CompactionUtils.getRenamingActionsForUnschedulingCompactionForFileId(metaClient, fileId,
            Optional.empty(), skipValidation);

    runRenamingOps(metaClient, renameActions);

    // Ready to remove this file-Id from compaction request
    Pair<String, HoodieCompactionOperation> compactionOperationWithInstant =
        CompactionUtils.getAllPendingCompactionOperations(metaClient).get(fileId);
    HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, compactionOperationWithInstant.getKey());
    List<HoodieCompactionOperation> newOps = plan.getOperations().stream()
        .filter(op -> !op.getFileId().equals(fileId)).collect(Collectors.toList());
    HoodieCompactionPlan newPlan =
        HoodieCompactionPlan.newBuilder().setOperations(newOps).setExtraMetadata(plan.getExtraMetadata()).build();
    HoodieInstant inflight = new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION,
        compactionOperationWithInstant.getLeft());
    Path inflightPath = new Path(metaClient.getMetaPath(), inflight.getFileName());
    if (metaClient.getFs().exists(inflightPath)) {
      // revert if in inflight state
      metaClient.getActiveTimeline().revertCompactionInflightToRequested(inflight);
    }
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

  /**
   * This operation is to repair the log-file names such that the compaction request
   * is consistent with the state of fileIds. This operation is typically used when
   * there are partial failures (IOExceptions) when unscheduling fileIds or compactions
   * @param compactionInstant Compaction Instant to be repaired
   * @return
   * @throws Exception
   */
  @CliCommand(value = "compaction repair", help = "Repair Compaction")
  public String repairCompaction(
      @CliOption(key = "compactionInstant", mandatory = true, help = "Compaction Instant")
      final String compactionInstant)
      throws Exception {
    HoodieTableMetaClient metaClient = HoodieCLI.tableMetadata;
    List<CompactionValidationResult> validationResults =
        CompactionUtils.validateCompactionPlan(metaClient, compactionInstant);
    List<CompactionValidationResult> failed = validationResults.stream()
        .filter(v -> !v.isSuccess()).collect(Collectors.toList());
    if (failed.isEmpty()) {
      return "Compaction instant " + compactionInstant + " already valid. Nothing to repair.";
    }

    final HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitsAndCompactionTimeline());
    List<Pair<HoodieLogFile, HoodieLogFile>> renameActions = failed.stream().flatMap(v ->
        CompactionUtils.getRenamingActionsToAlignWithCompactionOperation(metaClient, compactionInstant,
            v.getOperation(), Optional.of(fsView)).stream()).collect(Collectors.toList());
    runRenamingOps(metaClient, renameActions);
    validationResults = CompactionUtils.validateCompactionPlan(metaClient, compactionInstant);
    failed = validationResults.stream().filter(v -> !v.isSuccess()).collect(Collectors.toList());
    List<Comparable[]> rows = new ArrayList<>();
    failed.stream().forEach(r -> {
      Comparable[] row = new Comparable[]{r.getOperation().getFileId(),
          r.getOperation().getBaseInstantTime(), r.getOperation().getDataFilePath().orElse(""),
          r.getOperation().getDeltaFilePaths().size(), r.isSuccess(),
          r.getErrorMessage().isPresent() ? r.getErrorMessage().get().getMessage() : ""};
      rows.add(row);
    });

    System.out.println("\n\n\nThe following file-Ids are still invalid after repair:\n\n\n");
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    TableHeader header = new TableHeader()
        .addTableHeaderField("File Id")
        .addTableHeaderField("Base Instant Time")
        .addTableHeaderField("Base Data File")
        .addTableHeaderField("Num Delta Files")
        .addTableHeaderField("Valid")
        .addTableHeaderField("Error");

    return HoodiePrintHelper.print(header, fieldNameToConverterMap, null, false, -1, false, rows);
  }

  /**
   * Schedule compaction run
   * @param tableName     Table Name to be compacted
   * @return
   * @throws Exception
   */
  @CliCommand(value = "compaction schedule", help = "Schedule Compaction")
  public String scheduleCompact(
      @CliOption(key = "tableName", mandatory = true, help = "Table name") String tableName,
      @CliOption(key = "strategyClass",
          unspecifiedDefaultValue = "com.uber.hoodie.io.compact.strategy.UnBoundedCompactionStrategy",
          help = "Strategy Class") String strategyClass,
      @CliOption(key = "extraMetadata", unspecifiedDefaultValue = "{}", help = "Extra Metadata in Json")
      final String extraMetadata,
      @CliOption(key = "sparkMemory", mandatory = true, help = "Spark executor memory") String sparkMemory)
      throws Exception {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    // First get a compaction instant time and pass it to spark launcher for scheduling compaction
    String compactionInstantTime = HoodieActiveTimeline.createNewCommitTime();

    if (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ) {
      String sparkPropertiesPath = Utils.getDefaultPropertiesFile(
          scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
      SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
      // Ensure extraMetadata is valid
      new ObjectMapper().readValue(extraMetadata, HashMap.class);

      sparkLauncher.addAppArgs(SparkCommand.COMPACT_SCHEDULE.toString(), HoodieCLI.tableMetadata.getBasePath(),
          tableName, compactionInstantTime, "", "1", "", sparkMemory, "1", strategyClass, extraMetadata);
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

  /**
   * Run Compaction
   *
   * @param tableName             Table Name to run compaction
   * @param rowKeyField           Row Key Field
   * @param parallelism           Parallelism
   * @param schemaFilePath        Schema File Path
   * @param sparkMemory           Spark Executor Memory
   * @param extraMetadata         Extra Metadata
   * @param retry                 Num Retries
   * @param compactionInstantTime Compaction Instant Time
   */
  @CliCommand(value = "compaction run", help = "Run Compaction for given instant time")
  public String compact(
      @CliOption(key = "tableName", mandatory = true, help = "Table name") final String tableName,
      @CliOption(key = "rowKeyField", mandatory = true, help = "Row key field name") final String rowKeyField,
      @CliOption(key = {
          "parallelism"}, mandatory = true, help = "Parallelism for hoodie compaction") final String parallelism,
      @CliOption(key = "schemaFilePath", mandatory = true, help = "Path for Avro schema file") final String
          schemaFilePath,
      @CliOption(key = "sparkMemory", mandatory = true, help = "Spark executor memory") final String sparkMemory,
      @CliOption(key = "extraMetadata", unspecifiedDefaultValue = "{}", help = "Extra Metadata in Json")
      final String extraMetadata,
      @CliOption(key = "retry", mandatory = true, help = "Number of retries") final String retry,
      @CliOption(key = "compactionInstant", mandatory = true, help = "Base path for the target hoodie dataset") final
      String compactionInstantTime) throws Exception {
    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    if (HoodieCLI.tableMetadata.getTableType() == HoodieTableType.MERGE_ON_READ) {
      // Ensure extraMetadata is valid
      new ObjectMapper().readValue(extraMetadata, HashMap.class);
      String sparkPropertiesPath = Utils.getDefaultPropertiesFile(
          scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
      SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
      sparkLauncher.addAppArgs(SparkCommand.COMPACT_RUN.toString(), HoodieCLI.tableMetadata.getBasePath(),
          tableName, compactionInstantTime, rowKeyField, parallelism, schemaFilePath, sparkMemory, retry, "",
          extraMetadata);
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