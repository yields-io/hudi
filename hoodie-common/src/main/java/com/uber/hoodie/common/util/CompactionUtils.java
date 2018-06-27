/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util;

import com.google.common.base.Preconditions;
import com.uber.hoodie.avro.model.HoodieCompactionOperation;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.common.model.CompactionOperation;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Helper class to generate compaction plan from FileGroup/FileSlice abstraction
 */
public class CompactionUtils {

  /**
   * Generate compaction operation from file-slice
   *
   * @param partitionPath          Partition path
   * @param fileSlice              File Slice
   * @param metricsCaptureFunction Metrics Capture function
   * @return Compaction Operation
   */
  public static HoodieCompactionOperation buildFromFileSlice(String partitionPath, FileSlice fileSlice,
      Optional<Function<Pair<String, FileSlice>, Map<String, Double>>> metricsCaptureFunction) {
    HoodieCompactionOperation.Builder builder = HoodieCompactionOperation.newBuilder();
    builder.setPartitionPath(partitionPath);
    builder.setFileId(fileSlice.getFileId());
    builder.setBaseInstantTime(fileSlice.getBaseInstantTime());
    builder.setDeltaFilePaths(fileSlice.getLogFiles().map(lf -> lf.getPath().toString()).collect(Collectors.toList()));
    if (fileSlice.getDataFile().isPresent()) {
      builder.setDataFilePath(fileSlice.getDataFile().get().getPath());
    }

    if (metricsCaptureFunction.isPresent()) {
      builder.setMetrics(metricsCaptureFunction.get().apply(Pair.of(partitionPath, fileSlice)));
    }
    return builder.build();
  }

  /**
   * Generate compaction plan from file-slices
   *
   * @param partitionFileSlicePairs list of partition file-slice pairs
   * @param extraMetadata           Extra Metadata
   * @param metricsCaptureFunction  Metrics Capture function
   */
  public static HoodieCompactionPlan buildFromFileSlices(
      List<Pair<String, FileSlice>> partitionFileSlicePairs,
      Optional<Map<String, String>> extraMetadata,
      Optional<Function<Pair<String, FileSlice>, Map<String, Double>>> metricsCaptureFunction) {
    HoodieCompactionPlan.Builder builder = HoodieCompactionPlan.newBuilder();
    extraMetadata.ifPresent(m -> builder.setExtraMetadata(m));
    builder.setOperations(partitionFileSlicePairs.stream().map(pfPair ->
        buildFromFileSlice(pfPair.getKey(), pfPair.getValue(), metricsCaptureFunction)).collect(Collectors.toList()));
    return builder.build();
  }

  /**
   * Build Avro generated Compaction operation payload from compaction operation POJO for serialization
   */
  public static HoodieCompactionOperation buildHoodieCompactionOperation(CompactionOperation op) {
    return HoodieCompactionOperation.newBuilder().setFileId(op.getFileId())
        .setBaseInstantTime(op.getBaseInstantTime())
        .setPartitionPath(op.getPartitionPath())
        .setDataFilePath(op.getDataFilePath().isPresent() ? op.getDataFilePath().get() : null)
        .setDeltaFilePaths(op.getDeltaFilePaths())
        .setMetrics(op.getMetrics()).build();
  }

  /**
   * Build Compaction operation payload from Avro version for using in Spark executors
   *
   * @param hc HoodieCompactionOperation
   */
  public static CompactionOperation buildCompactionOperation(HoodieCompactionOperation hc) {
    return CompactionOperation.convertFromAvroRecordInstance(hc);
  }

  /**
   * Get all pending compaction plans along with their instants
   *
   * @param metaClient Hoodie Meta Client
   */
  public static List<Pair<HoodieInstant, HoodieCompactionPlan>> getAllPendingCompactionPlans(
      HoodieTableMetaClient metaClient) {
    List<HoodieInstant> pendingCompactionInstants =
        metaClient.getActiveTimeline().filterPendingCompactionTimeline().getInstants().collect(Collectors.toList());
    return pendingCompactionInstants.stream().map(instant -> {
      try {
        return Pair.of(instant, getCompactionPlan(metaClient, instant.getTimestamp()));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    }).collect(Collectors.toList());
  }

  public static HoodieCompactionPlan getCompactionPlan(HoodieTableMetaClient metaClient,
      String compactionInstant) throws IOException {
    HoodieCompactionPlan compactionPlan = AvroUtils.deserializeCompactionPlan(
        metaClient.getActiveTimeline().getInstantAuxiliaryDetails(
            HoodieTimeline.getCompactionRequestedInstant(compactionInstant)).get());
    return compactionPlan;
  }

  /**
   * Get all file-ids with pending Compaction operations and their target compaction instant time
   *
   * @param metaClient Hoodie Table Meta Client
   */
  public static Map<String, Pair<String, HoodieCompactionOperation>> getAllPendingCompactionOperations(
      HoodieTableMetaClient metaClient) {
    List<Pair<HoodieInstant, HoodieCompactionPlan>> pendingCompactionPlanWithInstants =
        getAllPendingCompactionPlans(metaClient);

    Map<String, Pair<String, HoodieCompactionOperation>> fileIdToPendingCompactionWithInstantMap = new HashMap<>();
    pendingCompactionPlanWithInstants.stream().flatMap(instantPlanPair -> {
      HoodieInstant instant = instantPlanPair.getKey();
      HoodieCompactionPlan compactionPlan = instantPlanPair.getValue();
      List<HoodieCompactionOperation> ops = compactionPlan.getOperations();
      if (null != ops) {
        return ops.stream().map(op -> {
          return Pair.of(op.getFileId(), Pair.of(instant.getTimestamp(), op));
        });
      } else {
        return Stream.empty();
      }
    }).forEach(pair -> {
      // Defensive check to ensure a single-fileId does not have more than one pending compaction
      if (fileIdToPendingCompactionWithInstantMap.containsKey(pair.getKey())) {
        String msg = "Hoodie File Id (" + pair.getKey() + ") has more thant 1 pending compactions. Instants: "
            + pair.getValue() + ", " + fileIdToPendingCompactionWithInstantMap.get(pair.getKey());
        throw new IllegalStateException(msg);
      }
      fileIdToPendingCompactionWithInstantMap.put(pair.getKey(), pair.getValue());
    });
    return fileIdToPendingCompactionWithInstantMap;
  }

  /**
   * Generate renaming actions for unscheduling a pending compaction plan. NOTE: Can only be used safely when no writer
   * (ingestion/compaction) is running.
   *
   * @param metaClient        Hoodie Table MetaClient
   * @param compactionInstant Compaction Instant to be unscheduled
   * @param fsViewOpt         Cached File System View
   * @return list of pairs of log-files (old, new) and for each pair, rename must be done to successfully unschedule
   * compaction.
   */
  public static List<Pair<HoodieLogFile, HoodieLogFile>> getRenamingActionsForUnschedulingCompactionPlan(
      HoodieTableMetaClient metaClient, String compactionInstant, Optional<HoodieTableFileSystemView> fsViewOpt)
      throws IOException {
    HoodieTableFileSystemView fsView = fsViewOpt.isPresent() ? fsViewOpt.get() :
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    HoodieCompactionPlan plan = getCompactionPlan(metaClient, compactionInstant);
    if (plan.getOperations() != null) {
      return plan.getOperations().stream().flatMap(op -> {
        try {
          return getRenamingActionsForUnschedulingCompactionOperation(metaClient, compactionInstant, op,
              Optional.of(fsView)).stream();
        } catch (IOException ioe) {
          throw new HoodieIOException(ioe.getMessage(), ioe);
        } catch (ValidationException ve) {
          throw new HoodieException(ve);
        }
      }).collect(Collectors.toList());
    }
    return new ArrayList<>();
  }

  /**
   * Generate renaming actions for unscheduling a fileId from pending compaction. NOTE: Can only be used safely when no
   * writer (ingestion/compaction) is running.
   *
   * @param metaClient Hoodie Table MetaClient
   * @param fileId     FileId to remove compaction
   * @param fsViewOpt  Cached File System View
   * @return list of pairs of log-files (old, new) and for each pair, rename must be done to successfully unschedule
   * compaction.
   */
  public static List<Pair<HoodieLogFile, HoodieLogFile>> getRenamingActionsForUnschedulingCompactionForFileId(
      HoodieTableMetaClient metaClient, String fileId, Optional<HoodieTableFileSystemView> fsViewOpt)
      throws IOException, ValidationException {
    Map<String, Pair<String, HoodieCompactionOperation>> allPendingCompactions =
        CompactionUtils.getAllPendingCompactionOperations(metaClient);
    if (allPendingCompactions.containsKey(fileId)) {
      Pair<String, HoodieCompactionOperation> opWithInstant = allPendingCompactions.get(fileId);
      return getRenamingActionsForUnschedulingCompactionOperation(metaClient, opWithInstant.getKey(),
          opWithInstant.getValue(), Optional.empty());
    }
    throw new HoodieException("FileId " + fileId + " not in pending compaction");
  }

  /**
   * Generate renaming actions for unscheduling a compaction operation NOTE: Can only be used safely when no writer
   * (ingestion/compaction) is running.
   *
   * @param metaClient        Hoodie Table MetaClient
   * @param compactionInstant Compaction Instant
   * @param operation         Compaction Operation
   * @param fsViewOpt         Cached File System View
   * @return list of pairs of log-files (old, new) and for each pair, rename must be done to successfully unschedule
   * compaction.
   */
  public static List<Pair<HoodieLogFile, HoodieLogFile>> getRenamingActionsForUnschedulingCompactionOperation(
      HoodieTableMetaClient metaClient, String compactionInstant, HoodieCompactionOperation operation,
      Optional<HoodieTableFileSystemView> fsViewOpt) throws IOException, ValidationException {
    List<Pair<HoodieLogFile, HoodieLogFile>> result = new ArrayList<>();
    HoodieTableFileSystemView fileSystemView = fsViewOpt.isPresent() ? fsViewOpt.get() :
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    validateCompactionOperation(metaClient, compactionInstant, operation, Optional.of(fileSystemView));
    HoodieInstant lastInstant = metaClient.getCommitsAndCompactionTimeline().lastInstant().get();
    FileSlice merged =
        fileSystemView.getLatestMergedFileSlicesBeforeOrOn(operation.getPartitionPath(), lastInstant.getTimestamp())
            .filter(fs -> fs.getFileId().equals(operation.getFileId())).findFirst().get();
    List<HoodieLogFile> logFilesToRepair =
        merged.getLogFiles().filter(lf -> lf.getBaseCommitTime().equals(compactionInstant))
            .collect(Collectors.toList());
    logFilesToRepair.sort(HoodieLogFile.getBaseInstantAndLogVersionComparator().reversed());
    FileSlice fileSliceForCompaction =
        fileSystemView.getLatestFileSlicesBeforeOrOn(operation.getPartitionPath(), operation.getBaseInstantTime())
            .filter(fs -> fs.getFileId().equals(operation.getFileId())).findFirst().get();
    int maxUsedVersion =
        fileSliceForCompaction.getLogFiles().findFirst().map(lf -> lf.getLogVersion())
            .orElse(HoodieLogFile.LOGFILE_BASE_VERSION - 1);
    String logExtn = fileSliceForCompaction.getLogFiles().findFirst().map(lf -> "." + lf.getFileExtension())
        .orElse(HoodieLogFile.DELTA_EXTENSION);
    String parentPath = fileSliceForCompaction.getDataFile().map(df -> new Path(df.getPath()).getParent().toString())
        .orElse(fileSliceForCompaction.getLogFiles().findFirst().map(lf -> lf.getPath().getParent().toString()).get());
    for (HoodieLogFile toRepair : logFilesToRepair) {
      int version = maxUsedVersion + 1;
      HoodieLogFile newLf = new HoodieLogFile(new Path(parentPath, FSUtils.makeLogFileName(operation.getFileId(),
          logExtn, operation.getBaseInstantTime(), version)));
      result.add(Pair.of(toRepair, newLf));
      maxUsedVersion = version;
    }
    return result;
  }

  /**
   * Rename log files. This is done for un-scheduling a pending compaction operation NOTE: Can only be used safely when
   * no writer (ingestion/compaction) is running.
   *
   * @param metaClient Hoodie Table Meta-Client
   * @param oldLogFile Old Log File
   * @param newLogFile New Log File
   */
  public static void renameLogFile(HoodieTableMetaClient metaClient, HoodieLogFile oldLogFile,
      HoodieLogFile newLogFile) throws IOException {
    FileStatus[] statuses = metaClient.getFs().listStatus(oldLogFile.getPath());
    Preconditions.checkArgument(statuses.length == 1,"Only one status must be present");
    Preconditions.checkArgument(statuses[0].isFile(), "Source File must exist");
    Preconditions.checkArgument(oldLogFile.getPath().getParent().equals(newLogFile.getPath().getParent()),
        "Log file must only be moved within the parent directory");
    metaClient.getFs().rename(oldLogFile.getPath(), newLogFile.getPath());
  }

  /**
   * Validate if a compaction operation is valid
   *
   * @param metaClient        Hoodie Table Meta client
   * @param compactionInstant Compaction Instant
   * @param operation         Compaction Operation
   * @param fsViewOpt         File System View
   */
  public static void validateCompactionOperation(HoodieTableMetaClient metaClient, String compactionInstant,
      HoodieCompactionOperation operation, Optional<HoodieTableFileSystemView> fsViewOpt)
      throws IOException, ValidationException {
    HoodieTableFileSystemView fileSystemView = fsViewOpt.isPresent() ? fsViewOpt.get() :
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    Optional<HoodieInstant> lastInstant = metaClient.getCommitsAndCompactionTimeline().lastInstant();
    if (lastInstant.isPresent()) {
      Optional<FileSlice> fileSliceOptional =
          fileSystemView.getLatestMergedFileSlicesBeforeOrOn(operation.getPartitionPath(),
              lastInstant.get().getTimestamp()).filter(fs -> fs.getFileId().equals(operation.getFileId())).findFirst();
      if (fileSliceOptional.isPresent()) {
        FileSlice fs = fileSliceOptional.get();
        Optional<HoodieDataFile> df = fs.getDataFile();
        if (operation.getDataFilePath() != null) {
          String expPath = metaClient.getFs().getFileStatus(new Path(operation.getDataFilePath())).getPath().toString();
          Preconditions.checkArgument(df.isPresent(), "Data File must be present");
          Preconditions.checkArgument(df.get().getPath().equals(expPath),
              "Base Path in operation is specified as " + expPath + " but got path " + df.get().getPath());
        }
        Set<HoodieLogFile> logFilesInFileSlice = fs.getLogFiles().collect(Collectors.toSet());
        Set<HoodieLogFile> logFilesInCompactionOp = operation.getDeltaFilePaths().stream()
            .map(dp -> {
              try {
                FileStatus[] fileStatuses = metaClient.getFs().listStatus(new Path(dp));
                Preconditions.checkArgument(fileStatuses.length == 1, "Expect only 1 file-status");
                return new HoodieLogFile(fileStatuses[0]);
              } catch (IOException ioe) {
                throw new HoodieIOException(ioe.getMessage(), ioe);
              }
            }).collect(Collectors.toSet());
        Set<HoodieLogFile> missing =
            logFilesInCompactionOp.stream().filter(lf -> !logFilesInFileSlice.contains(lf)).collect(Collectors.toSet());
        Preconditions.checkArgument(missing.isEmpty(),
            "All log files specified in compaction operation is not present. Missing :" + missing
                + ", Exp :" + logFilesInCompactionOp + ", Got :" + logFilesInFileSlice);
        Set<HoodieLogFile> diff =
            logFilesInFileSlice.stream().filter(lf -> !logFilesInCompactionOp.contains(lf)).collect(Collectors.toSet());
        Preconditions.checkArgument(diff.stream()
                .filter(lf -> !lf.getBaseCommitTime().equals(compactionInstant)).count() == 0,
            "There are some log-files which are neither specified in compaction plan "
                + "nor present after compaction request instant. Some of these :" + diff);
      } else {
        throw new ValidationException("Unable to find file-slice for file-id (" + operation.getFileId()
            + " Compaction operation is invalid.");
      }
    } else {
      throw new ValidationException("Unable to find any committed instant. Compaction Operation may "
          + "be pointing to stale file-slices");
    }
  }

  /**
   * Validate all the compaction operations in a compaction plan
   *
   * @param metaClient        Hoodie Table Meta Client
   * @param compactionInstant Compaction Instant
   */
  public static List<ValidationResult> validateCompactionPlan(HoodieTableMetaClient metaClient,
      String compactionInstant) throws IOException {
    HoodieCompactionPlan plan = getCompactionPlan(metaClient, compactionInstant);
    HoodieTableFileSystemView fsView =
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());

    if (plan.getOperations() != null) {
      return plan.getOperations().stream().map(op -> {
        try {
          validateCompactionOperation(metaClient, compactionInstant, op, Optional.of(fsView));
          return new ValidationResult(op, true, Optional.empty());
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        } catch (ValidationException | IllegalArgumentException e) {
          return new ValidationResult(op, false, Optional.of(e));
        }
      }).collect(Collectors.toList());
    }
    return new ArrayList<>();
  }

  /**
   * Holds validation result for batch validations
   */
  public static class ValidationResult {
    private final HoodieCompactionOperation operation;
    private final boolean success;
    private final Optional<Exception> exception;

    public HoodieCompactionOperation getOperation() {
      return operation;
    }

    public boolean isSuccess() {
      return success;
    }

    public Optional<Exception> getErrorMessage() {
      return exception;
    }

    public ValidationResult(HoodieCompactionOperation operation, boolean success,
        Optional<Exception> exception) {
      this.operation = operation;
      this.success = success;
      this.exception = exception;
    }
  }

  public static class ValidationException extends Exception {

    public ValidationException(String msg) {
      super(msg);
    }
  }
}
