/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

import static com.uber.hoodie.common.model.HoodieTestUtils.DEFAULT_PARTITION_PATHS;
import static com.uber.hoodie.common.model.HoodieTestUtils.getDefaultHadoopConf;
import static com.uber.hoodie.common.table.HoodieTimeline.COMPACTION_ACTION;
import static com.uber.hoodie.common.table.HoodieTimeline.DELTA_COMMIT_ACTION;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.hoodie.avro.model.HoodieCompactionOperation;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.CompactionUtils.CompactionValidationResult;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCompactionUtils {

  private static final Map<String, Double> metrics =
      new ImmutableMap.Builder<String, Double>()
          .put("key1", 1.0)
          .put("key2", 3.0).build();
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  private HoodieTableMetaClient metaClient;
  private String basePath;
  private Function<Pair<String, FileSlice>, Map<String, Double>> metricsCaptureFn = (partitionFileSlice) -> metrics;

  @Before
  public void init() throws IOException {
    metaClient = HoodieTestUtils.initTableType(getDefaultHadoopConf(),
        tmpFolder.getRoot().getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    basePath = metaClient.getBasePath();
  }

  @Test
  public void testBuildFromFileSlice() {
    // Empty File-Slice with no data and log files
    FileSlice emptyFileSlice = new FileSlice("000", "empty1");
    HoodieCompactionOperation op = CompactionUtils.buildFromFileSlice(
        DEFAULT_PARTITION_PATHS[0], emptyFileSlice, Optional.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(emptyFileSlice, op, DEFAULT_PARTITION_PATHS[0]);

    // File Slice with data-file but no log files
    FileSlice noLogFileSlice = new FileSlice("000", "noLog1");
    noLogFileSlice.setDataFile(new TestHoodieDataFile("/tmp/noLog.parquet"));
    op = CompactionUtils.buildFromFileSlice(
        DEFAULT_PARTITION_PATHS[0], noLogFileSlice, Optional.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(noLogFileSlice, op, DEFAULT_PARTITION_PATHS[0]);

    //File Slice with no data-file but log files present
    FileSlice noDataFileSlice = new FileSlice("000", "noData1");
    noDataFileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 1))));
    noDataFileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 2))));
    op = CompactionUtils.buildFromFileSlice(
        DEFAULT_PARTITION_PATHS[0], noDataFileSlice, Optional.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(noDataFileSlice, op, DEFAULT_PARTITION_PATHS[0]);

    //File Slice with data-file and log files present
    FileSlice fileSlice = new FileSlice("000", "noData1");
    fileSlice.setDataFile(new TestHoodieDataFile("/tmp/noLog.parquet"));
    fileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 1))));
    fileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 2))));
    op = CompactionUtils.buildFromFileSlice(
        DEFAULT_PARTITION_PATHS[0], fileSlice, Optional.of(metricsCaptureFn));
    testFileSliceCompactionOpEquality(fileSlice, op, DEFAULT_PARTITION_PATHS[0]);
  }

  /**
   * Generate input for compaction plan tests
   */
  private Pair<List<Pair<String, FileSlice>>, HoodieCompactionPlan> buildCompactionPlan() {
    FileSlice emptyFileSlice = new FileSlice("000", "empty1");
    FileSlice fileSlice = new FileSlice("000", "noData1");
    fileSlice.setDataFile(new TestHoodieDataFile("/tmp/noLog.parquet"));
    fileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 1))));
    fileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 2))));
    FileSlice noLogFileSlice = new FileSlice("000", "noLog1");
    noLogFileSlice.setDataFile(new TestHoodieDataFile("/tmp/noLog.parquet"));
    FileSlice noDataFileSlice = new FileSlice("000", "noData1");
    noDataFileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 1))));
    noDataFileSlice.addLogFile(new HoodieLogFile(new Path(
        FSUtils.makeLogFileName("noData1", ".log", "000", 2))));
    List<FileSlice> fileSliceList = Arrays.asList(emptyFileSlice, noDataFileSlice, fileSlice, noLogFileSlice);
    List<Pair<String, FileSlice>> input = fileSliceList.stream().map(f -> Pair.of(DEFAULT_PARTITION_PATHS[0], f))
        .collect(Collectors.toList());
    return Pair.of(input, CompactionUtils.buildFromFileSlices(input, Optional.empty(), Optional.of(metricsCaptureFn)));
  }

  @Test
  public void testBuildFromFileSlices() {
    Pair<List<Pair<String, FileSlice>>, HoodieCompactionPlan> inputAndPlan = buildCompactionPlan();
    testFileSlicesCompactionPlanEquality(inputAndPlan.getKey(), inputAndPlan.getValue());
  }

  @Test
  public void testCompactionTransformation() {
    // check HoodieCompactionOperation <=> CompactionOperation transformation function
    Pair<List<Pair<String, FileSlice>>, HoodieCompactionPlan> inputAndPlan = buildCompactionPlan();
    HoodieCompactionPlan plan = inputAndPlan.getRight();
    List<HoodieCompactionOperation> originalOps = plan.getOperations();
    List<HoodieCompactionOperation> regeneratedOps =
        originalOps.stream().map(op -> {
          // Convert to CompactionOperation
          return CompactionUtils.buildCompactionOperation(op);
        }).map(op2 -> {
          // Convert back to HoodieCompactionOperation and check for equality
          return CompactionUtils.buildHoodieCompactionOperation(op2);
        }).collect(Collectors.toList());
    Assert.assertTrue("Transformation did get tested", originalOps.size() > 0);
    Assert.assertEquals("All fields set correctly in transformations", originalOps, regeneratedOps);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetAllPendingCompactionOperationsWithDupFileId() throws IOException {
    // Case where there is duplicate fileIds in compaction requests
    HoodieCompactionPlan plan1 = createCompactionPlan("000", "001", 10, true, true);
    HoodieCompactionPlan plan2 = createCompactionPlan("002", "003", 0, false, false);
    scheduleCompaction("001", plan1);
    scheduleCompaction("003", plan2);
    // schedule same plan again so that there will be duplicates
    scheduleCompaction("005", plan1);
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    Map<String, Pair<String, HoodieCompactionOperation>> res =
        CompactionUtils.getAllPendingCompactionOperations(metaClient);
  }

  @Test
  public void testGetAllPendingCompactionOperations() throws IOException {
    // Case where there are 4 compaction requests where 1 is empty.
    testGetAllPendingCompactionOperations(false, 10, 10, 10, 0);
  }

  @Test
  public void testGetAllPendingInflightCompactionOperations() throws IOException {
    // Case where there are 4 compaction requests where 1 is empty. All of them are marked inflight
    testGetAllPendingCompactionOperations(true, 10, 10, 10, 0);
  }

  @Test
  public void testGetAllPendingCompactionOperationsForEmptyCompactions() throws IOException {
    // Case where there are 4 compaction requests and all are empty.
    testGetAllPendingCompactionOperations(false, 0, 0, 0, 0);
  }

  @Test
  public void testUnscheduleCompactionPlan() throws IOException {
    int numEntriesPerInstant = 10;
    testGetAllPendingCompactionOperations(false, numEntriesPerInstant, numEntriesPerInstant,
        numEntriesPerInstant, numEntriesPerInstant);
    // THere are delta-commits after compaction instant
    validateUnSchedule("000", "001", numEntriesPerInstant, 2 * numEntriesPerInstant);
    // THere are delta-commits after compaction instant
    validateUnSchedule("002", "003", numEntriesPerInstant, 2 * numEntriesPerInstant);
    // THere are no delta-commits after compaction instant
    validateUnSchedule("004", "005", numEntriesPerInstant, 0);
    // THere are no delta-commits after compaction instant
    validateUnSchedule("006", "007", numEntriesPerInstant, 0);
  }

  @Test
  public void testRepairCompactionPlan() throws IOException {
    int numEntriesPerInstant = 10;
    testGetAllPendingCompactionOperations(false, numEntriesPerInstant, numEntriesPerInstant,
        numEntriesPerInstant, numEntriesPerInstant);
    // THere are delta-commits after compaction instant
    validateRepair("000", "001", numEntriesPerInstant, 2 * numEntriesPerInstant);
    // THere are delta-commits after compaction instant
    validateRepair("002", "003", numEntriesPerInstant, 2 * numEntriesPerInstant);
    // THere are no delta-commits after compaction instant
    validateRepair("004", "005", numEntriesPerInstant, 0);
    // THere are no delta-commits after compaction instant
    validateRepair("006", "007", numEntriesPerInstant, 0);
  }

  private void validateRepair(String ingestionInstant, String compactionInstant, int numEntriesPerInstant,
      int expNumRepairs) throws IOException {
    List<Pair<HoodieLogFile, HoodieLogFile>> renameFiles =
        validateUnSchedule(ingestionInstant, compactionInstant, numEntriesPerInstant, expNumRepairs);
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    List<CompactionValidationResult> result = CompactionUtils.validateCompactionPlan(metaClient, compactionInstant);
    if (expNumRepairs > 0) {
      Assert.assertTrue("Expect some failures in validation", result.stream().filter(r -> !r.isSuccess()).count() > 0);
    }
    // Now repair
    List<Pair<HoodieLogFile, HoodieLogFile>> undoFiles = result.stream().flatMap(r ->
        CompactionUtils.getRenamingActionsToAlignWithCompactionOperation(metaClient,
            compactionInstant, r.getOperation(), Optional.empty()).stream())
        .map(rn -> {
          try {
            CompactionUtils.renameLogFile(metaClient, rn.getKey(), rn.getValue());
          } catch (IOException e) {
            throw new HoodieIOException(e.getMessage(), e);
          }
          return rn;
        }).collect(Collectors.toList());
    Map<String, String> renameFilesFromUndo =
        undoFiles.stream().collect(Collectors.toMap(p -> p.getRight().getPath().toString(),
            x -> x.getLeft().getPath().toString()));
    Map<String, String> expRenameFiles =
        renameFiles.stream().collect(Collectors.toMap(p -> p.getLeft().getPath().toString(),
            x -> x.getRight().getPath().toString()));
    if (expNumRepairs > 0) {
      Assert.assertFalse("Rename Files must be non-empty", renameFiles.isEmpty());
    } else {
      Assert.assertTrue("Rename Files must be empty", renameFiles.isEmpty());
    }
    expRenameFiles.entrySet().stream().forEach(r -> {
      System.out.println("Key :" + r.getKey() + " renamed to " + r.getValue() + " rolled back to "
          + renameFilesFromUndo.get(r.getKey()));
    });

    Assert.assertEquals("Undo must completely rollback renames", expRenameFiles, renameFilesFromUndo);
    // Now expect validation to succeed
    result = CompactionUtils.validateCompactionPlan(metaClient, compactionInstant);
    Assert.assertTrue("Expect no failures in validation", result.stream().filter(r -> !r.isSuccess()).count() == 0);
    Assert.assertEquals("Expected Num Repairs", expNumRepairs, undoFiles.size());
  }

  private List<Pair<HoodieLogFile, HoodieLogFile>> validateUnSchedule(String ingestionInstant,
      String compactionInstant, int numEntriesPerInstant, int expNumRenames) throws IOException {
    List<CompactionValidationResult> validationResults = CompactionUtils.validateCompactionPlan(metaClient,
        compactionInstant);
    Assert.assertFalse("Some validations failed",
        validationResults.stream().filter(v -> !v.isSuccess()).findAny().isPresent());
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(metaClient, compactionInstant);
    List<Pair<HoodieLogFile, HoodieLogFile>> renameFiles =
        CompactionUtils.getRenamingActionsForUnschedulingCompactionPlan(metaClient, compactionInstant,
            Optional.empty(), false);
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    /*
     * Log files belonging to file-slices created because of compaction request must be renamed
     */
    Set<HoodieLogFile> gotLogFilesToBeRenamed = renameFiles.stream().map(p -> p.getLeft()).collect(Collectors.toSet());
    final HoodieTableFileSystemView fsView =
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    Set<HoodieLogFile> expLogFilesToBeRenamed = fsView.getLatestFileSlices(DEFAULT_PARTITION_PATHS[0])
        .filter(fs -> fs.getBaseInstantTime().equals(compactionInstant))
        .flatMap(fs -> fs.getLogFiles())
        .collect(Collectors.toSet());
    Assert.assertEquals("Log files belonging to file-slices created because of compaction request must be renamed",
        expLogFilesToBeRenamed, gotLogFilesToBeRenamed);

    /**
     * Ensure new names of log-files are on expected lines
     */
    Set<HoodieLogFile> uniqNewLogFiles = new HashSet<>();
    Set<HoodieLogFile> uniqOldLogFiles = new HashSet<>();

    renameFiles.stream().forEach(lfPair -> {
      Assert.assertFalse("Old Log File Names do not collide", uniqOldLogFiles.contains(lfPair.getKey()));
      Assert.assertFalse("New Log File Names do not collide", uniqNewLogFiles.contains(lfPair.getValue()));
      uniqOldLogFiles.add(lfPair.getKey());
      uniqNewLogFiles.add(lfPair.getValue());
    });

    renameFiles.stream().forEach(lfPair -> {
      HoodieLogFile oldLogFile = lfPair.getLeft();
      HoodieLogFile newLogFile = lfPair.getValue();
      Assert.assertEquals("Base Commit time is expected", ingestionInstant, newLogFile.getBaseCommitTime());
      Assert.assertEquals("Base Commit time is expected", compactionInstant, oldLogFile.getBaseCommitTime());
      Assert.assertEquals("File Id is expected", oldLogFile.getFileId(), newLogFile.getFileId());
      HoodieLogFile lastLogFileBeforeCompaction =
          fsView.getLatestMergedFileSlicesBeforeOrOn(DEFAULT_PARTITION_PATHS[0], ingestionInstant)
              .filter(fs -> fs.getFileId().equals(oldLogFile.getFileId()))
              .map(fs -> fs.getLogFiles().findFirst().get()).findFirst().get();
      Assert.assertEquals("Log Version expected",
          lastLogFileBeforeCompaction.getLogVersion() + oldLogFile.getLogVersion(),
          newLogFile.getLogVersion());
      Assert.assertTrue("Log version does not collide",
          newLogFile.getLogVersion() > lastLogFileBeforeCompaction.getLogVersion());
    });

    Map<String, Long> fileIdToCountsBeforeRenaming =
        fsView.getLatestMergedFileSlicesBeforeOrOn(DEFAULT_PARTITION_PATHS[0], compactionInstant)
            .filter(fs -> fs.getBaseInstantTime().equals(ingestionInstant))
            .map(fs -> Pair.of(fs.getFileId(), fs.getLogFiles().count()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    // Do the renaming
    renameFiles.stream().forEach(lfPair -> {
      try {
        CompactionUtils.renameLogFile(metaClient, lfPair.getLeft(), lfPair.getRight());
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });

    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    final HoodieTableFileSystemView newFsView =
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    // Expect all file-slice whose base-commit is same as compaction commit to contain no new Log files
    newFsView.getLatestFileSlicesBeforeOrOn(DEFAULT_PARTITION_PATHS[0], compactionInstant)
        .filter(fs -> fs.getBaseInstantTime().equals(compactionInstant)).forEach(fs -> {
          Assert.assertFalse("No Data file must be present", fs.getDataFile().isPresent());
          Assert.assertTrue("No Log Files", fs.getLogFiles().count() == 0);
        });

    // Ensure same number of log-files before and after renaming per fileId
    Map<String, Long> fileIdToCountsAfterRenaming =
        newFsView.getAllFileGroups(DEFAULT_PARTITION_PATHS[0]).flatMap(fg -> fg.getAllFileSlices())
            .filter(fs -> fs.getBaseInstantTime().equals(ingestionInstant))
            .map(fs -> Pair.of(fs.getFileId(), fs.getLogFiles().count()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    Assert.assertEquals("Each File Id has same number of log-files",
        fileIdToCountsBeforeRenaming, fileIdToCountsAfterRenaming);
    Assert.assertEquals("Not Empty", numEntriesPerInstant, fileIdToCountsAfterRenaming.size());
    Assert.assertEquals("Expected number of renames", expNumRenames, renameFiles.size());
    return renameFiles;
  }

  private Map<String, Pair<String, HoodieCompactionOperation>> testGetAllPendingCompactionOperations(boolean inflight,
      int numEntriesInPlan1, int numEntriesInPlan2,
      int numEntriesInPlan3, int numEntriesInPlan4) throws IOException {
    HoodieCompactionPlan plan1 = createCompactionPlan("000", "001", numEntriesInPlan1, true, true);
    HoodieCompactionPlan plan2 = createCompactionPlan("002", "003", numEntriesInPlan2, false, true);
    HoodieCompactionPlan plan3 = createCompactionPlan("004", "005", numEntriesInPlan3, true, false);
    HoodieCompactionPlan plan4 = createCompactionPlan("006", "007", numEntriesInPlan4, false, false);

    if (inflight) {
      scheduleInflightCompaction("001", plan1);
      scheduleInflightCompaction("003", plan2);
      scheduleInflightCompaction("005", plan3);
      scheduleInflightCompaction("007", plan4);
    } else {
      scheduleCompaction("001", plan1);
      scheduleCompaction("003", plan2);
      scheduleCompaction("005", plan3);
      scheduleCompaction("007", plan4);
    }

    createDeltaCommit("000");
    createDeltaCommit("002");
    createDeltaCommit("004");
    createDeltaCommit("006");

    Map<String, String> baseInstantsToCompaction =
        new ImmutableMap.Builder<String, String>().put("000", "001").put("002", "003")
            .put("004", "005").put("006", "007").build();
    List<Integer> expectedNumEntries =
        Arrays.asList(numEntriesInPlan1, numEntriesInPlan2, numEntriesInPlan3, numEntriesInPlan4);
    List<HoodieCompactionPlan> plans = new ImmutableList.Builder<HoodieCompactionPlan>()
        .add(plan1, plan2, plan3, plan4).build();
    IntStream.range(0, 4).boxed().forEach(idx -> {
      if (expectedNumEntries.get(idx) > 0) {
        Assert.assertEquals("check if plan " + idx + " has exp entries",
            expectedNumEntries.get(idx).longValue(), plans.get(idx).getOperations().size());
      } else {
        Assert.assertNull("Plan " + idx + " has null ops", plans.get(idx).getOperations());
      }
    });

    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    Map<String, Pair<String, HoodieCompactionOperation>> pendingCompactionMap =
        CompactionUtils.getAllPendingCompactionOperations(metaClient);

    Map<String, Pair<String, HoodieCompactionOperation>> expPendingCompactionMap =
        generateExpectedCompactionOperations(Arrays.asList(plan1, plan2, plan3, plan4), baseInstantsToCompaction);

    // Ensure Compaction operations are fine.
    Assert.assertEquals(expPendingCompactionMap, pendingCompactionMap);
    return expPendingCompactionMap;
  }

  private Map<String, Pair<String, HoodieCompactionOperation>> generateExpectedCompactionOperations(
      List<HoodieCompactionPlan> plans, Map<String, String> baseInstantsToCompaction) {
    return plans.stream()
        .flatMap(plan -> {
          if (plan.getOperations() != null) {
            return plan.getOperations().stream().map(op -> Pair.of(op.getFileId(),
                Pair.of(baseInstantsToCompaction.get(op.getBaseInstantTime()), op)));
          }
          return Stream.empty();
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  private void scheduleCompaction(String instantTime, HoodieCompactionPlan compactionPlan) throws IOException {
    metaClient.getActiveTimeline().saveToCompactionRequested(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, instantTime),
        AvroUtils.serializeCompactionPlan(compactionPlan));
  }

  private void createDeltaCommit(String instantTime) throws IOException {
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(State.INFLIGHT, DELTA_COMMIT_ACTION, instantTime), Optional.empty());
  }

  private void scheduleInflightCompaction(String instantTime, HoodieCompactionPlan compactionPlan) throws IOException {
    scheduleCompaction(instantTime, compactionPlan);
    metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(
        new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, instantTime));
  }

  private HoodieCompactionPlan createCompactionPlan(String instantId, String compactionInstantId,
      int numFileIds, boolean createDataFile, boolean deltaCommitsAfterCompactionRequests) {
    List<HoodieCompactionOperation> ops = IntStream.range(0, numFileIds).boxed().map(idx -> {
      try {
        String fileId = UUID.randomUUID().toString();
        if (createDataFile) {
          HoodieTestUtils.createDataFile(basePath, DEFAULT_PARTITION_PATHS[0], instantId, fileId);
        }
        HoodieTestUtils.createNewLogFile(metaClient.getFs(), basePath, DEFAULT_PARTITION_PATHS[0],
            instantId, fileId, Optional.of(1));
        HoodieTestUtils.createNewLogFile(metaClient.getFs(), basePath, DEFAULT_PARTITION_PATHS[0],
            instantId, fileId, Optional.of(2));
        FileSlice slice = new FileSlice(instantId, fileId);
        if (createDataFile) {
          slice.setDataFile(new TestHoodieDataFile(metaClient.getBasePath() + "/" + DEFAULT_PARTITION_PATHS[0]
              + "/" + FSUtils.makeDataFileName(instantId, 1, fileId)));
        }
        String logFilePath1 = HoodieTestUtils.getLogFilePath(basePath, DEFAULT_PARTITION_PATHS[0], instantId, fileId,
            Optional.of(1));
        String logFilePath2 = HoodieTestUtils.getLogFilePath(basePath, DEFAULT_PARTITION_PATHS[0], instantId, fileId,
            Optional.of(2));
        slice.addLogFile(new HoodieLogFile(new Path(logFilePath1)));
        slice.addLogFile(new HoodieLogFile(new Path(logFilePath2)));
        HoodieCompactionOperation op =
            CompactionUtils.buildFromFileSlice(DEFAULT_PARTITION_PATHS[0], slice, Optional.empty());
        if (deltaCommitsAfterCompactionRequests) {
          HoodieTestUtils.createNewLogFile(metaClient.getFs(), basePath, DEFAULT_PARTITION_PATHS[0],
              compactionInstantId, fileId, Optional.of(1));
          HoodieTestUtils.createNewLogFile(metaClient.getFs(), basePath, DEFAULT_PARTITION_PATHS[0],
              compactionInstantId, fileId, Optional.of(2));
        }
        return op;
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }).collect(Collectors.toList());
    return new HoodieCompactionPlan(ops.isEmpty() ? null : ops, new HashMap<>());
  }

  /**
   * Validates if  generated compaction plan matches with input file-slices
   *
   * @param input File Slices with partition-path
   * @param plan  Compaction Plan
   */
  private void testFileSlicesCompactionPlanEquality(List<Pair<String, FileSlice>> input,
      HoodieCompactionPlan plan) {
    Assert.assertEquals("All file-slices present", input.size(), plan.getOperations().size());
    IntStream.range(0, input.size()).boxed().forEach(idx ->
        testFileSliceCompactionOpEquality(input.get(idx).getValue(), plan.getOperations().get(idx),
            input.get(idx).getKey()));
  }

  /**
   * Validates if generated compaction operation matches with input file slice and partition path
   *
   * @param slice            File Slice
   * @param op               HoodieCompactionOperation
   * @param expPartitionPath Partition path
   */
  private void testFileSliceCompactionOpEquality(FileSlice slice, HoodieCompactionOperation op,
      String expPartitionPath) {
    Assert.assertEquals("Partition path is correct", expPartitionPath, op.getPartitionPath());
    Assert.assertEquals("Same base-instant", slice.getBaseInstantTime(), op.getBaseInstantTime());
    Assert.assertEquals("Same file-id", slice.getFileId(), op.getFileId());
    if (slice.getDataFile().isPresent()) {
      Assert.assertEquals("Same data-file", slice.getDataFile().get().getPath(), op.getDataFilePath());
    }
    List<String> paths = slice.getLogFiles().map(l -> l.getPath().toString()).collect(Collectors.toList());
    IntStream.range(0, paths.size()).boxed().forEach(idx -> {
      Assert.assertEquals("Log File Index " + idx, paths.get(idx), op.getDeltaFilePaths().get(idx));
    });
    Assert.assertEquals("Metrics set", metrics, op.getMetrics());
  }


  private static class TestHoodieDataFile extends HoodieDataFile {

    private final String path;

    public TestHoodieDataFile(String path) {
      super(null);
      this.path = path;
    }

    @Override
    public String getPath() {
      return path;
    }

    @Override
    public String getFileId() {
      return UUID.randomUUID().toString();
    }

    @Override
    public String getCommitTime() {
      return "100";
    }

    @Override
    public long getFileSize() {
      return 0;
    }
  }
}
