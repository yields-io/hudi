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

package com.uber.hoodie;

import com.codahale.metrics.Timer;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.exception.HoodieCompactionException;
import com.uber.hoodie.metrics.HoodieMetrics;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Non-Public API : Compaction Handler taking care of both scheduling and running compactions
 */
class CompactionClientHandler<T extends HoodieRecordPayload> implements Serializable {

  private static Logger logger = LogManager.getLogger(CompactionClientHandler.class);

  private final transient FileSystem fs;
  private final transient JavaSparkContext jsc;
  private final HoodieWriteConfig config;
  private final transient HoodieMetrics metrics;
  private transient Timer.Context pendingCompactionTimer;

  protected CompactionClientHandler(JavaSparkContext jsc, HoodieWriteConfig clientConfig,
      HoodieMetrics metrics) {
    this.fs = FSUtils.getFs(clientConfig.getBasePath(), jsc.hadoopConfiguration());
    this.jsc = jsc;
    this.config = clientConfig;
    this.metrics = metrics;
  }

  /**
   * Provides a new commit time for a compaction (commit) operation
   */
  protected String startCompaction() {
    String commitTime = HoodieActiveTimeline.createNewCommitTime();
    logger.info("Generate a new commit time " + commitTime);
    startCompactionWithTime(commitTime);
    return commitTime;
  }

  /**
   * Since MOR tableType default to {@link HoodieTimeline#DELTA_COMMIT_ACTION}, we need to
   * explicitly set to {@link HoodieTimeline#COMMIT_ACTION} for compaction
   *
   * @param commitTime Compaction Commit Time
   */
  protected void startCompactionWithTime(String commitTime) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    String commitActionType = HoodieTimeline.COMMIT_ACTION;
    activeTimeline.createInflight(new HoodieInstant(true, commitActionType, commitTime));
  }

  /**
   * Performs a compaction operation on a dataset. WARNING: Compaction operation cannot be executed
   * asynchronously. Please always use this serially before or after an insert/upsert action.
   *
   * @param table         Hoodie Table
   * @param commitTime    Compacton Instant time
   * @param extraMetadata Extra Metadata to store
   * @return WriteStatus RDD of compaction
   */
  protected JavaRDD<WriteStatus> compact(HoodieTable<T> table, String commitTime, boolean autoCommit,
      Optional<Map<String, String>> extraMetadata)
      throws IOException {
    JavaRDD<WriteStatus> statuses = forceCompact(table, commitTime, autoCommit, extraMetadata);
    return statuses;
  }

  /**
   * Performs a compaction operation on a dataset. WARNING: Compaction operation cannot be executed
   * asynchronously. Please always use this serially before or after an insert/upsert action.
   *
   * @param compactionCommitTime Compaction Commit Time
   * @param extraMetadata        Extra Metadata to store
   */
  private void forceCompact(String compactionCommitTime, Optional<Map<String, String>> extraMetadata)
      throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config);
    forceCompact(table, compactionCommitTime, true, extraMetadata);
  }

  /**
   * Run compaction and handle commit/metrics
   *
   * @param table                Hoodie Table
   * @param compactionCommitTime Instant Time
   * @param autoCommit           Commit
   * @param extraMetadata        Extra Metadata to store
   */
  private JavaRDD<WriteStatus> forceCompact(HoodieTable<T> table, String compactionCommitTime, boolean autoCommit,
      Optional<Map<String, String>> extraMetadata) {
    pendingCompactionTimer = metrics.getCommitCtx();
    JavaRDD<WriteStatus> compactedStatuses = table.compact(jsc, compactionCommitTime);

    // Trigger the insert and collect statuses
    compactedStatuses = compactedStatuses.persist(config.getWriteStatusStorageLevel());
    commitCompaction(compactedStatuses, table, compactionCommitTime, autoCommit, extraMetadata);
    return compactedStatuses;
  }

  /**
   * Commit Compaction and track metrics
   *
   * @param compactedStatuses    Compaction Write status
   * @param table                Hoodie Table
   * @param compactionCommitTime Compaction Commit Time
   * @param autoCommit           Auto Commit
   * @param extraMetadata        Extra Metadata to store
   */
  protected void commitCompaction(JavaRDD<WriteStatus> compactedStatuses,
      HoodieTable<T> table, String compactionCommitTime,
      boolean autoCommit, Optional<Map<String, String>> extraMetadata) {
    if (!compactedStatuses.isEmpty() && autoCommit) {
      HoodieCommitMetadata metadata =
          commitForceCompaction(compactedStatuses, table, compactionCommitTime, extraMetadata);
      if (pendingCompactionTimer != null) {
        long durationInMs = metrics.getDurationInMs(pendingCompactionTimer.stop());
        try {
          metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(compactionCommitTime).getTime(),
              durationInMs, metadata, HoodieActiveTimeline.COMPACTION_ACTION);
        } catch (ParseException e) {
          throw new HoodieCommitException(
              "Commit time is not of valid format.Failed to commit compaction " + config.getBasePath()
                  + " at time " + compactionCommitTime, e);
        }
      }
      logger.info("Compacted successfully on commit " + compactionCommitTime);
    } else {
      logger.info("Compaction did not run for commit " + compactionCommitTime);
    }
    // Reset compaction
    pendingCompactionTimer = null;
  }

  /**
   * Performs a compaction operation on a dataset. WARNING: Compaction operation cannot be executed
   * asynchronously. Please always use this serially before or after an insert/upsert action.
   */
  protected String forceCompact() throws IOException {
    String compactionCommitTime = startCompaction();
    forceCompact(compactionCommitTime, Optional.empty());
    return compactionCommitTime;
  }

  /**
   * Commit compaction forcefully
   *
   * @param writeStatuses        Compaction Write statuses
   * @param table                Hoodie Table
   * @param compactionCommitTime Compaction instant time
   */
  private HoodieCommitMetadata commitForceCompaction(JavaRDD<WriteStatus> writeStatuses,
      HoodieTable table, String compactionCommitTime, Optional<Map<String, String>> extraMetadata) {

    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
    List<Tuple2<String, HoodieWriteStat>> stats =
        CommitUtils.generateWriteStats(metadata, writeStatuses, extraMetadata);

    logger.info("Compaction finished with result " + metadata);

    logger.info("Committing Compaction " + compactionCommitTime);
    HoodieActiveTimeline activeTimeline = table.getMetaClient().getActiveTimeline();

    try {
      activeTimeline.saveAsComplete(
          new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, compactionCommitTime),
          Optional.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + table.getMetaClient().getBasePath() + " at time " + compactionCommitTime, e);
    }
    return metadata;
  }
}
