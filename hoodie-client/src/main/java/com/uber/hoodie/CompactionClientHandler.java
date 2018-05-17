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
import com.google.common.collect.ImmutableList;
import com.uber.hoodie.avro.model.HoodieCompactionWorkload;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.util.AvroUtils;
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
  private final RollbackHandler<T> rollbackHandler;
  private final transient HoodieMetrics metrics;
  private transient Timer.Context pendingCompactionTimer;

  protected CompactionClientHandler(JavaSparkContext jsc, HoodieWriteConfig clientConfig,
      RollbackHandler<T> rollbackHandler, HoodieMetrics metrics) {
    this.fs = FSUtils.getFs(clientConfig.getBasePath(), jsc.hadoopConfiguration());
    this.jsc = jsc;
    this.config = clientConfig;
    this.rollbackHandler = rollbackHandler;
    this.metrics = metrics;
  }

  /**
   * Selects file-ids for compaction and creates a new compaction instant time
   *
   * @param extraMetadata Extra Metadata to be set
   * @return Compaction Instant Time
   */
  protected String scheduleCompaction(Optional<Map<String, String>> extraMetadata)
      throws IOException {
    String instantTime = HoodieActiveTimeline.createNewCommitTime();
    logger.info("Generate a new instant time for async compaction :" + instantTime);
    scheduleCompactionWithInstant(instantTime, extraMetadata);
    return instantTime;
  }

  /**
   * Selects file-ids for compaction and schedules compaction for the instant time passed
   *
   * @param instantTime   Instant Time
   * @param extraMetadata Extra Metadata to be set
   */
  protected void scheduleCompactionWithInstant(String instantTime, Optional<Map<String, String>> extraMetadata)
      throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config);
    HoodieCompactionWorkload workload = table.scheduleCompaction(jsc, instantTime);
    extraMetadata.ifPresent(workload::setExtraMetadata);
    HoodieInstant compactionInstant =
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
    metaClient.getActiveTimeline().saveToRequested(compactionInstant, AvroUtils.serializeCompactionWorkload(workload));
  }

  /**
   * Perform compaction operations as specified in the compaction commit file
   *
   * @param table                 Hoodie Table
   * @param compactionInstantTime Compacton Instant time
   * @param autoCommit            Commit after compaction
   * @return WriteStatus RDD of compaction
   */
  protected JavaRDD<WriteStatus> runCompaction(HoodieTable<T> table, String compactionInstantTime, boolean autoCommit)
      throws IOException {
    // Refresh timeline as it possible clients could have called scheduleCompaction and runCompaction without refresh
    HoodieActiveTimeline timeline = table.getActiveTimeline().reload();
    HoodieTimeline pendingCompactionTimeline = timeline.getPendingCompactionTimeline();

    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      //inflight compaction - Needs to rollback first deleting new parquet files before we run compaction.
      rollbackInflightCompaction(inflightInstant, timeline);
    }

    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(instant)) {
      return runCompaction(table, instant, timeline, autoCommit);
    } else {
      throw new IllegalStateException("No Compaction request available at " + compactionInstantTime
          + " to run compaction");
    }
  }

  /**
   * Rollback partial compactions
   * @param inflightInstant Inflight Compaction Instant
   * @param timeline Active Timeline
   */
  private void rollbackInflightCompaction(HoodieInstant inflightInstant, HoodieActiveTimeline timeline) {
    rollbackHandler.rollback(ImmutableList.copyOf(new String[] { inflightInstant.getTimestamp() }));
    // Revert instant state file
    timeline.revertFromInflightToRequested(inflightInstant,
        HoodieTimeline.getCompactionRequestedInstant(inflightInstant.getTimestamp()), Optional.empty());
  }

  /**
   * Perform compaction operations as specified in the compaction commit file
   *
   * @param table              Hoodie Table
   * @param compactionInstant  Compacton Instant time
   * @param activeTimeline Active Timeline
   * @param autoCommit         Commit after compaction
   * @return RDD of Write Status
   */
  private JavaRDD<WriteStatus> runCompaction(HoodieTable<T> table,
      HoodieInstant compactionInstant, HoodieActiveTimeline activeTimeline, boolean autoCommit) throws IOException {
    HoodieCompactionWorkload workload = AvroUtils.deserializeHoodieCompactionWorkload(
        activeTimeline.getInstantDetails(compactionInstant).get());
    // Mark instant as compaction inflight
    activeTimeline.transitionFromRequestedToInflight(compactionInstant,
        HoodieTimeline.getInflightInstant(compactionInstant), Optional.empty());
    JavaRDD<WriteStatus> statuses = table.compact(jsc, compactionInstant.getTimestamp(), workload);
    commitCompaction(statuses, table, compactionInstant.getTimestamp(), autoCommit,
        Optional.ofNullable(workload.getExtraMetadata()));
    return statuses;
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
  protected void commitCompaction(JavaRDD<WriteStatus> compactedStatuses, HoodieTable<T> table,
      String compactionCommitTime, boolean autoCommit, Optional<Map<String, String>> extraMetadata) {
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
   * Commit compaction forcefully
   *
   * @param writeStatuses         Compaction Write statuses
   * @param table                 Hoodie Table
   * @param compactionInstantTime Compaction instant time
   */
  private HoodieCommitMetadata commitForceCompaction(JavaRDD<WriteStatus> writeStatuses,
      HoodieTable table, String compactionInstantTime, Optional<Map<String, String>> extraMetadata) {

    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
    List<Tuple2<String, HoodieWriteStat>> stats =
        CommitUtils.generateWriteStats(metadata, writeStatuses, extraMetadata);

    logger.info("Compaction finished for instant " + compactionInstantTime + " with result " + metadata);
    HoodieActiveTimeline activeTimeline = table.getMetaClient().getActiveTimeline();
    try {
      activeTimeline.saveAsComplete(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, compactionInstantTime),
          Optional.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + table.getMetaClient().getBasePath() + " at time " + compactionInstantTime, e);
    }
    return metadata;
  }
}