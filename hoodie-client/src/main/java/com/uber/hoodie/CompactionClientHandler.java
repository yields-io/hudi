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
import java.util.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Non-Public API : Compaction Handler taking care of both scheduling and running compactions
 */
class CompactionClientHandler<T extends HoodieRecordPayload> implements Serializable {

  private static Logger logger = LogManager.getLogger(CompactionClientHandler.class);

  private final transient FileSystem fs;
  private final transient JavaSparkContext jsc;
  private final HoodieWriteConfig config;
  private final transient HoodieMetrics metrics;

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
   */
  /**
   * Performs a compaction operation on a dataset. WARNING: Compaction operation cannot be executed
   * asynchronously. Please always use this serially before or after an insert/upsert action.
   *
   * @param table      Hoodie Table
   * @param commitTime Compacton Instant time
   * @return WriteStatus RDD of compaction
   */
  protected JavaRDD<WriteStatus> compact(HoodieTable<T> table, String commitTime) throws IOException {
    // TODO : Fix table.getActionType for MOR table type to return different actions based on delta or compaction
    JavaRDD<WriteStatus> statuses = table.compact(jsc, commitTime);
    // Trigger the insert and collect statuses
    return statuses.persist(config.getWriteStatusStorageLevel());
  }

  /**
   * Performs a compaction operation on a dataset. WARNING: Compaction operation cannot be executed
   * asynchronously. Please always use this serially before or after an insert/upsert action.
   *
   * @param compactionCommitTime Compaction Commit Time
   */
  private void forceCompact(String compactionCommitTime) throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config);
    // TODO : Fix table.getActionType for MOR table type to return different actions based on delta or compaction and
    // then use getTableAndInitCtx
    Timer.Context writeTimerContext = metrics.getCommitCtx();
    JavaRDD<WriteStatus> compactedStatuses = table.compact(jsc, compactionCommitTime);
    if (!compactedStatuses.isEmpty()) {
      HoodieCommitMetadata metadata = commitForceCompaction(compactedStatuses, metaClient, compactionCommitTime);
      long durationInMs = metrics.getDurationInMs(writeTimerContext.stop());
      try {
        metrics
            .updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(compactionCommitTime).getTime(),
                durationInMs, metadata, HoodieActiveTimeline.COMMIT_ACTION);
      } catch (ParseException e) {
        throw new HoodieCommitException(
            "Commit time is not of valid format.Failed to commit " + config.getBasePath()
                + " at time " + compactionCommitTime, e);
      }
      logger.info("Compacted successfully on commit " + compactionCommitTime);
    } else {
      writeTimerContext.close();
      logger.info("Compaction did not run for commit " + compactionCommitTime);
    }
  }

  /**
   * Performs a compaction operation on a dataset. WARNING: Compaction operation cannot be executed
   * asynchronously. Please always use this serially before or after an insert/upsert action.
   */
  protected String forceCompact() throws IOException {
    String compactionCommitTime = startCompaction();
    forceCompact(compactionCommitTime);
    return compactionCommitTime;
  }

  private HoodieCommitMetadata commitForceCompaction(JavaRDD<WriteStatus> writeStatuses,
      HoodieTableMetaClient metaClient, String compactionCommitTime) {
    List<HoodieWriteStat> updateStatusMap = writeStatuses.map(writeStatus -> writeStatus.getStat())
        .collect();

    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }

    logger.info("Compaction finished with result " + metadata);

    logger.info("Committing Compaction " + compactionCommitTime);
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    try {
      activeTimeline.saveAsComplete(
          new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, compactionCommitTime),
          Optional.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + metaClient.getBasePath() + " at time " + compactionCommitTime, e);
    }
    return metadata;
  }
}
