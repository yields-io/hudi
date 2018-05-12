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
import com.uber.hoodie.avro.model.HoodieRollbackMetadata;
import com.uber.hoodie.common.HoodieRollbackStat;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieRollbackException;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.metrics.HoodieMetrics;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Non-Public class performing Rollback of instants. Used by write and compaction use-cases
 */
class RollbackHandler<T extends HoodieRecordPayload> implements Serializable {

  private static Logger logger = LogManager.getLogger(RollbackHandler.class);

  private final transient FileSystem fs;
  private final transient JavaSparkContext jsc;
  private final HoodieWriteConfig config;
  private final transient HoodieIndex<T> index;
  private final transient HoodieMetrics metrics;

  protected RollbackHandler(JavaSparkContext jsc, HoodieIndex<T> index, HoodieWriteConfig clientConfig,
      HoodieMetrics metrics) {
    this.fs = FSUtils.getFs(clientConfig.getBasePath(), jsc.hadoopConfiguration());
    this.jsc = jsc;
    this.index = index;
    this.config = clientConfig;
    this.metrics = metrics;
  }

  /**
   * Roll back commits
   * @param commits
   */
  public void rollback(List<String> commits) {
    if (commits.isEmpty()) {
      logger.info("List of commits to rollback is empty");
      return;
    }

    final Timer.Context context = metrics.getRollbackCtx();
    String startRollbackTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());

    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieTimeline inflightTimeline = table.getInflightCommitTimeline();
    HoodieTimeline commitTimeline = table.getCompletedCommitTimeline();

    // Check if any of the commits is a savepoint - do not allow rollback on those commits
    List<String> savepoints = table.getCompletedSavepointTimeline().getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    commits.forEach(s -> {
      if (savepoints.contains(s)) {
        throw new HoodieRollbackException(
            "Could not rollback a savepointed commit. Delete savepoint first before rolling back"
                + s);
      }
    });

    try {
      if (commitTimeline.empty() && inflightTimeline.empty()) {
        // nothing to rollback
        logger.info("No commits to rollback " + commits);
      }

      // Make sure only the last n commits are being rolled back
      // If there is a commit in-between or after that is not rolled back, then abort
      String lastCommit = commits.get(commits.size() - 1);
      if (!commitTimeline.empty() && !commitTimeline
          .findInstantsAfter(lastCommit, Integer.MAX_VALUE).empty()) {
        throw new HoodieRollbackException(
            "Found commits after time :" + lastCommit + ", please rollback greater commits first");
      }

      List<String> inflights = inflightTimeline.getInstants().map(HoodieInstant::getTimestamp)
          .collect(Collectors.toList());
      if (!inflights.isEmpty() && inflights.indexOf(lastCommit) != inflights.size() - 1) {
        throw new HoodieRollbackException("Found in-flight commits after time :" + lastCommit
            + ", please rollback greater commits first");
      }

      List<HoodieRollbackStat> stats = table.rollback(jsc, commits);

      // cleanup index entries
      commits.stream().forEach(s -> {
        if (!index.rollbackCommit(s)) {
          throw new HoodieRollbackException("Rollback index changes failed, for time :" + s);
        }
      });
      logger.info("Index rolled back for commits " + commits);

      Optional<Long> durationInMs = Optional.empty();
      if (context != null) {
        durationInMs = Optional.of(metrics.getDurationInMs(context.stop()));
        Long numFilesDeleted = stats.stream().mapToLong(stat -> stat.getSuccessDeleteFiles().size())
            .sum();
        metrics.updateRollbackMetrics(durationInMs.get(), numFilesDeleted);
      }
      HoodieRollbackMetadata rollbackMetadata = AvroUtils
          .convertRollbackMetadata(startRollbackTime, durationInMs, commits, stats);
      table.getActiveTimeline().saveAsComplete(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, startRollbackTime),
          AvroUtils.serializeRollbackMetadata(rollbackMetadata));
      logger.info("Commits " + commits + " rollback is complete");

      if (!table.getActiveTimeline().getCleanerTimeline().empty()) {
        logger.info("Cleaning up older rollback meta files");
        // Cleanup of older cleaner meta files
        // TODO - make the commit archival generic and archive rollback metadata
        FSUtils.deleteOlderRollbackMetaFiles(fs, table.getMetaClient().getMetaPath(),
            table.getActiveTimeline().getRollbackTimeline().getInstants());
      }
    } catch (IOException e) {
      throw new HoodieRollbackException(
          "Failed to rollback " + config.getBasePath() + " commits " + commits, e);
    }
  }
}
