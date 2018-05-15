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
import com.uber.hoodie.metrics.HoodieMetrics;
import com.uber.hoodie.table.HoodieTable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Utils class providing common functions used in commit
 */
class CommitUtils {

  private static Logger logger = LogManager.getLogger(CommitUtils.class);

  /**
   * Generate partition level write stats
   *
   * @param metadata      Metadata to write
   * @param writeStatuses Write Statuses
   * @param extraMetadata Additional Metadata to write
   * @return Partition Level stats
   */
  protected static List<Tuple2<String, HoodieWriteStat>> generateWriteStats(HoodieCommitMetadata metadata,
      JavaRDD<WriteStatus> writeStatuses,
      Optional<Map<String, String>> extraMetadata) {
    List<Tuple2<String, HoodieWriteStat>> stats = writeStatuses.mapToPair(
        (PairFunction<WriteStatus, String, HoodieWriteStat>) writeStatus -> new Tuple2<>(
            writeStatus.getPartitionPath(), writeStatus.getStat())).collect();
    for (Tuple2<String, HoodieWriteStat> stat : stats) {
      metadata.addWriteStat(stat._1(), stat._2());
    }

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach((k, v) -> metadata.addMetadata(k, v));
    }
    return stats;
  }

  /**
   * Finalize Write on table and update metrics
   *
   * @param jsc     Spark Context
   * @param table   Hoodie Table
   * @param metrics Metrics
   * @param stats   Partition Level stats
   * @param <T>     Record Payload Type
   */
  protected static <T extends HoodieRecordPayload> void finalizeWrite(JavaSparkContext jsc,
      HoodieTable<T> table, HoodieMetrics metrics, List<Tuple2<String, HoodieWriteStat>> stats) {
    // Finalize write
    final Timer.Context finalizeCtx = metrics.getFinalizeCtx();
    final Optional<Integer> result = table.finalizeWrite(jsc, stats);
    if (finalizeCtx != null && result.isPresent()) {
      Optional<Long> durationInMs = Optional.of(metrics.getDurationInMs(finalizeCtx.stop()));
      durationInMs.ifPresent(duration -> {
        logger.info("Finalize write elapsed time (milliseconds): " + duration);
        metrics.updateFinalizeWriteMetrics(duration, result.get());
      });
    }
  }
}
