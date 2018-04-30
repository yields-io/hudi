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

import com.uber.hoodie.config.HoodieCompactionConfig;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.spark.api.java.JavaRDD;

/**
 * Compaction Client that lets users to initiate and stop compaction
 */
public interface HoodieCompactionClient {


  /**
   * Initializes the client. Called before any other API. This is just listed
   * here to describe the interface. During implementation, this will not
   * be part of this interface but will be moved to a builder/factory logic
   * that instantiates the compaction Client
   * @param compactionConfig Config that sets up all static properties including
   *                         base-path for compaction client to use.
   */
  void initialize(HoodieCompactionConfig compactionConfig);

  /**
   * Request compaction at marked commit. In this mode,
   * caller controls the run-loop and triggers compaction
   * @param compactionCommitTime Marked commit for compaction
   * @param compactionRunConfig Config to determine number of executors to use for compaction
   * @return RDD describing compaction status
   * @throws IllegalArgumentException if requested commit is not the first unfinished compaction
   *                                  commit or if the commit is not a compaction-marked commit
   * @throws IllegalStateException If another compaction is running in the same JVM. Note that
   *                               Hoodie supports only one outstanding compaction for a dataset.
   *                               This exception only protects against compactions triggering from
   *                               the same client.
   */
  Future<JavaRDD<CompactionStatus>> compact(
      String compactionCommitTime,
      Object compactionRunConfig)
      throws IllegalArgumentException, IllegalStateException;

  /**
   * This API allows Hoodie to control the run-loop. This is a blocking API. A callback
   * must be passed which will be triggered after every compaction is done.
   *
   * @param compactionRunConfigProvider Allows clients to override configuration for next run of compaction
   *                                 (e.g: Allows clients to set parallelism for next compaction). By default,
   *                                 compaction client will try to acquire as much executors as possible to run all
   *                                 file level compactions in parallel but with an upper bound
   * @param  rateLimiter       Allows Clients to rate-limit the number of compactions run in a time-period
   *                           It provides a latch-like interface which compaction client will call to permit running
   *                           compaction for next round.
   * @param compactionNotifier Notifier for beginning and end of compaction round.
   */
  void runForever(
      Optional<Function<JavaRDD<CompactionStatus>, Object>> compactionRunConfigProvider,
      Optional<Object> rateLimiter,
      Optional<HoodieCompactionNotifier> compactionNotifier);


  /**
   * Gracefully shutdown Compaction client. Currently running compaction is allowed to finish
   * and shutdown
   */
  void shutdown();

  /**
   * Forcefully shutsdown compaction by killing currently running compaction job
   */
  void shutdownNow();


  /**
   * Get the commit time of earliest unfinished Compaction
   * @return
   */
  String getEarliestUnfinishedCompaction();

  /**
   * Get all the marked compactions in commit order
   * @return
   */
  List<String> getUnfinishedCompactions();

  /**
   * Compaction Lag Details. Allows clients to inspect the amount of work
   * that compaction needs to do before it can catchup with the latest commit
   * @return Compaction Lag Details
   */
  Object getCompactionLagDetails();
}
