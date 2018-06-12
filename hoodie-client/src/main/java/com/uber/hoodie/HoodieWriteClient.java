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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.uber.hoodie.avro.model.HoodieCleanMetadata;
import com.uber.hoodie.avro.model.HoodieCompactionWorkload;
import com.uber.hoodie.avro.model.HoodieRollbackMetadata;
import com.uber.hoodie.avro.model.HoodieSavepointMetadata;
import com.uber.hoodie.common.HoodieCleanStat;
import com.uber.hoodie.common.HoodieRollbackStat;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.CompactionUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.exception.HoodieCompactionException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieInsertException;
import com.uber.hoodie.exception.HoodieRollbackException;
import com.uber.hoodie.exception.HoodieSavepointException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.func.BulkInsertMapFunction;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.io.HoodieCommitArchiveLog;
import com.uber.hoodie.io.compact.HoodieCompactionInstantWithWorkload;
import com.uber.hoodie.metrics.HoodieMetrics;
import com.uber.hoodie.table.HoodieTable;
import com.uber.hoodie.table.UserDefinedBulkInsertPartitioner;
import com.uber.hoodie.table.WorkloadProfile;
import com.uber.hoodie.table.WorkloadStat;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Option;
import scala.Tuple2;

/**
 * Hoodie Write Client helps you build datasets on HDFS [insert()] and then perform efficient
 * mutations on a HDFS dataset [upsert()]
 * <p>
 * Note that, at any given time, there can only be one Spark job performing these operatons on a
 * Hoodie dataset.
 */
public class HoodieWriteClient<T extends HoodieRecordPayload> implements Serializable {

  private static Logger logger = LogManager.getLogger(HoodieWriteClient.class);
  private final transient FileSystem fs;
  private final transient JavaSparkContext jsc;
  private final HoodieWriteConfig config;
  private final boolean rollbackInFlight;
  private final transient HoodieMetrics metrics;
  private final transient HoodieIndex<T> index;
  private transient Timer.Context writeContext = null;
  private transient Timer.Context compactionTimer;

  /**
   * @param jsc
   * @param clientConfig
   * @throws Exception
   */
  public HoodieWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig) throws Exception {
    this(jsc, clientConfig, false);
  }

  /**
   * @param jsc
   * @param clientConfig
   * @param rollbackInFlight
   */
  public HoodieWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig,
      boolean rollbackInFlight) {
    this(jsc, clientConfig, rollbackInFlight, HoodieIndex.createIndex(clientConfig, jsc));
  }

  @VisibleForTesting
  HoodieWriteClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig,
      boolean rollbackInFlight, HoodieIndex index) {
    this.fs = FSUtils.getFs(clientConfig.getBasePath(), jsc.hadoopConfiguration());
    this.jsc = jsc;
    this.config = clientConfig;
    this.index = index;
    this.metrics = new HoodieMetrics(config, config.getTableName());
    this.rollbackInFlight = rollbackInFlight;
  }

  public static SparkConf registerClasses(SparkConf conf) {
    conf.registerKryoClasses(
        new Class[] {HoodieWriteConfig.class, HoodieRecord.class, HoodieKey.class});
    return conf;
  }

  /**
   * Filter out HoodieRecords that already exists in the output folder. This is useful in
   * deduplication.
   *
   * @param hoodieRecords Input RDD of Hoodie records.
   * @return A subset of hoodieRecords RDD, with existing records filtered out.
   */
  public JavaRDD<HoodieRecord<T>> filterExists(JavaRDD<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);

    JavaRDD<HoodieRecord<T>> recordsWithLocation = index.tagLocation(hoodieRecords, table);
    return recordsWithLocation.filter(v1 -> !v1.isCurrentLocationKnown());
  }

  /**
   * Upserts a bunch of new records into the Hoodie table, at the supplied commitTime
   */
  public JavaRDD<WriteStatus> upsert(JavaRDD<HoodieRecord<T>> records, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx();
    try {
      // De-dupe/merge if needed
      JavaRDD<HoodieRecord<T>> dedupedRecords = combineOnCondition(
          config.shouldCombineBeforeUpsert(), records, config.getUpsertShuffleParallelism());

      // perform index loop up to get existing location of records
      JavaRDD<HoodieRecord<T>> taggedRecords = index.tagLocation(dedupedRecords, table);
      return upsertRecordsInternal(taggedRecords, commitTime, table, true);
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + commitTime, e);
    }
  }

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied commitTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if
   * needed.
   *
   * @param preppedRecords Prepared HoodieRecords to upsert
   * @param commitTime     Commit Time handle
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> upsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords,
      final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx();
    try {
      return upsertRecordsInternal(preppedRecords, commitTime, table, true);
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException(
          "Failed to upsert prepared records for commit time " + commitTime, e);
    }
  }

  /**
   * Inserts the given HoodieRecords, into the table. This API is intended to be used for normal
   * writes.
   * <p>
   * This implementation skips the index check and is able to leverage benefits such as small file
   * handling/blocking alignment, as with upsert(), by profiling the workload
   *
   * @param records    HoodieRecords to insert
   * @param commitTime Commit Time handle
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> insert(JavaRDD<HoodieRecord<T>> records, final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx();
    try {
      // De-dupe/merge if needed
      JavaRDD<HoodieRecord<T>> dedupedRecords = combineOnCondition(
          config.shouldCombineBeforeInsert(), records, config.getInsertShuffleParallelism());

      return upsertRecordsInternal(dedupedRecords, commitTime, table, false);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to insert for commit time " + commitTime, e);
    }
  }

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied commitTime.
   * <p>
   * This implementation skips the index check, skips de-duping and is able to leverage benefits
   * such as small file handling/blocking alignment, as with insert(), by profiling the workload.
   * The prepared HoodieRecords should be de-duped if needed.
   *
   * @param preppedRecords HoodieRecords to insert
   * @param commitTime     Commit Time handle
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> insertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords,
      final String commitTime) {
    HoodieTable<T> table = getTableAndInitCtx();
    try {
      return upsertRecordsInternal(preppedRecords, commitTime, table, false);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException(
          "Failed to insert prepared records for commit time " + commitTime, e);
    }
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk
   * loads into a Hoodie table for the very first time (e.g: converting an existing dataset to
   * Hoodie).
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and
   * attempts to control the numbers of files with less memory compared to the {@link
   * HoodieWriteClient#insert(JavaRDD, String)}
   *
   * @param records    HoodieRecords to insert
   * @param commitTime Commit Time handle
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records,
      final String commitTime) {
    return bulkInsert(records, commitTime, Option.empty());
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk
   * loads into a Hoodie table for the very first time (e.g: converting an existing dataset to
   * Hoodie).
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and
   * attempts to control the numbers of files with less memory compared to the {@link
   * HoodieWriteClient#insert(JavaRDD, String)}. Optionally it allows users to specify their own
   * partitioner. If specified then it will be used for repartitioning records. See {@link
   * UserDefinedBulkInsertPartitioner}.
   *
   * @param records               HoodieRecords to insert
   * @param commitTime            Commit Time handle
   * @param bulkInsertPartitioner If specified then it will be used to partition input records
   *                              before they are inserted into hoodie.
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, final String commitTime,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieTable<T> table = getTableAndInitCtx();
    try {
      // De-dupe/merge if needed
      JavaRDD<HoodieRecord<T>> dedupedRecords = combineOnCondition(
          config.shouldCombineBeforeInsert(), records, config.getInsertShuffleParallelism());

      return bulkInsertInternal(dedupedRecords, commitTime, table, bulkInsertPartitioner);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to bulk insert for commit time " + commitTime, e);
    }
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk
   * loads into a Hoodie table for the very first time (e.g: converting an existing dataset to
   * Hoodie).  The input records should contain no duplicates if needed.
   * <p>
   * This implementation uses sortBy (which does range partitioning based on reservoir sampling) and
   * attempts to control the numbers of files with less memory compared to the {@link
   * HoodieWriteClient#insert(JavaRDD, String)}. Optionally it allows users to specify their own
   * partitioner. If specified then it will be used for repartitioning records. See {@link
   * UserDefinedBulkInsertPartitioner}.
   *
   * @param preppedRecords        HoodieRecords to insert
   * @param commitTime            Commit Time handle
   * @param bulkInsertPartitioner If specified then it will be used to partition input records
   *                              before they are inserted into hoodie.
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> bulkInsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords,
      final String commitTime, Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieTable<T> table = getTableAndInitCtx();
    try {
      return bulkInsertInternal(preppedRecords, commitTime, table, bulkInsertPartitioner);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException(
          "Failed to bulk insert prepared records for commit time " + commitTime, e);
    }
  }

  private JavaRDD<WriteStatus> bulkInsertInternal(JavaRDD<HoodieRecord<T>> dedupedRecords,
      String commitTime, HoodieTable<T> table,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    final JavaRDD<HoodieRecord<T>> repartitionedRecords;
    if (bulkInsertPartitioner.isDefined()) {
      repartitionedRecords = bulkInsertPartitioner.get()
          .repartitionRecords(dedupedRecords, config.getBulkInsertShuffleParallelism());
    } else {
      // Now, sort the records and line them up nicely for loading.
      repartitionedRecords = dedupedRecords.sortBy(record -> {
        // Let's use "partitionPath + key" as the sort key. Spark, will ensure
        // the records split evenly across RDD partitions, such that small partitions fit
        // into 1 RDD partition, while big ones spread evenly across multiple RDD partitions
        return String.format("%s+%s", record.getPartitionPath(), record.getRecordKey());
      }, true, config.getBulkInsertShuffleParallelism());
    }
    JavaRDD<WriteStatus> writeStatusRDD = repartitionedRecords
        .mapPartitionsWithIndex(new BulkInsertMapFunction<T>(commitTime, config, table), true)
        .flatMap(writeStatuses -> writeStatuses.iterator());

    return updateIndexAndCommitIfNeeded(writeStatusRDD, table, commitTime);
  }

  private void commitOnAutoCommit(String commitTime, JavaRDD<WriteStatus> resultRDD,
      String actionType) {
    if (config.shouldAutoCommit()) {
      logger.info("Auto commit enabled: Committing " + commitTime);
      boolean commitResult = commit(commitTime, resultRDD, Optional.empty(), actionType);
      if (!commitResult) {
        throw new HoodieCommitException("Failed to commit " + commitTime);
      }
    } else {
      logger.info("Auto commit disabled for " + commitTime);
    }
  }

  private JavaRDD<HoodieRecord<T>> combineOnCondition(boolean condition,
      JavaRDD<HoodieRecord<T>> records, int parallelism) {
    if (condition) {
      return deduplicateRecords(records, parallelism);
    }
    return records;
  }

  /**
   * Save the workload profile in an intermediate file (here re-using commit files) This is useful
   * when performing rollback for MOR datasets. Only updates are recorded in the workload profile
   * metadata since updates to log blocks are unknown across batches Inserts (which are new parquet
   * files) are rolled back based on commit time. // TODO : Create a new WorkloadProfile metadata
   * file instead of using HoodieCommitMetadata
   */
  private void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, HoodieTable<T> table,
      String commitTime) throws HoodieCommitException {
    try {
      HoodieCommitMetadata metadata = new HoodieCommitMetadata();
      profile.getPartitionPaths().stream().forEach(path -> {
        WorkloadStat partitionStat = profile.getWorkloadStat(path.toString());
        partitionStat.getUpdateLocationToCount().entrySet().stream().forEach(entry -> {
          HoodieWriteStat writeStat = new HoodieWriteStat();
          writeStat.setFileId(entry.getKey());
          writeStat.setPrevCommit(entry.getValue().getKey());
          writeStat.setNumUpdateWrites(entry.getValue().getValue());
          metadata.addWriteStat(path.toString(), writeStat);
        });
      });

      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      Optional<HoodieInstant> instant = activeTimeline.filterInflightsExcludingCompaction().lastInstant();
      activeTimeline.saveToInflight(instant.get(),
          Optional.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException io) {
      throw new HoodieCommitException(
          "Failed to commit " + commitTime + " unable to save inflight metadata ", io);
    }
  }

  private JavaRDD<WriteStatus> upsertRecordsInternal(JavaRDD<HoodieRecord<T>> preppedRecords,
      String commitTime, HoodieTable<T> hoodieTable, final boolean isUpsert) {

    // Cache the tagged records, so we don't end up computing both
    // TODO: Consistent contract in HoodieWriteClient regarding preppedRecord storage level handling
    if (preppedRecords.getStorageLevel() == StorageLevel.NONE()) {
      preppedRecords.persist(StorageLevel.MEMORY_AND_DISK_SER());
    } else {
      logger.info("RDD PreppedRecords was persisted at: " + preppedRecords.getStorageLevel());
    }

    WorkloadProfile profile = null;
    if (hoodieTable.isWorkloadProfileNeeded()) {
      profile = new WorkloadProfile(preppedRecords);
      logger.info("Workload profile :" + profile);
      saveWorkloadProfileMetadataToInflight(profile, hoodieTable, commitTime);
    }

    // partition using the insert partitioner
    final Partitioner partitioner = getPartitioner(hoodieTable, isUpsert, profile);
    JavaRDD<HoodieRecord<T>> partitionedRecords = partition(preppedRecords, partitioner);
    JavaRDD<WriteStatus> writeStatusRDD = partitionedRecords
        .mapPartitionsWithIndex((partition, recordItr) -> {
          if (isUpsert) {
            return hoodieTable.handleUpsertPartition(commitTime, partition, recordItr, partitioner);
          } else {
            return hoodieTable.handleInsertPartition(commitTime, partition, recordItr, partitioner);
          }
        }, true).flatMap(writeStatuses -> writeStatuses.iterator());

    return updateIndexAndCommitIfNeeded(writeStatusRDD, hoodieTable, commitTime);
  }

  private Partitioner getPartitioner(HoodieTable table, boolean isUpsert, WorkloadProfile profile) {
    if (isUpsert) {
      return table.getUpsertPartitioner(profile);
    } else {
      return table.getInsertPartitioner(profile);
    }
  }

  private JavaRDD<WriteStatus> updateIndexAndCommitIfNeeded(JavaRDD<WriteStatus> writeStatusRDD,
      HoodieTable<T> table, String commitTime) {
    // Update the index back
    JavaRDD<WriteStatus> statuses = index.updateLocation(writeStatusRDD, table);
    // Trigger the insert and collect statuses
    statuses = statuses.persist(config.getWriteStatusStorageLevel());
    commitOnAutoCommit(commitTime, statuses, table.getCommitActionType());
    return statuses;
  }

  private JavaRDD<HoodieRecord<T>> partition(JavaRDD<HoodieRecord<T>> dedupedRecords,
      Partitioner partitioner) {
    return dedupedRecords.mapToPair(record -> new Tuple2<>(
        new Tuple2<>(record.getKey(), Option.apply(record.getCurrentLocation())), record))
        .partitionBy(partitioner).map(tuple -> tuple._2());
  }

  /**
   * Commit changes performed at the given commitTime marker
   */
  public boolean commit(String commitTime, JavaRDD<WriteStatus> writeStatuses) {
    return commit(commitTime, writeStatuses, Optional.empty());
  }

  /**
   * Commit changes performed at the given commitTime marker
   */
  public boolean commit(String commitTime, JavaRDD<WriteStatus> writeStatuses,
      Optional<Map<String, String>> extraMetadata) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);
    return commit(commitTime, writeStatuses, extraMetadata, table.getCommitActionType());
  }

  private boolean commit(String commitTime, JavaRDD<WriteStatus> writeStatuses,
      Optional<Map<String, String>> extraMetadata, String actionType) {

    logger.info("Commiting " + commitTime);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

    List<Tuple2<String, HoodieWriteStat>> stats = writeStatuses.mapToPair(
        (PairFunction<WriteStatus, String, HoodieWriteStat>) writeStatus -> new Tuple2<>(
            writeStatus.getPartitionPath(), writeStatus.getStat())).collect();

    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    for (Tuple2<String, HoodieWriteStat> stat : stats) {
      metadata.addWriteStat(stat._1(), stat._2());
    }

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

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach((k, v) -> metadata.addMetadata(k, v));
    }

    try {
      activeTimeline.saveAsComplete(new HoodieInstant(true, actionType, commitTime),
          Optional.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      // Save was a success
      // Do a inline compaction if enabled
      if (config.isInlineCompaction()) {
        metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "true");
        forceCompact(extraMetadata);
      } else {
        metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "false");
      }

      // We cannot have unbounded commit files. Archive commits if we have to archive
      HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(config,
          new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true));
      archiveLog.archiveIfRequired();
      if (config.isAutoClean()) {
        // Call clean to cleanup if there is anything to cleanup after the commit,
        logger.info("Auto cleaning is enabled. Running cleaner now");
        clean(commitTime);
      } else {
        logger.info("Auto cleaning is not enabled. Not running cleaner now");
      }
      if (writeContext != null) {
        long durationInMs = metrics.getDurationInMs(writeContext.stop());
        metrics
            .updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(commitTime).getTime(),
                durationInMs, metadata, actionType);
        writeContext = null;
      }
      logger.info("Committed " + commitTime);
    } catch (IOException e) {
      throw new HoodieCommitException(
          "Failed to commit " + config.getBasePath() + " at time " + commitTime, e);
    } catch (ParseException e) {
      throw new HoodieCommitException(
          "Commit time is not of valid format.Failed to commit " + config.getBasePath()
              + " at time " + commitTime, e);
    }
    return true;
  }

  /**
   * Savepoint a specific commit. Latest version of data files as of the passed in commitTime will
   * be referenced in the savepoint and will never be cleaned. The savepointed commit will never be
   * rolledback or archived.
   * <p>
   * This gives an option to rollback the state to the savepoint anytime. Savepoint needs to be
   * manually created and deleted.
   * <p>
   * Savepoint should be on a commit that could not have been cleaned.
   *
   * @param user    - User creating the savepoint
   * @param comment - Comment for the savepoint
   * @return true if the savepoint was created successfully
   */
  public boolean savepoint(String user, String comment) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);
    if (table.getCompletedCommitTimeline().empty()) {
      throw new HoodieSavepointException("Could not savepoint. Commit timeline is empty");
    }

    String latestCommit = table.getCompletedCommitTimeline().lastInstant().get().getTimestamp();
    logger.info("Savepointing latest commit " + latestCommit);
    return savepoint(latestCommit, user, comment);
  }

  /**
   * Savepoint a specific commit. Latest version of data files as of the passed in commitTime will
   * be referenced in the savepoint and will never be cleaned. The savepointed commit will never be
   * rolledback or archived.
   * <p>
   * This gives an option to rollback the state to the savepoint anytime. Savepoint needs to be
   * manually created and deleted.
   * <p>
   * Savepoint should be on a commit that could not have been cleaned.
   *
   * @param commitTime - commit that should be savepointed
   * @param user       - User creating the savepoint
   * @param comment    - Comment for the savepoint
   * @return true if the savepoint was created successfully
   */
  public boolean savepoint(String commitTime, String user, String comment) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);
    Optional<HoodieInstant> cleanInstant = table.getCompletedCleanTimeline().lastInstant();

    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION,
        commitTime);
    if (!table.getCompletedCommitTimeline().containsInstant(commitInstant)) {
      throw new HoodieSavepointException(
          "Could not savepoint non-existing commit " + commitInstant);
    }

    try {
      // Check the last commit that was not cleaned and check if savepoint time is > that commit
      String lastCommitRetained;
      if (cleanInstant.isPresent()) {
        HoodieCleanMetadata cleanMetadata = AvroUtils.deserializeHoodieCleanMetadata(
            table.getActiveTimeline().getInstantDetails(cleanInstant.get()).get());
        lastCommitRetained = cleanMetadata.getEarliestCommitToRetain();
      } else {
        lastCommitRetained = table.getCompletedCommitTimeline().firstInstant().get().getTimestamp();
      }

      // Cannot allow savepoint time on a commit that could have been cleaned
      Preconditions.checkArgument(HoodieTimeline
              .compareTimestamps(commitTime, lastCommitRetained, HoodieTimeline.GREATER_OR_EQUAL),
          "Could not savepoint commit " + commitTime + " as this is beyond the lookup window "
              + lastCommitRetained);

      Map<String, List<String>> latestFilesMap = jsc.parallelize(FSUtils
          .getAllPartitionPaths(fs, table.getMetaClient().getBasePath(),
              config.shouldAssumeDatePartitioning()))
          .mapToPair((PairFunction<String, String, List<String>>) partitionPath -> {
            // Scan all partitions files with this commit time
            logger.info("Collecting latest files in partition path " + partitionPath);
            TableFileSystemView.ReadOptimizedView view = table.getROFileSystemView();
            List<String> latestFiles = view.getLatestDataFilesBeforeOrOn(partitionPath, commitTime)
                .map(HoodieDataFile::getFileName).collect(Collectors.toList());
            return new Tuple2<>(partitionPath, latestFiles);
          }).collectAsMap();

      HoodieSavepointMetadata metadata = AvroUtils
          .convertSavepointMetadata(user, comment, latestFilesMap);
      // Nothing to save in the savepoint
      table.getActiveTimeline()
          .saveAsComplete(new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, commitTime),
              AvroUtils.serializeSavepointMetadata(metadata));
      logger.info("Savepoint " + commitTime + " created");
      return true;
    } catch (IOException e) {
      throw new HoodieSavepointException("Failed to savepoint " + commitTime, e);
    }
  }

  /**
   * Delete a savepoint that was created. Once the savepoint is deleted, the commit can be
   * rolledback and cleaner may clean up data files.
   *
   * @param savepointTime - delete the savepoint
   * @return true if the savepoint was deleted successfully
   */
  public void deleteSavepoint(String savepointTime) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

    HoodieInstant savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION,
        savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      logger.warn("No savepoint present " + savepointTime);
      return;
    }

    activeTimeline.revertToInflight(savePoint);
    activeTimeline
        .deleteInflight(new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, savepointTime));
    logger.info("Savepoint " + savepointTime + " deleted");
  }

  /**
   * Delete a compaction request that is pending.
   *
   * NOTE - This is an Admin operation.
   * With async compaction, this is expected to be called with async compaction and ingestion shutdown.
   * Otherwise, async compactor could fail with errors
   *
   * @param compactionTime - delete the compaction time
   * @return
   */
  public void deletePendingCompaction(String compactionTime) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

    HoodieInstant compactionRequestedInstant =
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionTime);
    HoodieInstant compactionInflightInstant =
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionTime);
    boolean isCompactionInstantInRequestedState = table.getActiveTimeline().filterPendingCompactionTimeline()
        .containsInstant(compactionRequestedInstant);
    boolean isCompactionInstantInInflightState = table.getActiveTimeline().filterPendingCompactionTimeline()
        .containsInstant(compactionInflightInstant);

    if (isCompactionInstantInRequestedState) {
      activeTimeline.deleteCompactionRequested(compactionRequestedInstant);
    } else if (isCompactionInstantInInflightState) {
      activeTimeline.revertCompactionInflightToRequested(compactionInflightInstant);
      activeTimeline.deleteCompactionRequested(compactionRequestedInstant);
    } else {
      logger.error("No compaction present " + compactionTime);
      throw new IllegalArgumentException("No compaction present " + compactionTime);
    }
    logger.info("Compaction " + compactionTime + " deleted");
  }

  /**
   * Rollback the state to the savepoint. WARNING: This rollsback recent commits and deleted data
   * files. Queries accessing the files will mostly fail. This should be done during a downtime.
   *
   * @param savepointTime - savepoint time to rollback to
   * @return true if the savepoint was rollecback to successfully
   */
  public boolean rollbackToSavepoint(String savepointTime) {
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    // Rollback to savepoint is expected to be a manual operation and no concurrent ingestion or compaction is expected
    // to be running. Rollback to savepoint also removes any pending compaction actions that are generated after
    // savepoint time. Allowing pending compaction to be retained is not safe as those workload could be referencing
    // file-slices that will be rolled-back as part of this operation
    HoodieTimeline commitTimeline = table.getCommitsAndCompactionTimeline();

    HoodieInstant savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION,
        savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      throw new HoodieRollbackException("No savepoint for commitTime " + savepointTime);
    }

    List<String> commitsToRollback = commitTimeline
        .findInstantsAfter(savepointTime, Integer.MAX_VALUE).getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    logger.info("Rolling back commits " + commitsToRollback);

    rollback(commitsToRollback);

    // Make sure the rollback was successful
    Optional<HoodieInstant> lastInstant = activeTimeline.reload().getCommitsAndCompactionTimeline()
        .filterCompletedAndCompactionInstants().lastInstant();
    Preconditions.checkArgument(lastInstant.isPresent());
    Preconditions.checkArgument(lastInstant.get().getTimestamp().equals(savepointTime),
        savepointTime + "is not the last commit after rolling back " + commitsToRollback
            + ", last commit was " + lastInstant.get().getTimestamp());
    return true;
  }

  /**
   * Rollback the (inflight/committed) record changes with the given commit time. Three steps: (1)
   * Atomically unpublish this commit (2) clean indexing data, (3) clean new generated parquet
   * files. (4) Finally delete .commit or .inflight file,
   */
  public boolean rollback(final String commitTime) throws HoodieRollbackException {
    rollback(Lists.newArrayList(commitTime));
    return true;
  }

  private void rollback(List<String> commits) {
    if (commits.isEmpty()) {
      logger.info("List of commits to rollback is empty");
      return;
    }

    final Timer.Context context = metrics.getRollbackCtx();
    String startRollbackTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());

    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);
    Set<String> pendingCompactions =
        table.getActiveTimeline().filterPendingCompactionTimeline().getInstants()
            .map(HoodieInstant::getTimestamp).collect(Collectors.toSet());
    HoodieTimeline inflightCommitTimeline = table.getInflightCommitTimeline();
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

    List<String> pendingCompactionToRollback =
        commits.stream().filter(c -> pendingCompactions.contains(c)).collect(Collectors.toList());
    List<String> commitsToRollback =
        commits.stream().filter(c -> !pendingCompactions.contains(c)).collect(Collectors.toList());

    try {
      if (commitTimeline.empty() && inflightCommitTimeline.empty()) {
        // nothing to rollback
        logger.info("No commits to rollback " + commitsToRollback);
      }

      // Make sure only the last n commits are being rolled back
      // If there is a commit in-between or after that is not rolled back, then abort
      String lastCommit = null;
      if (!commitsToRollback.isEmpty()) {
        lastCommit = commitsToRollback.get(commitsToRollback.size() - 1);
      }

      if ((lastCommit != null) && !commitTimeline.empty() && !commitTimeline
          .findInstantsAfter(lastCommit, Integer.MAX_VALUE).empty()) {
        throw new HoodieRollbackException(
            "Found commits after time :" + lastCommit + ", please rollback greater commits first");
      }

      List<String> inflights = inflightCommitTimeline.getInstants().map(HoodieInstant::getTimestamp)
          .collect(Collectors.toList());
      if ((lastCommit != null) && !inflights.isEmpty() && (inflights.indexOf(lastCommit) != inflights.size() - 1)) {
        throw new HoodieRollbackException("Found in-flight commits after time :" + lastCommit
            + ", please rollback greater commits first");
      }

      // Remove interleaving pending compactions before rolling back commits
      pendingCompactionToRollback.stream().forEach(this::deletePendingCompaction);

      List<HoodieRollbackStat> stats = table.rollback(jsc, commitsToRollback);

      // cleanup index entries
      commitsToRollback.stream().forEach(s -> {
        if (!index.rollbackCommit(s)) {
          throw new HoodieRollbackException("Rollback index changes failed, for time :" + s);
        }
      });
      logger.info("Index rolled back for commits " + commitsToRollback);

      Optional<Long> durationInMs = Optional.empty();
      if (context != null) {
        durationInMs = Optional.of(metrics.getDurationInMs(context.stop()));
        Long numFilesDeleted = stats.stream().mapToLong(stat -> stat.getSuccessDeleteFiles().size())
            .sum();
        metrics.updateRollbackMetrics(durationInMs.get(), numFilesDeleted);
      }
      HoodieRollbackMetadata rollbackMetadata = AvroUtils
          .convertRollbackMetadata(startRollbackTime, durationInMs, commitsToRollback, stats);
      table.getActiveTimeline().saveAsComplete(
          new HoodieInstant(true, HoodieTimeline.ROLLBACK_ACTION, startRollbackTime),
          AvroUtils.serializeRollbackMetadata(rollbackMetadata));
      logger.info("Commits " + commitsToRollback + " rollback is complete");

      if (!table.getActiveTimeline().getCleanerTimeline().empty()) {
        logger.info("Cleaning up older rollback meta files");
        // Cleanup of older cleaner meta files
        // TODO - make the commit archival generic and archive rollback metadata
        FSUtils.deleteOlderRollbackMetaFiles(fs, table.getMetaClient().getMetaPath(),
            table.getActiveTimeline().getRollbackTimeline().getInstants());
      }
    } catch (IOException e) {
      throw new HoodieRollbackException(
          "Failed to rollback " + config.getBasePath() + " commits " + commitsToRollback, e);
    }
  }

  /**
   * Releases any resources used by the client.
   */
  public void close() {
    // UNDER CONSTRUCTION
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based
   * on the configurations and CleaningPolicy used. (typically files that no longer can be used by a
   * running query can be cleaned)
   */
  public void clean() throws HoodieIOException {
    String startCleanTime = HoodieActiveTimeline.createNewCommitTime();
    clean(startCleanTime);
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based
   * on the configurations and CleaningPolicy used. (typically files that no longer can be used by a
   * running query can be cleaned)
   */
  private void clean(String startCleanTime) throws HoodieIOException {
    try {
      logger.info("Cleaner started");
      final Timer.Context context = metrics.getCleanCtx();

      // Create a Hoodie table which encapsulated the commits and files visible
      HoodieTable<T> table = HoodieTable.getHoodieTable(
          new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);

      List<HoodieCleanStat> cleanStats = table.clean(jsc);
      if (cleanStats.isEmpty()) {
        return;
      }

      // Emit metrics (duration, numFilesDeleted) if needed
      Optional<Long> durationInMs = Optional.empty();
      if (context != null) {
        durationInMs = Optional.of(metrics.getDurationInMs(context.stop()));
        logger.info("cleanerElaspsedTime (Minutes): " + durationInMs.get() / (1000 * 60));
      }

      // Create the metadata and save it
      HoodieCleanMetadata metadata = AvroUtils
          .convertCleanMetadata(startCleanTime, durationInMs, cleanStats);
      logger.info("Cleaned " + metadata.getTotalFilesDeleted() + " files");
      metrics
          .updateCleanMetrics(durationInMs.orElseGet(() -> -1L), metadata.getTotalFilesDeleted());

      table.getActiveTimeline()
          .saveAsComplete(new HoodieInstant(true, HoodieTimeline.CLEAN_ACTION, startCleanTime),
              AvroUtils.serializeCleanMetadata(metadata));
      logger.info("Marked clean started on " + startCleanTime + " as complete");

      if (!table.getActiveTimeline().getCleanerTimeline().empty()) {
        // Cleanup of older cleaner meta files
        // TODO - make the commit archival generic and archive clean metadata
        FSUtils.deleteOlderCleanMetaFiles(fs, table.getMetaClient().getMetaPath(),
            table.getActiveTimeline().getCleanerTimeline().getInstants());
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to clean up after commit", e);
    }
  }

  /**
   * Provides a new commit time for a write operation (insert/update)
   */
  public String startCommit() {
    String commitTime = HoodieActiveTimeline.createNewCommitTime();
    startCommitWithTime(commitTime);
    return commitTime;
  }

  public void startCommitWithTime(String instantTime) {
    if (rollbackInFlight) {
      // Only rollback inflight commit/delta-commits. Do not touch compaction commits
      rollbackInflightCommits();
    }
    logger.info("Generate a new commit time " + instantTime);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath());
    // if there are pending compactions, their instantTime must not be greater than that of this instant time
    metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().ifPresent(latestPending -> {
      Preconditions.checkArgument(
          HoodieTimeline.compareTimestamps(latestPending.getTimestamp(), instantTime, HoodieTimeline.LESSER),
          "Latest pending compaction instant time must be earlier "
              + "than this instant time. Latest Compaction :" + latestPending + ",  Ingesting at " + instantTime);
    });
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    String commitActionType = table.getCommitActionType();
    activeTimeline.createInflight(new HoodieInstant(true, commitActionType, instantTime));
  }


  /**
   * Schedules a new compaction instant
   * @param extraMetadata
   * @return
   */
  public String scheduleCompaction(Optional<Map<String, String>> extraMetadata) throws IOException {
    String instantTime = HoodieActiveTimeline.createNewCommitTime();
    logger.info("Generate a new instant time " + instantTime);
    scheduleCompactionWithTime(instantTime, extraMetadata);
    return instantTime;
  }

  /**
   * Schedules a new compaction instant with passed-in instant time
   * @param instantTime     Compaction Instant Time
   * @param extraMetadata   Extra Metadata to be stored
   */
  public void scheduleCompactionWithTime(String instantTime, Optional<Map<String, String>> extraMetadata)
      throws IOException {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    // if there are inflight ingestions, their instantTime must not be less than that of compaction instant time
    metaClient.getActiveTimeline().filterInflightsExcludingCompaction().firstInstant().ifPresent(earliestInflight -> {
      Preconditions.checkArgument(
          HoodieTimeline.compareTimestamps(earliestInflight.getTimestamp(), instantTime, HoodieTimeline.GREATER),
          "Earliest ingestion inflight instant time must be later "
              + "than compaction time. Earliest :" + earliestInflight + ", Compaction scheduled at " + instantTime);
    });
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config);
    HoodieCompactionWorkload workload = table.scheduleCompaction(jsc, instantTime);
    extraMetadata.ifPresent(workload::setExtraMetadata);
    HoodieInstant compactionInstant =
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
        AvroUtils.serializeCompactionWorkload(workload));
  }

  /**
   * Return all pending compactions with instant time for clients to decide what to compact next.
   * @return
   */
  public List<HoodieCompactionInstantWithWorkload> getPendingCompactions() {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    return CompactionUtils.getAllPendingCompactionWorkloads(metaClient).stream()
        .map(instantWorkloadPair ->
            new HoodieCompactionInstantWithWorkload(instantWorkloadPair.getKey().getTimestamp(),
                instantWorkloadPair.getValue()))
        .collect(Collectors.toList());
  }

  /**
   * Performs Compaction for the workload stored in instant-time
   * @param compactionInstantTime   Compaction Instant Time
   * @return
   * @throws IOException
   */
  public JavaRDD<WriteStatus> compact(String compactionInstantTime) throws IOException {
    return compact(compactionInstantTime, config.shouldAutoCommit());
  }

  /**
   * Commit a compaction operation. Allow passing additional meta-data to be stored in commit instant file.
   */
  public void commitCompaction(String compactionInstantTime, JavaRDD<WriteStatus> writeStatuses,
      Optional<Map<String, String>> extraMetadata) throws IOException {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieCompactionWorkload workload = AvroUtils.deserializeCompactionWorkload(
        timeline.getInstantAuxiliaryDetails(HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get());
    // Merge extra meta-data passed by user with the one already in inflight compaction
    Optional<Map<String, String>> mergedMetaData = extraMetadata.map(m -> {
      Map<String, String> merged = new HashMap<>();
      Map<String, String> extraMetaDataFromInstantFile = workload.getExtraMetadata();
      if (extraMetaDataFromInstantFile != null) {
        merged.putAll(extraMetaDataFromInstantFile);
      }
      // Overwrite/Merge with the user-passed meta-data
      merged.putAll(m);
      return Optional.of(merged);
    }).orElseGet(() -> Optional.ofNullable(workload.getExtraMetadata()));
    commitCompaction(writeStatuses, table, compactionInstantTime, true, mergedMetaData);
  }

  /**
   * Deduplicate Hoodie records, using the given deduplication funciton.
   */
  JavaRDD<HoodieRecord<T>> deduplicateRecords(JavaRDD<HoodieRecord<T>> records,
      int parallelism) {
    boolean isIndexingGlobal = index.isGlobal();
    return records
        .mapToPair(record -> {
          HoodieKey hoodieKey = record.getKey();
          // If index used is global, then records are expected to differ in their partitionPath
          Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
          return new Tuple2<>(key, record);
        })
        .reduceByKey((rec1, rec2) -> {
          @SuppressWarnings("unchecked") T reducedData = (T) rec1.getData()
              .preCombine(rec2.getData());
          // we cannot allow the user to change the key or partitionPath, since that will affect
          // everything
          // so pick it from one of the records.
          return new HoodieRecord<T>(rec1.getKey(), reducedData);
        }, parallelism).map(recordTuple -> recordTuple._2());
  }

  /**
   * Cleanup all inflight commits
   */
  private void rollbackInflightCommits() {
    HoodieTable<T> table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);
    HoodieTimeline inflightTimeline = table.getCommitsTimeline().filterInflightsExcludingCompaction();
    List<String> commits = inflightTimeline.getInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    Collections.reverse(commits);
    for (String commit : commits) {
      rollback(commit);
    }
  }

  private HoodieTable getTableAndInitCtx() {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable table = HoodieTable.getHoodieTable(
        new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true), config);
    if (table.getCommitActionType() == HoodieTimeline.COMMIT_ACTION) {
      writeContext = metrics.getCommitCtx();
    } else {
      writeContext = metrics.getDeltaCommitCtx();
    }
    return table;
  }

  /**
   * Compaction specific private methods
   */

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time
   * @param compactionInstantTime   Compaction Instant Time
   * @return
   * @throws IOException
   */
  private JavaRDD<WriteStatus> compact(String compactionInstantTime, boolean autoCommit) throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config);
    HoodieTimeline pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      //inflight compaction - Needs to rollback first deleting new parquet files before we run compaction.
      rollbackInflightCompaction(inflightInstant, table);
      // refresh table
      metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true);
      table = HoodieTable.getHoodieTable(metaClient, config);
      pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    }

    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(instant)) {
      return runCompaction(instant, metaClient.getActiveTimeline(), autoCommit);
    } else {
      throw new IllegalStateException("No Compaction request available at " + compactionInstantTime
          + " to run compaction");
    }
  }

  /**
   * Perform compaction operations as specified in the compaction commit file
   *
   * @param compactionInstant  Compacton Instant time
   * @param activeTimeline Active Timeline
   * @param autoCommit         Commit after compaction
   * @return RDD of Write Status
   */
  private JavaRDD<WriteStatus> runCompaction(
      HoodieInstant compactionInstant, HoodieActiveTimeline activeTimeline, boolean autoCommit) throws IOException {
    HoodieCompactionWorkload workload = AvroUtils.deserializeCompactionWorkload(
        activeTimeline.getInstantAuxiliaryDetails(compactionInstant).get());
    // Mark instant as compaction inflight
    activeTimeline.transitionCompactionRequestedToInflight(compactionInstant);
    compactionTimer = metrics.getCompactionCtx();
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config);
    JavaRDD<WriteStatus> statuses = table.compact(jsc, compactionInstant.getTimestamp(), workload);
    // Force compaction action
    statuses.persist(config.getWriteStatusStorageLevel());
    // pass extra-metada so that it gets stored in commit file automatically
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
          commitForceCompaction(compactedStatuses, table.getMetaClient(), compactionCommitTime, extraMetadata);
      if (compactionTimer != null) {
        long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
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
  }

  /**
   * Rollback partial compactions
   * @param inflightInstant Inflight Compaction Instant
   * @param table Hoodie Table
   */
  private void rollbackInflightCompaction(HoodieInstant inflightInstant, HoodieTable table) throws IOException {
    table.rollback(jsc, ImmutableList.copyOf(new String[] { inflightInstant.getTimestamp() }));
    // Revert instant state file
    table.getActiveTimeline().revertCompactionInflightToRequested(inflightInstant);
  }

  private HoodieCommitMetadata commitForceCompaction(JavaRDD<WriteStatus> writeStatuses,
      HoodieTableMetaClient metaClient, String compactionCommitTime, Optional<Map<String, String>> extraMetadata) {
    List<HoodieWriteStat> updateStatusMap = writeStatuses.map(writeStatus -> writeStatus.getStat())
        .collect();

    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }

    // Copy extraMetadata
    extraMetadata.ifPresent(m -> {
      m.entrySet().stream().forEach(e -> {
        metadata.addMetadata(e.getKey(), e.getValue());
      });
    });

    logger.info("Compaction finished with result " + metadata);

    logger.info("Committing Compaction " + compactionCommitTime);
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    try {
      activeTimeline.transitionCompactionInflightToComplete(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionCommitTime),
          Optional.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + metaClient.getBasePath() + " at time " + compactionCommitTime, e);
    }
    return metadata;
  }

  /**
   * Performs a compaction operation on a dataset. WARNING: Compaction operation cannot be executed
   * asynchronously. Please always use this serially before or after an insert/upsert action.
   */
  private String forceCompact(Optional<Map<String, String>> extraMetadata) throws IOException {
    String compactionCommitTime = scheduleCompaction(extraMetadata);
    compact(compactionCommitTime);
    return compactionCommitTime;
  }
}
