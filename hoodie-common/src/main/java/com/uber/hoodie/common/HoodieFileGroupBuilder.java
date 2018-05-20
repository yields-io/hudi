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

package com.uber.hoodie.common;

import com.google.common.base.Preconditions;
import com.uber.hoodie.avro.model.HoodieCompactionWorkload;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.exception.HoodieException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javafx.util.Pair;
import org.apache.commons.lang3.builder.Builder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * FileGroupBuilder for Hoodie File Group.
 * Handles cases when delta-commits happened after compaction requests.
 * Ensures the latest file-slice for RT view contains all the log-files including
 * the ones after pending compaction requests.
 *
 * File-Group/File-Slice Constraints maintained by this class:
 * ===========================================================
 * 1. A file-group is identified by an unique file-Id within a partition.
 * 2. A file-group has one or more file-slices.
 * 3. Exactly one file-slice for a file-group provides the latest snapshot view (A view which is capable of providing
 *    all the latest records in that file-group).
 * 4. A file-group can have at-most one pending compaction.
 * 5. The latest file-slice in a file-group will include all of
 *    (a) the data-file corresponding to the last committed instant before a compaction request instant (if present)
 *    (b) the set of log-files due to delta-instants after the instant (a) but before compaction request instant
 *    (c) the set of log-files written after compaction request instant (if present)
 *
 * FileGroupBuilder uses compaction workload meta-data to figure out which file-slices are due to outstanding
 * compaction and makes adjustment accordingly to satisfy above constraints
 */
public class HoodieFileGroupBuilder implements Builder<HoodieFileGroup> {

  private static final transient Logger log = LogManager.getLogger(HoodieFileGroupBuilder.class);

  /**
   * Slices of files in this group, sorted with greater commit first.
   */
  private final NavigableMap<String, FileSlice> fileSlices = new TreeMap<>(HoodieFileGroup.getCommitTimeComparator());
  /**
   * Partition containing the file group.
   */
  private final String partitionPath;
  /**
   * uniquely identifies the file group
   */
  private final String id;
  /**
   * Timeline, based on which all getter work
   */
  private final HoodieTimeline timeline;

  /**
   * Timeline containing pending (requested + inflight) compactions
   */
  private final Set<String> pendingCompactionInstantTimes;

  /**
   * For file-Ids that are marked for compaction, track their compaction time so
   * that it can be set in file-slices
   */
  private final Map<String, String> fileIdToPendingCompactionInstantTimes;

  public HoodieFileGroupBuilder(String partitionPath, String id, HoodieTableMetaClient metaClient,
      HoodieTimeline timeline) {
    this.partitionPath = partitionPath;
    this.id = id;
    this.timeline = timeline;
    List<HoodieInstant> pendingCompactionInstants =
        metaClient.getActiveTimeline().filterPendingCompactionTimeline().getInstants().collect(Collectors.toList());
    this.pendingCompactionInstantTimes = pendingCompactionInstants.stream()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toSet());
    this.fileIdToPendingCompactionInstantTimes = new HashMap<>();
    // Pending Compaction Instants
    pendingCompactionInstants.stream().flatMap(instant -> {
      try {
        HoodieCompactionWorkload workload =
            AvroUtils.deserializeHoodieCompactionWorkload(timeline.getInstantDetails(instant).get());
        return workload.getOperations().stream().map(op -> {
          return new Pair<String, String>(op.getFileId(), instant.getTimestamp());
        });
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    }).forEach(pair -> {
      // Defensive check to ensure a single-fileId does not have more than one pending compaction
      if (fileIdToPendingCompactionInstantTimes.containsKey(pair.getKey())) {
        String msg = "Hoodie File Id (" + pair.getKey() + ") has more thant 1 pending compactions. Instants: "
            + pair.getValue() + ", " + fileIdToPendingCompactionInstantTimes.get(pair.getKey());
        log.error(msg);
        throw new IllegalStateException(msg);
      }
      fileIdToPendingCompactionInstantTimes.put(pair.getKey(), pair.getValue());
    });
  }

  /**
   * Add a new datafile into the file group
   */
  public HoodieFileGroupBuilder withDataFile(HoodieDataFile dataFile) {
    if (!fileSlices.containsKey(dataFile.getCommitTime())) {
      fileSlices.put(dataFile.getCommitTime(), new FileSlice(dataFile.getCommitTime(), id));
    }
    fileSlices.get(dataFile.getCommitTime()).setDataFile(dataFile);
    return this;
  }

  /**
   * Add a new log file into the group
   */
  public HoodieFileGroupBuilder withLogFile(HoodieLogFile logFile) {
    // We allow log-files whose base-commit is that of incomplete compaction instant (requested/inflight)
    // to be added to the file-group. It is possible that the log-file was generated because of an inflight
    // delta-instant. Even in this case, the log file will be added to the file-groups.
    // The log-blocks(log-files) belonging to inflight delta-instants are filtered-out during record-reading.
    if (!fileSlices.containsKey(logFile.getBaseCommitTime())) {
      fileSlices.put(logFile.getBaseCommitTime(), new FileSlice(logFile.getBaseCommitTime(), id));
    }
    fileSlices.get(logFile.getBaseCommitTime()).addLogFile(logFile);
    return this;
  }

  /**
   * Helper to transform fake file-slices due to pending compaction.
   * If there were file-slices whose base-commit is from pending compaction, un-set any data-files and
   * set outstandingCompactionInstant
   * @return
   */
  private NavigableMap<String, FileSlice> transformFileSlicesForPendingCompactionInstants() {
    NavigableMap<String, FileSlice> newFileSlices = new TreeMap<>(HoodieFileGroup.getCommitTimeComparator());
    fileSlices.entrySet().stream().map(entry -> {
      // this is a pending compaction, ensure outstandingCompactionInstant is set.
      if (fileIdToPendingCompactionInstantTimes.containsKey(entry.getValue().getFileId())) {
        FileSlice fakeFileSlice = new FileSlice(entry.getValue().getBaseCommitTime(), entry.getValue().getFileId());
        fakeFileSlice.setOutstandingCompactionInstant(
            fileIdToPendingCompactionInstantTimes.get(entry.getValue().getFileId()));
        entry.getValue().getLogFiles().forEach(fakeFileSlice::addLogFile);
        if (!pendingCompactionInstantTimes.contains(entry.getValue().getBaseCommitTime())) {
          // Data File is not from inflight compaction. So add the data-file if present
          if (entry.getValue().getDataFile().isPresent()) {
            fakeFileSlice.setDataFile(entry.getValue().getDataFile().get());
          }
        }
        return new Pair<>(entry.getKey(), fakeFileSlice);
      }
      return new Pair<>(entry.getKey(), entry.getValue());
    }).forEach(p -> {
      newFileSlices.put(p.getKey(), p.getValue());
    });
    return newFileSlices;
  }

  @Override
  public HoodieFileGroup build() {
    Preconditions.checkNotNull(id, "File Id must not be null");
    Preconditions.checkNotNull(timeline, "Timeline must not be null");

    // We can have at-most last 2 file-slices without base file. This is possible if a compaction request
    // has been set for a file-group first time. In this case, the file group may not even have a data file
    // (in the case of log files supporting inserts).
    Preconditions.checkArgument(fileSlices.values().stream().filter(f -> !f.getDataFile().isPresent()).count() <= 2);

    NavigableMap<String, FileSlice> newFileSlices = transformFileSlicesForPendingCompactionInstants();
    Map<String, FileSlice> mergedFileSlices = newFileSlices;
    if (!newFileSlices.isEmpty()) {
      FileSlice lastSlice = newFileSlices.lastEntry().getValue();
      // When file-group has more than one file-slice and the last file-slice does not have data-file, then
      // it is the pending compaction case
      if (!lastSlice.getDataFile().isPresent() && (fileSlices.size() > 1)) {
        // last file slice is due to compaction (fake)
        mergedFileSlices = mergeFileGroupSlices(newFileSlices, lastSlice);
      }
    }
    return new HoodieFileGroup(partitionPath, id, timeline, mergedFileSlices);
  }

  /**
   * Helper to merge fake file-slice due to pending compaction with latest file-slice and provide the correct
   * latest view of the file-group
   *
   * @param rawFileSlices File Slices including fake file slices
   * @param fakeFileSlice Fake file slice
   */
  private static Map<String, FileSlice> mergeFileGroupSlices(NavigableMap<String, FileSlice> rawFileSlices,
      FileSlice fakeFileSlice) {
    final Map<String, FileSlice> mergedFileSlices = new TreeMap<>(HoodieFileGroup.getReverseCommitTimeComparator());
    // case where last file-slice is due to compaction request.
    // We need to merge the latest 2 file-slices
    Entry<String, FileSlice> lastEntry = rawFileSlices.lastEntry();
    Entry<String, FileSlice> penultimateEntry = rawFileSlices.lowerEntry(lastEntry.getKey());
    Preconditions.checkArgument(lastEntry.getValue() == fakeFileSlice,
        "Sanity check to ensure the last file-slice is the one not having the data-file. "
            + "Last Entry=" + lastEntry.getValue()
            + " Expected=" + fakeFileSlice);
    FileSlice merged = mergeCompactionPendingFileSlices(lastEntry.getValue(), penultimateEntry.getValue());
    // Create new file-slices for the file-group
    rawFileSlices.entrySet().stream().filter(
        fileSliceEntry -> {
          // All file-slices with base-commit less than that of penultimate entry
          return HoodieFileGroup.getCommitTimeComparator().compare(fileSliceEntry.getKey(),
              penultimateEntry.getKey()) < 0;
        })
        .forEach(fileSliceEntry -> mergedFileSlices.put(fileSliceEntry.getKey(), fileSliceEntry.getValue()));
    // Add last Entry to complete
    mergedFileSlices.put(merged.getBaseCommitTime(), merged);
    return mergedFileSlices;
  }

  /**
   * Helper to merge last 2 file-slices so that the last file-slice base-commit is considered outstanding compaction
   * instant. These 2 file-slices do not have compaction done yet.
   *
   * @param lastSlice        Latest File slice for a file-group
   * @param penultimateSlice Penultimate file slice for a file-group in commit timeline order
   */
  private static FileSlice mergeCompactionPendingFileSlices(FileSlice lastSlice, FileSlice penultimateSlice) {
    FileSlice merged = new FileSlice(penultimateSlice.getBaseCommitTime(), penultimateSlice.getFileId());
    merged.setOutstandingCompactionInstant(lastSlice.getBaseCommitTime());
    if (penultimateSlice.getDataFile().isPresent()) {
      merged.setDataFile(penultimateSlice.getDataFile().get());
    }
    // Add Log files from penultimate and last slices
    lastSlice.getLogFiles().forEach(lf -> merged.addLogFile(lf));
    penultimateSlice.getLogFiles().forEach(lf -> merged.addLogFile(lf));
    return merged;
  }
}
