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

package com.uber.hoodie.cli.utils;

import static com.uber.hoodie.common.table.HoodieTimeline.CLEAN_ACTION;
import static com.uber.hoodie.common.table.HoodieTimeline.COMMIT_ACTION;
import static com.uber.hoodie.common.table.HoodieTimeline.COMPACTION_ACTION;
import static com.uber.hoodie.common.table.HoodieTimeline.DELTA_COMMIT_ACTION;
import static com.uber.hoodie.common.table.HoodieTimeline.ROLLBACK_ACTION;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.hoodie.avro.model.HoodieCleanMetadata;
import com.uber.hoodie.avro.model.HoodieCleanPartitionMetadata;
import com.uber.hoodie.avro.model.HoodieRollbackMetadata;
import com.uber.hoodie.avro.model.HoodieRollbackPartitionMetadata;
import com.uber.hoodie.avro.model.HoodieWriteStat;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.FSUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.Path;

public class FileTimeline {

  private final HoodieTableMetaClient metaClient;
  private final HoodieTimeline timeline;
  private final String partition;
  private final Map<String, List<Triple<String, Boolean, FileTracker>>> fileIdToTimeline = new HashMap<>();

  public FileTimeline(HoodieTableMetaClient metaClient, HoodieTimeline timeline, String partition) {
    this.metaClient = metaClient;
    this.timeline = timeline;
    this.partition = partition;
  }

  private void collectCleanMetadata(HoodieInstant instant)
      throws IOException {
    HoodieCleanMetadata cleanMetadata =
        AvroUtils.deserializeHoodieCleanMetadata(timeline.getInstantDetails(instant).get());
    HoodieCleanPartitionMetadata partitionMetadata = cleanMetadata.getPartitionMetadata().get(partition);

    if (null != partitionMetadata) {
      Stream.concat(partitionMetadata.getFailedDeleteFiles().stream().map(r -> Pair.of(false, r)),
          partitionMetadata.getSuccessDeleteFiles().stream().map(r -> Pair.of(true, r)))
          .forEach(deletePathPair -> {
            FileTracker tracker = buildFileTracker(deletePathPair.getRight(), null);
            List<Triple<String, Boolean, FileTracker>> trackers = fileIdToTimeline.get(tracker.fileId);
            if (null == trackers) {
              trackers = new ArrayList<>();
              fileIdToTimeline.put(tracker.fileId, trackers);
            }
            trackers.add(Triple.of(CLEAN_ACTION, deletePathPair.getLeft(), tracker));
          });
    }
  }

  public static com.uber.hoodie.avro.model.HoodieCommitMetadata commitMetadataConverter(
      HoodieCommitMetadata hoodieCommitMetadata) {
    ObjectMapper mapper = new ObjectMapper();
    //Need this to ignore other public get() methods
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    com.uber.hoodie.avro.model.HoodieCommitMetadata avroMetaData = mapper
        .convertValue(hoodieCommitMetadata, com.uber.hoodie.avro.model.HoodieCommitMetadata.class);
    return avroMetaData;
  }

  private void collectCommitMetadata(HoodieInstant instant, String action)
      throws IOException {
    com.uber.hoodie.avro.model.HoodieCommitMetadata metadata = null;
    try {
      metadata = commitMetadataConverter(
          HoodieCommitMetadata.fromJsonString(new String(timeline.getInstantDetails(instant).get())));
    } catch (Exception ex) {
      metadata = AvroUtils.deserializeAvroMetadata(timeline.getInstantDetails(instant).get(),
          com.uber.hoodie.avro.model.HoodieCommitMetadata.class);
    }
    List<HoodieWriteStat> writeStats = metadata.getPartitionToWriteStats().get(partition);

    if (null != writeStats) {
      for (HoodieWriteStat wStat : writeStats) {
        FileTracker tracker = buildFileTracker(wStat.getPath(), wStat);
        List<Triple<String, Boolean, FileTracker>> trackers = fileIdToTimeline.get(tracker.fileId);
        if (null == trackers) {
          trackers = new ArrayList<>();
          fileIdToTimeline.put(tracker.fileId, trackers);
        }
        trackers.add(Triple.of(action, true, tracker));
      }
    }
  }

  private void collectRollbackMetadata(HoodieInstant instant)
      throws IOException {
    HoodieRollbackMetadata metadata = AvroUtils.deserializeAvroMetadata(
        timeline.getInstantDetails(instant).get(), HoodieRollbackMetadata.class);
    HoodieRollbackPartitionMetadata partitionMetadata = metadata.getPartitionMetadata().get(partition);

    if (null != partitionMetadata) {
      Stream.concat(partitionMetadata.getFailedDeleteFiles().stream().map(r -> Pair.of(false, r)),
          partitionMetadata.getSuccessDeleteFiles().stream().map(r -> Pair.of(true, r)))
          .forEach(deletePathPair -> {
            FileTracker tracker = buildFileTracker(deletePathPair.getRight(), null);
            List<Triple<String, Boolean, FileTracker>> trackers = fileIdToTimeline.get(tracker.fileId);
            if (null == trackers) {
              trackers = new ArrayList<>();
              fileIdToTimeline.put(tracker.fileId, trackers);
            }
            trackers.add(Triple.of(ROLLBACK_ACTION, deletePathPair.getLeft(), tracker));
          });
    }
  }

  private FileTracker buildFileTracker(String filePathStr, HoodieWriteStat writeStat) {
    Path filePath = new Path(filePathStr);
    String fileName = filePath.getName();
    if (FSUtils.isLogFile(filePath)) {
      final String fileId = FSUtils.getFileIdFromLogPath(filePath);
      final String baseInstant = FSUtils.getBaseCommitTimeFromLogPath(filePath);
      return new FileTracker(fileId, baseInstant, false, writeStat,
          Optional.of(FSUtils.getFileVersionFromLog(filePath)));
    } else {
      final String fileId = FSUtils.getFileId(fileName);
      final String baseInstant = FSUtils.getBaseInstantFromDataFile(fileName);
      return new FileTracker(fileId, baseInstant, true, writeStat, Optional.empty());
    }
  }

  public void collect() {
    timeline.getInstants().forEach(instant -> {
      try {
        switch (instant.getAction()) {
          case HoodieTimeline.CLEAN_ACTION: {
            collectCleanMetadata(instant);
            break;
          }
          case COMMIT_ACTION: {
            collectCommitMetadata(instant, COMMIT_ACTION);
            break;
          }
          case HoodieTimeline.ROLLBACK_ACTION: {
            collectRollbackMetadata(instant);
            break;
          }
          case HoodieTimeline.SAVEPOINT_ACTION: {
            break;
          }
          case HoodieTimeline.DELTA_COMMIT_ACTION: {
            collectCommitMetadata(instant, DELTA_COMMIT_ACTION);
            break;
          }
          case COMPACTION_ACTION: {
            break;
          }
          default:
            throw new UnsupportedOperationException("Action (" + instant.getAction() + ") not fully supported yet");
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    });
  }

  public Map<String, List<Triple<String, Boolean, FileTracker>>> getFileIdToTimeline() {
    return fileIdToTimeline;
  }

  public static class FileTracker {

    public final String fileId;
    public final String baseInstant;
    public final boolean isBaseFile;
    public final HoodieWriteStat writeStat;
    public final Optional<Integer> logVersion;

    public FileTracker(String fileId, String baseInstant, boolean isBaseFile, HoodieWriteStat writeStat,
        Optional<Integer> logVersion) {
      this.fileId = fileId;
      this.baseInstant = baseInstant;
      this.isBaseFile = isBaseFile;
      this.writeStat = writeStat;
      this.logVersion = logVersion;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FileTracker)) {
        return false;
      }
      FileTracker that = (FileTracker) o;
      return isBaseFile == that.isBaseFile
          && Objects.equals(fileId, that.fileId)
          && Objects.equals(baseInstant, that.baseInstant)
          && Objects.equals(logVersion, that.logVersion);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fileId, baseInstant, isBaseFile, logVersion);
    }
  }
}