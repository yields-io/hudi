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

package com.uber.hoodie.cli.commands;

import static com.uber.hoodie.common.table.HoodieTimeline.CLEAN_ACTION;
import static com.uber.hoodie.common.table.HoodieTimeline.LESSER;
import static com.uber.hoodie.common.table.HoodieTimeline.ROLLBACK_ACTION;

import com.uber.hoodie.avro.model.HoodieWriteStat;
import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.cli.HoodiePrintHelper;
import com.uber.hoodie.cli.TableHeader;
import com.uber.hoodie.cli.commands.RollbacksCommand.RollbackTimeline;
import com.uber.hoodie.cli.utils.FileTimeline;
import com.uber.hoodie.cli.utils.FileTimeline.FileTracker;
import com.uber.hoodie.cli.utils.HoodieArchivedTimeline;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieDefaultTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class FileTimelineCommand implements CommandMarker {

  private static final transient Logger log = LogManager.getLogger(FileTimelineCommand.class);

  @CliCommand(value = "files timeline", help = "Mapped timeline for files in a partition")
  public String timeline(
      @CliOption(key = {"partition"}, mandatory = true, help = "Partition To Check")
          String partitionPath,
      @CliOption(key = {"beginInstant"}, unspecifiedDefaultValue = "0", help = "Begin Instant")
          String beginInstant,
      @CliOption(key = {"endInstant"}, unspecifiedDefaultValue = "0", help = "End Instant")
          String endInstant,
      @CliOption(key = {"includeStats"}, help = "Ordering", unspecifiedDefaultValue = "false") boolean includeStats,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false")
          boolean headerOnly) throws IOException {
    HoodieTimeline timeline = buildCompositeTimeline(beginInstant, endInstant);
    FileTimeline fileTimeline = new FileTimeline(HoodieCLI.tableMetadata, timeline, partitionPath);
    fileTimeline.collect();
    Map<String, List<Triple<String, Boolean, FileTracker>>> result = fileTimeline.getFileIdToTimeline();

    ArrayList<Comparable[]> rows = new ArrayList<>();
    result.entrySet().forEach(entry -> {
      entry.getValue().forEach(e2 -> {
        int idx = 0;
        Comparable[] row = new Comparable[ includeStats ? 16 : 7];
        row[idx++] = partitionPath;
        row[idx++] = entry.getKey();
        row[idx++] = e2.getLeft();
        row[idx++] = e2.getMiddle() ? "SUCCESS" : "FAIL";
        row[idx++] = e2.getRight().baseInstant;
        row[idx++] = e2.getRight().isBaseFile ? "PARQUET" : "LOG";
        row[idx++] = e2.getRight().logVersion.isPresent() ? e2.getRight().logVersion.get() : "";
        HoodieWriteStat writeStat = e2.getRight().writeStat;
        if (writeStat != null) {
          row[idx++] = writeStat.getPrevCommit();
          row[idx++] = writeStat.getNumWrites();
          row[idx++] = writeStat.getNumDeletes();
          row[idx++] = writeStat.getNumUpdateWrites();
          row[idx++] = writeStat.getTotalWriteBytes();
          row[idx++] = writeStat.getTotalWriteErrors();
          row[idx++] = writeStat.getTotalLogFiles();
          row[idx++] = writeStat.getTotalLogRecords();
          row[idx++] = writeStat.getTotalUpdatedRecordsCompacted();
        }
        rows.add(row);
      });
    });

    TableHeader header = new TableHeader()
        .addTableHeaderField("Partition")
        .addTableHeaderField("FileId")
        .addTableHeaderField("Action")
        .addTableHeaderField("Status")
        .addTableHeaderField("BaseInstant")
        .addTableHeaderField("Type")
        .addTableHeaderField("LogVersion");

    if (includeStats) {
      header = header.addTableHeaderField("prevCommit")
          .addTableHeaderField("Records Written")
          .addTableHeaderField("Records Deleted")
          .addTableHeaderField("numUpdateWrites")
          .addTableHeaderField("totalWriteBytes")
          .addTableHeaderField("totalWriteErrors")
          .addTableHeaderField("totalLogFiles")
          .addTableHeaderField("totalLogRecords")
          .addTableHeaderField("totalUpdatedRecordsCompacted");
    }
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  private static HoodieTimeline buildCompositeTimeline(String beginInstant, String endInstant) throws IOException {
    HoodieTableMetaClient metaClient = HoodieCLI.tableMetadata;
    HoodieTimeline timeline = new HoodieDefaultTimeline(Stream.empty(), null);
    Stream<HoodieInstant> archivedInstants = Stream.empty();
    Optional<HoodieInstant> activeCommitsBegin = metaClient.getActiveTimeline().getCommitsTimeline().firstInstant();
    Optional<HoodieInstant> activeCleansBegin = metaClient.getActiveTimeline().getCleanerTimeline().firstInstant();

    if (!activeCommitsBegin.isPresent() || HoodieTimeline.compareTimestamps(activeCommitsBegin.get().getTimestamp(),
        beginInstant, HoodieTimeline.GREATER)) {
      timeline = new HoodieArchivedTimeline(metaClient);
      archivedInstants = timeline.getInstants()
          .filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), beginInstant,
              HoodieTimeline.GREATER_OR_EQUAL))
          .filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), endInstant,
              HoodieTimeline.LESSER_OR_EQUAL));
    }
    final HoodieTimeline archivedTimeline = timeline;

    Stream<HoodieInstant> activeInstants = metaClient.getActiveTimeline().getInstants()
        .filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), beginInstant,
            HoodieTimeline.GREATER_OR_EQUAL))
        .filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), endInstant,
            HoodieTimeline.LESSER_OR_EQUAL));

    final RollbackTimeline rollbackTimeline = new RollbackTimeline(metaClient);
    Stream<HoodieInstant> rollbackInstants = rollbackTimeline.getInstants()
        .filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), beginInstant,
            HoodieTimeline.GREATER_OR_EQUAL))
        .filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), endInstant,
            HoodieTimeline.LESSER_OR_EQUAL));
    Stream<HoodieInstant> instants = Stream.concat(rollbackInstants,
        Stream.concat(activeInstants, archivedInstants));

    HoodieTimeline compositeTimeline = new HoodieDefaultTimeline(instants, instant -> {
      if (instant.getAction().equals(ROLLBACK_ACTION)) {
        return rollbackTimeline.getInstantDetails(instant);
      } else if (instant.getAction().equals(CLEAN_ACTION)) {
        if (!activeCleansBegin.isPresent() || HoodieTimeline.compareTimestamps(instant.getTimestamp(),
            activeCleansBegin.get().getTimestamp(), LESSER)) {
          return archivedTimeline.getInstantDetails(instant);
        } else {
          return  metaClient.getActiveTimeline().getInstantDetails(instant);
        }
      } else if (!activeCommitsBegin.isPresent() || HoodieTimeline.compareTimestamps(instant.getTimestamp(),
          activeCommitsBegin.get().getTimestamp(), LESSER)) {
        return archivedTimeline.getInstantDetails(instant);
      } else {
        return metaClient.getActiveTimeline().getInstantDetails(instant);
      }
    });
    return compositeTimeline;
  }
}
