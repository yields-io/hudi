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

import com.uber.hoodie.avro.model.HoodieCleanMetadata;
import com.uber.hoodie.avro.model.HoodieCleanPartitionMetadata;
import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.cli.HoodiePrintHelper;
import com.uber.hoodie.cli.TableBuffer;
import com.uber.hoodie.cli.TableFieldType;
import com.uber.hoodie.cli.TableHeader;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.AvroUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class CleansCommand implements CommandMarker {

  @CliAvailabilityIndicator({"cleans show"})
  public boolean isShowAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliAvailabilityIndicator({"cleans refresh"})
  public boolean isRefreshAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliAvailabilityIndicator({"clean showpartitions"})
  public boolean isCommitShowAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliCommand(value = "cleans show", help = "Show the cleans")
  public String showCleans(
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false")
      final boolean headerOnly) throws IOException {

    TableHeader header = new TableHeader()
        .addTableHeaderField("CleanTime", TableFieldType.NUMERIC)
        .addTableHeaderField("EarliestCommandRetained", TableFieldType.TEXT)
        .addTableHeaderField("Total Files Deleted", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Time Taken", TableFieldType.NUMERIC);

    if (headerOnly) {
      return HoodiePrintHelper.print(header);
    }

    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCleanerTimeline().filterCompletedInstants();
    List<HoodieInstant> cleans = timeline.getInstants().collect(Collectors.toList());
    List<String[]> rows = new ArrayList<>();
    Collections.reverse(cleans);
    for (int i = 0; i < cleans.size(); i++) {
      HoodieInstant clean = cleans.get(i);
      HoodieCleanMetadata cleanMetadata = AvroUtils
          .deserializeHoodieCleanMetadata(timeline.getInstantDetails(clean).get());
      rows.add(new String[]{clean.getTimestamp(), cleanMetadata.getEarliestCommitToRetain(),
          String.valueOf(cleanMetadata.getTotalFilesDeleted()), String.valueOf(cleanMetadata.getTimeTakenInMillis())});
    }

    TableBuffer buffer = new TableBuffer(header, new HashMap<>(),
        Optional.ofNullable(sortByField.isEmpty() ? null : sortByField),
        Optional.of(descending),
        Optional.ofNullable(limit <= 0 ? null : limit)).addAllRows(rows).flip();
    return HoodiePrintHelper.print(buffer);
  }

  @CliCommand(value = "cleans refresh", help = "Refresh the commits")
  public String refreshCleans() throws IOException {
    HoodieTableMetaClient metadata = new HoodieTableMetaClient(HoodieCLI.conf, HoodieCLI.tableMetadata.getBasePath());
    HoodieCLI.setTableMetadata(metadata);
    return "Metadata for table " + metadata.getTableConfig().getTableName() + " refreshed.";
  }

  @CliCommand(value = "clean showpartitions", help = "Show partition level details of a clean")
  public String showCleanPartitions(
      @CliOption(key = {"clean"}, help = "clean to show") final String commitTime,
      @CliOption(key = {"limit"}, help = "Limit commits", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false")
      final boolean headerOnly) throws Exception {
    TableHeader header = new TableHeader()
        .addTableHeaderField("Partition Path", TableFieldType.TEXT)
        .addTableHeaderField("Cleaning policy", TableFieldType.TEXT)
        .addTableHeaderField("Total Files Successfully Deleted", TableFieldType.NUMERIC)
        .addTableHeaderField("Total Failed Deletions", TableFieldType.NUMERIC);
    if (headerOnly) {
      return HoodiePrintHelper.print(header);

    }
    HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCleanerTimeline().filterCompletedInstants();
    HoodieInstant cleanInstant = new HoodieInstant(false, HoodieTimeline.CLEAN_ACTION, commitTime);

    if (!timeline.containsInstant(cleanInstant)) {
      return "Clean " + commitTime + " not found in metadata " + timeline;
    }
    HoodieCleanMetadata cleanMetadata = AvroUtils.deserializeHoodieCleanMetadata(
        timeline.getInstantDetails(cleanInstant).get());
    List<String[]> rows = new ArrayList<>();
    for (Map.Entry<String, HoodieCleanPartitionMetadata> entry : cleanMetadata.getPartitionMetadata().entrySet()) {
      String path = entry.getKey();
      HoodieCleanPartitionMetadata stats = entry.getValue();
      String policy = stats.getPolicy();
      String totalSuccessDeletedFiles = String.valueOf(stats.getSuccessDeleteFiles().size());
      String totalFailedDeletedFiles = String.valueOf(stats.getFailedDeleteFiles().size());
      rows.add(new String[]{path, policy, totalSuccessDeletedFiles, totalFailedDeletedFiles});
    }

    TableBuffer buffer = new TableBuffer(header, new HashMap<>(),
        Optional.ofNullable(sortByField.isEmpty() ? null : sortByField),
        Optional.of(descending),
        Optional.ofNullable(limit <= 0 ? null : limit)).addAllRows(rows).flip();
    return HoodiePrintHelper.print(buffer);
  }
}
