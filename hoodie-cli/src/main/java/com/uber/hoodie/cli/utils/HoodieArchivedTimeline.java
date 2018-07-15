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

import com.uber.hoodie.avro.model.HoodieArchivedMetaEntry;
import com.uber.hoodie.avro.model.HoodieCleanMetadata;
import com.uber.hoodie.avro.model.HoodieCommitMetadata;
import com.uber.hoodie.avro.model.HoodieRollbackMetadata;
import com.uber.hoodie.avro.model.HoodieSavepointMetadata;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.timeline.HoodieDefaultTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.util.FSUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Represents the Archived Timeline for the HoodieDataset. Instants for the last 12 hours (configurable) is in the
 * ActiveTimeline and the rest are in ArchivedTimeline. <p></p> Instants are read from the archive file during
 * initialization and never refreshed. To refresh, clients need to call reload() <p></p> This class can be serialized
 * and de-serialized and on de-serialization the FileSystem is re-initialized.
 */
public class HoodieArchivedTimeline extends HoodieDefaultTimeline {

  private static final String HOODIE_COMMIT_ARCHIVE_LOG_FILE = ".commits";
  private static final transient Logger log = LogManager.getLogger(
      com.uber.hoodie.common.table.timeline.HoodieArchivedTimeline.class);
  private HoodieTableMetaClient metaClient;
  private Map<HoodieInstant, Optional<byte[]>> readCommits = new HashMap<>();

  public static final org.apache.avro.Schema COMMIT_SCHEMA$ =
      new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"HoodieCommitMetadata\",\"fields\":[{\"name\":\"partitionToWriteStats\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"HoodieWriteStat\",\"fields\":[{\"name\":\"fileId\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"path\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"prevCommit\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"numWrites\",\"type\":[\"null\",\"long\"]},{\"name\":\"numDeletes\",\"type\":[\"null\",\"long\"]},{\"name\":\"numUpdateWrites\",\"type\":[\"null\",\"long\"]},{\"name\":\"totalWriteBytes\",\"type\":[\"null\",\"long\"]},{\"name\":\"totalWriteErrors\",\"type\":[\"null\",\"long\"]},{\"name\":\"partitionPath\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"totalLogRecords\",\"type\":[\"null\",\"long\"]},{\"name\":\"totalLogFiles\",\"type\":[\"null\",\"long\"]},{\"name\":\"totalUpdatedRecordsCompacted\",\"type\":[\"null\",\"long\"],\"default\":null}]}},\"avro.java.string\":\"String\"}]},{\"name\":\"extraMetadata\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"avro.java.string\":\"String\"}]}]}");

  public HoodieArchivedTimeline(HoodieTableMetaClient metaClient) throws IOException {
    FileSystem fileSystem = FSUtils.getFs(metaClient.getBasePath(), metaClient.getHadoopConf());
    FileStatus[] fsStatuses = fileSystem.globStatus(new Path(metaClient.getMetaPath(), ".commits_.archive*"));
    for (FileStatus fs : fsStatuses) {
      //read the archived file
      HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(fileSystem,
          new HoodieLogFile(fs.getPath()), HoodieArchivedMetaEntry.getClassSchema());

      //read the avro blocks
      this.instants = new ArrayList<>();
      while (reader.hasNext()) {
        HoodieAvroDataBlock blk = (HoodieAvroDataBlock) reader.next();
        List<IndexedRecord> records = blk.getRecords();
        for (IndexedRecord rec : records) {
          HoodieInstant instant = getInstant((GenericRecord)rec);
          readCommits.put(instant, getContentBytes((GenericRecord)rec));
          instants.add(instant);
        }
      }
    }
  }

  private byte[] serialize(Schema schema, GenericRecord record) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, baos);
    dataFileWriter.append(record);
    dataFileWriter.close();
    return baos.toByteArray();
  }

  /**
   * For serialization and de-serialization only.
   *
   * @deprecated
   */
  public HoodieArchivedTimeline() {
  }

  public static Path getArchiveLogPath(String archiveFolder) {
    return new Path(archiveFolder, HOODIE_COMMIT_ARCHIVE_LOG_FILE);
  }

  private Optional<byte[]> getContentBytes(GenericRecord record) {
    try {
      switch (record.get("actionType").toString()) {
        case HoodieTimeline.CLEAN_ACTION: {
          return Optional.of(serialize(HoodieCleanMetadata.SCHEMA$,
              (GenericRecord) record.get("hoodieCleanMetadata")));
        }
        case HoodieTimeline.COMMIT_ACTION: {
          return Optional.of(serialize(HoodieCommitMetadata.SCHEMA$,
              (GenericRecord) record.get("hoodieCommitMetadata")));
        }
        case HoodieTimeline.DELTA_COMMIT_ACTION: {
          return Optional.of(serialize(HoodieCommitMetadata.SCHEMA$,
              (GenericRecord) record.get("hoodieCommitMetadata")));
        }
        case HoodieTimeline.ROLLBACK_ACTION: {
          return Optional.of(serialize(HoodieRollbackMetadata.SCHEMA$,
              (GenericRecord) record.get("hoodieRollbackMetadata")));
        }
        case HoodieTimeline.SAVEPOINT_ACTION: {
          return Optional.of(serialize(HoodieSavepointMetadata.SCHEMA$,
              (GenericRecord) record.get("hoodieSavePointMetadata")));
        }
        default:
          throw new RuntimeException("unknown action (" + record.get("actionType") + ") in archive");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private HoodieInstant getInstant(GenericRecord record) {
    return new HoodieInstant(State.COMPLETED, record.get("actionType").toString(),
        record.get("commitTime").toString());
  }

  /**
   * This method is only used when this object is deserialized in a spark executor.
   *
   * @deprecated
   */
  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  @Override
  public Optional<byte[]> getInstantDetails(HoodieInstant instant) {
    Optional<byte[]> r = readCommits.get(instant);
    return r == null ? Optional.empty() : r;
  }

  public com.uber.hoodie.common.table.timeline.HoodieArchivedTimeline reload() {
    return new com.uber.hoodie.common.table.timeline.HoodieArchivedTimeline(metaClient);
  }

}
