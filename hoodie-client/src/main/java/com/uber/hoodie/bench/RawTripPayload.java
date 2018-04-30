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

package com.uber.hoodie.bench;

import com.uber.hoodie.avro.MercifulJsonConverter;
import com.uber.hoodie.common.model.HoodieRecordPayload;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Copied from data-hoodie
 * Hoodie payload that encapsulates a single trips row change log.
 */
public class RawTripPayload implements HoodieRecordPayload<RawTripPayload> {

  private static final transient ObjectMapper mapper = new ObjectMapper();
  private String partitionPath;
  private String rowKey;
  private long maxStreamioOffset;
  private byte[] jsonDataCompressed;
  private int dataSize;


  public RawTripPayload(String jsonData) throws Exception {
    this.jsonDataCompressed = compressData(jsonData);
    this.dataSize = jsonData.length();
    Map<String, Object> jsonRecordMap = mapper.readValue(jsonData, Map.class);

    // extract uuid
    this.rowKey = jsonRecordMap.get("_row_key").toString();
    if (rowKey == null || rowKey.isEmpty()) {
      throw new IllegalArgumentException("Row key empty or null for json record " + jsonData);
    }
    // extract partition path
    partitionPath = null;
    // TODO(vc): One slight issue to handle here would be, to just discard row changes without
    // a BASE column (we will get it later anyways)
    if (jsonRecordMap.containsKey("BASE")) {
      Map<String, Object> baseColMap = (Map<String, Object>) jsonRecordMap.get("BASE");
      if (baseColMap.containsKey("request_at")) {
        String requestAtVal = baseColMap.get("request_at").toString();
        // This is for some bad trips records whose BASE.request_at is epoch
        if (requestAtVal.contains("T")) {
          partitionPath = requestAtVal.split("T")[0].replace("-", "/");
        } else {
          Date date = new Date(Long.parseLong(requestAtVal));
          partitionPath = new SimpleDateFormat("yyyy/MM/dd").format(date);
        }

      }
    }

    // max streamio offset
    this.maxStreamioOffset =
        Long.parseLong(jsonRecordMap.get("_max_streamio_offset").toString());
  }


  @Override
  public RawTripPayload preCombine(RawTripPayload another) {
    return (this.maxStreamioOffset >= another.maxStreamioOffset) ? this : another;
  }

  @Override
  public Optional<IndexedRecord> combineAndGetUpdateValue(IndexedRecord oldRec, Schema schema) throws IOException {
    // Always return same record for this test job. The real production job will do real comparison.
    return getInsertValue(schema);
  }

  @Override
  public Optional<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    MercifulJsonConverter jsonConverter = new MercifulJsonConverter(schema);
    return Optional.of(jsonConverter.convert(getJsonData()));
  }

  public String getRowKey() {
    return rowKey;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getJsonData() throws IOException {
    return unCompressData(jsonDataCompressed);
  }

  private byte[] compressData(String jsonData) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DeflaterOutputStream dos =
        new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_COMPRESSION), true);
    try {
      dos.write(jsonData.getBytes());
    } finally {
      dos.flush();
      dos.close();
    }
    return baos.toByteArray();
  }


  private String unCompressData(byte[] data) throws IOException {
    InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(data));
    StringWriter sw = new StringWriter(dataSize);
    IOUtils.copy(iis, sw);
    return sw.toString();
  }
}
