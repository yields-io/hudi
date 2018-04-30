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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.func.ParquetReaderIterator;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class CreateHandlePerfMain {

  private static Logger logger = LogManager.getLogger(CreateHandlePerfMain.class);
  private final Config config;
  private final JavaSparkContext jsc;
  private final FileSystem fs;
  private final Schema schema;
  private final ParquetReaderIterator<RawTripPayload> readerIterator;
  private final List<RawTripPayload> rawTripPayloadList;

  public CreateHandlePerfMain(Config config) throws IOException {
    this.config = config;
    SparkConf sparkConf = new SparkConf().setAppName("Hoodie-snapshot-copier");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    this.jsc = new JavaSparkContext(sparkConf);
    this.fs = FSUtils.getFs(config.basePath, jsc.hadoopConfiguration());
    this.schema = new Schema.Parser().parse(new File(config.avroSchemaFilePath));
    AvroReadSupport.setAvroReadSchema(jsc.hadoopConfiguration(), schema);
    ParquetReader<RawTripPayload> reader = AvroParquetReader.builder(new Path(config.inputParquetFilePath))
        .withConf(jsc.hadoopConfiguration()).build();
    this.readerIterator = new ParquetReaderIterator<RawTripPayload>(reader);
    this.rawTripPayloadList = readParquet();
  }

  public static void main(String[] args) throws Exception {
    // Take input configs
    final Config cfg = new Config();
    new JCommander(cfg, args);
    CreateHandlePerfMain perfMain = new CreateHandlePerfMain(cfg);
  }

  public List<RawTripPayload> readParquet() {
    logger.info(String.format("Reading hoodie table from %targetBasePath ", config.basePath));
    List<RawTripPayload> payloadList = new ArrayList<>();
    while (readerIterator.hasNext()) {
      RawTripPayload payload = readerIterator.next();
      payloadList.add(payload);
      System.out.println("Payload : " + payload);
    }
    return payloadList;
  }

  static class Config implements Serializable {

    @Parameter(names = {"--base-path", "-bp"}, description = "Hoodie table base path", required = true)
    String basePath = null;

    @Parameter(names = {"--input-parquet-file", "-i"}, required = true, description = "Parquet File to read from")
    String inputParquetFilePath = null;

    @Parameter(names = {"--avro-schema-file", "-i"}, required = true, description = "Avro schema to read from")
    String avroSchemaFilePath = null;

    public String getBasePath() {
      return basePath;
    }

    public void setBasePath(String basePath) {
      this.basePath = basePath;
    }

    public String getInputParquetFilePath() {
      return inputParquetFilePath;
    }

    public void setInputParquetFilePath(String inputParquetFilePath) {
      this.inputParquetFilePath = inputParquetFilePath;
    }

    public String getAvroSchemaFilePath() {
      return avroSchemaFilePath;
    }

    public void setAvroSchemaFilePath(String avroSchemaFilePath) {
      this.avroSchemaFilePath = avroSchemaFilePath;
    }
  }
}
