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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import com.uber.hoodie.common.BloomFilter;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.func.ParquetReaderIterator;
import com.uber.hoodie.io.storage.HoodieParquetConfig;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class CreateHandlePerfTestMain {

  public final Config config;
  public final JavaSparkContext jsc;
  public final FileSystem fs;
  public final Schema schema;
  public final ParquetReaderIterator<IndexedRecord> readerIterator;
  public final List<IndexedRecord> recordList;

  public CreateHandlePerfTestMain(JavaSparkContext jsc,
      String basePath, String inputParquetFilePath, String avroSchemaFilePath,
      String outputDir, Integer numParquetFilesToWrite)
      throws IOException {
    this.jsc = jsc;
    this.config = new Config(basePath, inputParquetFilePath, avroSchemaFilePath, outputDir, numParquetFilesToWrite);
    this.fs = FSUtils.getFs(config.basePath, jsc.hadoopConfiguration());
    this.schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(new File(config.avroSchemaFilePath)));
    AvroReadSupport.setAvroReadSchema(jsc.hadoopConfiguration(), schema);
    ParquetReader<IndexedRecord> reader = AvroParquetReader.builder(new Path(config.inputParquetFilePath))
        .withConf(jsc.hadoopConfiguration()).build();
    this.readerIterator = new ParquetReaderIterator<>(reader);
    this.recordList = readParquet();
  }

  public static void main(String[] args) throws Exception {
    // Take input configs
    final Config cfg = new Config();
    new JCommander(cfg, args);
    SparkConf sparkConf = new SparkConf().setAppName("Hoodie-snapshot-copier");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    CreateHandlePerfTestMain perfMain = new CreateHandlePerfTestMain(jsc, cfg.basePath,
        cfg.inputParquetFilePath, cfg.avroSchemaFilePath, cfg.outputDir, cfg.numParquetFilesToWrite);
  }

  public Timer run() throws Exception {
    return run(config.numParquetFilesToWrite);
  }

  public Timer run(int numParquetFilesToWrite) throws Exception {
    final MetricRegistry metrics = new MetricRegistry();
    final Timer latencyTimer = metrics.timer("latency");
    for (int i = 0; i < numParquetFilesToWrite; i++) {
      Timer.Context c = latencyTimer.time();
      writeOneRound();
      c.stop();
    }

    Snapshot snapshot = latencyTimer.getSnapshot();
    System.out.println("Count :" + latencyTimer.getCount());
    System.out.println("Median :" + snapshot.getMedian());
    System.out.println("Mean :" + snapshot.getMean());
    System.out.println("Min :" + snapshot.getMin());
    System.out.println("75th :" + snapshot.get75thPercentile());
    System.out.println("95th :" + snapshot.get95thPercentile());
    System.out.println("98th :" + snapshot.get98thPercentile());
    System.out.println("Max :" + snapshot.getMax());
    System.out.println("StdDev :" + snapshot.getStdDev());
    return latencyTimer;
  }

  private void writeOneRound() throws Exception {
    String fileId = UUID.randomUUID().toString();
    String path = config.outputDir + "/" + fileId + ".test.parquet";
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(schema), schema, new BloomFilter(recordList.size(), 0.00001, 1));

    HoodieParquetConfig parquetConfig =
        new HoodieParquetConfig(writeSupport, CompressionCodecName.GZIP,
            ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, 120 * 1024 * 1024,
            jsc.hadoopConfiguration(),
            Double.valueOf(HoodieStorageConfig.DEFAULT_STREAM_COMPRESSION_RATIO));
    ParquetWriter writer = new ParquetWriter(new Path(path), ParquetFileWriter.Mode.CREATE,
        parquetConfig.getWriteSupport(),
        parquetConfig.getCompressionCodecName(), parquetConfig.getBlockSize(),
        parquetConfig.getPageSize(), parquetConfig.getPageSize(),
        ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED, ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
        ParquetWriter.DEFAULT_WRITER_VERSION, jsc.hadoopConfiguration());
    for (IndexedRecord r : recordList) {
      writer.write(r);
    }
    writer.close();
  }

  public List<IndexedRecord> readParquet() {
    System.out.println("Reading hoodie table from " + config.basePath);
    List<IndexedRecord> payloadList = new ArrayList<>();
    while (readerIterator.hasNext()) {
      IndexedRecord payload = readerIterator.next();
      payloadList.add(payload);
      //System.out.println("Payload : " + payload);
    }
    return payloadList;
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--base-path", "-bp"}, description = "Hoodie table base path", required = true)
    String basePath = null;

    @Parameter(names = {"--input-parquet-file", "-i"}, required = true, description = "Parquet File to read from")
    String inputParquetFilePath = null;

    @Parameter(names = {"--avro-schema-file", "-i"}, required = true, description = "Avro schema to read from")
    String avroSchemaFilePath = null;

    @Parameter(names = {"--output-dir", "-o"}, description = "Output Dir to write parquet files", required = true)
    String outputDir = null;

    @Parameter(names = {"--num-output-files", "-n"}, description = "#parquet files to write", required = true)
    Integer numParquetFilesToWrite = 10;

    public Config() {
    }

    public Config(String basePath, String inputParquetFilePath, String avroSchemaFilePath,
        String outputDir, Integer numParquetFilesToWrite) {
      this.basePath = basePath;
      this.inputParquetFilePath = inputParquetFilePath;
      this.avroSchemaFilePath = avroSchemaFilePath;
      this.outputDir = outputDir;
      this.numParquetFilesToWrite = numParquetFilesToWrite;
    }
  }
}
