package com.uber.hoodie.utilities;

import com.beust.jcommander.JCommander;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;

public class HDFSParquetUpserter extends HDFSParquetImporter {

  public HDFSParquetUpserter(Config cfg) throws IOException {
    super(cfg);
  }

  protected <T extends HoodieRecordPayload> JavaRDD<WriteStatus> importRecords(HoodieWriteClient client,
      String instantTime, JavaRDD<HoodieRecord<T>> hoodieRecords) {
    return client.upsert(hoodieRecords, instantTime);
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    HDFSParquetUpserter dataImporter = new HDFSParquetUpserter(cfg);
    dataImporter.dataImport(UtilHelpers.buildSparkContext(cfg.tableName, cfg.sparkMaster, cfg.sparkMemory), cfg.retry);
  }
}
