package com.uber.hoodie;

import org.apache.spark.api.java.JavaRDD;

public interface HoodieCompactionNotifier {

  void onStartCompaction(String commitTime);

  void onFinishCompaction(String commitTime,
      JavaRDD<CompactionStatus> compactionStatusJavaRDD);
}
