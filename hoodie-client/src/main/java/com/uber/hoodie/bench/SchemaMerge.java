package com.uber.hoodie.bench;

import java.io.File;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.kitesdk.data.spi.SchemaUtil;

public class SchemaMerge {

  public static void merge(String schemaPath1, String schemaPath2) throws Exception {
    Schema schema1 = new Schema.Parser().parse(new File(schemaPath1));
    Schema schema2 = new Schema.Parser().parse(new File(schemaPath2));

    Schema newSchema = SchemaUtil.mergeOrUnion(Arrays.asList(schema1, schema2));
    String schemaStr = newSchema.toString(true);
    System.out.println(schemaStr);
  }

  public static void main(String[] args) throws Exception {
    String path1 = "/Users/varadarb/uflurry_event_trace.avsc";
    String path2 = "/Users/varadarb/uber_hadoop_metadata.avsc";
    merge(path1, path2);
  }
}
