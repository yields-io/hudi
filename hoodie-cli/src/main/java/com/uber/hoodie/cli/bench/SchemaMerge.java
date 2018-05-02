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

package com.uber.hoodie.cli.bench;

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
