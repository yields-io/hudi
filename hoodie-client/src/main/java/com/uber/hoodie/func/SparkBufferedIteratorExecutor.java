/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.func;

import com.uber.hoodie.common.util.buffer.BufferProducer;
import com.uber.hoodie.common.util.buffer.BufferedIteratorExecutor;
import com.uber.hoodie.common.util.buffer.IteratorBasedBufferProducer;
import com.uber.hoodie.config.HoodieWriteConfig;
import java.util.Iterator;
import java.util.function.Function;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;

public class SparkBufferedIteratorExecutor<I, O, E> extends BufferedIteratorExecutor<I, O, E> {

  // Need to set current spark thread's TaskContext into newly launched thread so that new thread can access
  // TaskContext properties.
  final TaskContext sparkThreadTaskContext;

  public SparkBufferedIteratorExecutor(final HoodieWriteConfig hoodieConfig, final Iterator<I> inputItr,
      final Function<I, O> bufferedIteratorTransform) {
    this(hoodieConfig, new IteratorBasedBufferProducer<>(inputItr), bufferedIteratorTransform);
  }

  public SparkBufferedIteratorExecutor(final HoodieWriteConfig hoodieConfig, BufferProducer<I> producer,
      final Function<I, O> bufferedIteratorTransform) {
    super(hoodieConfig.getWriteBufferLimitBytes(), producer, bufferedIteratorTransform);
    this.sparkThreadTaskContext = TaskContext.get();
  }

  public void onStartingAsyncThread() {
    // Passing parent thread's TaskContext to newly launched thread for it to access original TaskContext properties.
    TaskContext$.MODULE$.setTaskContext(sparkThreadTaskContext);
  }

}
