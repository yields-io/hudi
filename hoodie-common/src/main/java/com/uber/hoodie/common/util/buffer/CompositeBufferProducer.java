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

package com.uber.hoodie.common.util.buffer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;

/**
 * Producer that composes multiple real producers of buffer data
 * @param <I> Input type for buffer items produced
 */
public class CompositeBufferProducer<I> extends BufferProducer<I> {

  // Real Producers
  private final List<BufferProducer<I>> producers;

  public CompositeBufferProducer(List<BufferProducer<I>> producers) {
    this.producers = producers;
  }

  public CompositeBufferProducer(BufferProducer<I>... producers) {
    this(Arrays.asList(producers));
  }

  @Override
  int getNumProducers() {
    return (int) producers.stream().mapToInt(BufferProducer::getNumProducers).count();
  }

  @Override
  void asyncStartProduction(MemoryBoundedBuffer<I, ?> buffer, ExecutorCompletionService<Void> executorService,
      Callable<Void> onStartingAsyncThreadCallback) {
    producers.stream().forEach(producer -> {
      producer.asyncStartProduction(buffer, executorService, onStartingAsyncThreadCallback);
    });
  }
}
