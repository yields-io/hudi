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
import java.util.function.Function;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Async Buffer multi-producer which allows custom functions to insert entries to buffer.
 *
 * @param <I> Type of entry produced for buffer
 */
public class FunctionBasedBufferProducer<I> extends BufferProducer<I> {

  private static final Logger logger = LogManager.getLogger(FunctionBasedBufferProducer.class);

  private final List<Function<MemoryBoundedBuffer<I, ?>, Boolean>> producerFunctions;

  public FunctionBasedBufferProducer(List<Function<MemoryBoundedBuffer<I, ?>, Boolean>> producerFunctions) {
    this.producerFunctions = producerFunctions;
  }

  public FunctionBasedBufferProducer(Function<MemoryBoundedBuffer<I, ?>, Boolean>... producerFunctions) {
    this(Arrays.asList(producerFunctions));
  }

  @Override
  public int getNumProducers() {
    return producerFunctions.size();
  }

  @Override
  public void asyncStartProduction(MemoryBoundedBuffer<I, ?> buffer,
      ExecutorCompletionService<Void> executorService, Callable<Void> onStartingAsyncThreadCallback) {
    producerFunctions.forEach(producerFunction -> {
      executorService.submit(() -> {
        logger.info("starting function which will buffer records");
        onStartingAsyncThreadCallback.call();
        producerFunction.apply(buffer);
        logger.info("finished function which will buffer records");
        return null;
      });
    });
  }
}
