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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Iterator based producers which pulls entry from iterator and produces items for the buffer
 *
 * @param <I> Item type produced for the buffer.
 */
public class IteratorBasedBufferProducer<I> extends BufferProducer<I> {

  private static final Logger logger = LogManager.getLogger(IteratorBasedBufferProducer.class);

  // List of input iterators for producing items in the buffer.
  private final List<Iterator<I>> inputIterators;

  public IteratorBasedBufferProducer(Iterator<I>... inputIterators) {
    this(Arrays.asList(inputIterators));
  }

  public IteratorBasedBufferProducer(List<Iterator<I>> inputIterators) {
    this.inputIterators = inputIterators;
  }

  @Override
  public int getNumProducers() {
    return inputIterators.size();
  }

  @Override
  public void asyncStartProduction(MemoryBoundedBuffer<I, ?> buffer,
      ExecutorCompletionService<Void> executorService, Callable<Void> onStartingAsyncThreadCallback) {
    inputIterators.forEach(inputIterator -> {
      executorService.submit(() -> {
        logger.info("starting to buffer records");
        onStartingAsyncThreadCallback.call();
        while (inputIterator.hasNext()) {
          buffer.insertRecord(inputIterator.next());
        }
        logger.info("finished buffering records");
        return null;
      });
    });
  }
}
