/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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


package com.uber.hoodie.common.util.buffer;

import com.uber.hoodie.exception.HoodieException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Executor for a BufferedIterator operation. This class takes as input the size limit, buffer producer and transformer
 * and exposes an API for custom processing of buffered entries
 */
public class BufferedIteratorExecutor<I, O, E> {

  private static Logger logger = LogManager.getLogger(BufferedIteratorExecutor.class);

  // Executor service used for launching writer thread.
  private final ExecutorService executorService;
  // Used for buffering records which is controlled by HoodieWriteConfig#WRITE_BUFFER_LIMIT_BYTES.
  private final MemoryBoundedBuffer<I, O> buffer;
  // Buffer Producer
  private final BufferProducer<I> producer;

  public BufferedIteratorExecutor(final long bufferLimitInBytes, BufferProducer<I> producer,
      final Function<I, O> bufferedIteratorTransform) {
    this.producer = producer;
    // Ensure single thread for each producer thread and one for consumer
    this.executorService = Executors.newFixedThreadPool(producer.getNumThreadsNeeded() + 1);
    this.buffer = new MemoryBoundedBuffer<>(bufferLimitInBytes, bufferedIteratorTransform);
  }

  /**
   * Callback to implement environment specific behavior
   */
  public void onStartingAsyncThread() {
    // Do Nothing in general context
  }

  /**
   * Start Only Producer
   * @return
   */
  public Future<Void> startProducers() {
    try {
      return producer.start(buffer, this.executorService, () -> {
        onStartingAsyncThread();
        return null;
      });
    } catch (Exception ex) {
      throw new HoodieException(ex);
    }
  }

  /**
   * Start only consumer
   * @param processFunction Process function to apply
   * @return
   */
  public Future<E> startConsumer(Function<MemoryBoundedBuffer, E> processFunction) {
    return executorService.submit(
        () -> {
          logger.info("starting hoodie buffer processor thread");
          onStartingAsyncThread();
          try {
            E result = processFunction.apply(buffer);
            logger.info("hoodie buffer processing is done; notifying producer thread");
            return result;
          } catch (Exception e) {
            logger.error("error buffer processing hoodie records", e);
            buffer.markAsFailed(e);
            throw e;
          }
        });
  }

  /**
   * Starts both production and consumption using the process function
   */
  public Future<E> start(Function<MemoryBoundedBuffer, E> processFunction) {
    try {
      Future<Void> produceResult = startProducers();

      Future<E> future = startConsumer(processFunction);

      // Wait for producers to complete
      produceResult.get();

      return future;
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  public boolean isRemaining() {
    return buffer.iterator().hasNext();
  }

  public void shutdown() {
    executorService.shutdownNow();
  }

  public MemoryBoundedBuffer<I, O> getBuffer() {
    return buffer;
  }
}
