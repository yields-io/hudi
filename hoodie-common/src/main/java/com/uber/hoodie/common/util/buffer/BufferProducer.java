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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Async Producer for MemoryBoundedBuffer. Memory Bounded Buffer supports
 * multiple producers single consumer pattern.
 *
 * @param <I> Input type for buffer items produced
 */
public abstract class BufferProducer<I> {

  private static final Logger logger = LogManager.getLogger(BufferProducer.class);

  /**
   * Returns number of producers that would be producing entries to buffer
   */
  abstract int getNumProducers();

  /**
   * Allows client to size the threads needed in ExecutorService
   * @return number of threads needed
   */
  public final int getNumThreadsNeeded() {
    // Add 1 for thread that stops production after all producers are done
    return getNumProducers() + 1;
  }

  /**
   * Main API to asynchronously start producing records.
   * @param buffer Memory buffer
   * @param executorService Executor Service to use
   * @param onStartingAsyncThreadCallback Callback called by spawned thread before producing
   * @return producer future
   * @throws Exception in case of error
   */
  public final Future<Void> start(
      final MemoryBoundedBuffer<I, ?> buffer, ExecutorService executorService,
      Callable<Void> onStartingAsyncThreadCallback) throws Exception {
    final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(executorService);
    // start async threads. No need to collect futures as completionService takes care of ensuring completion
    asyncStartProduction(buffer, completionService, onStartingAsyncThreadCallback);
    final int numProducers = getNumProducers();
    // Finally add an async task that will wait for producers to finish and mark buffer as done
    return executorService.submit(() -> {
      // THis is the thread that waits on other producers. Once all producers are done, this
      // thread will mark the buffer as done.
      logger.info("Starting helper thread to mark production as done after producers completed");
      onStartingAsyncThreadCallback.call();
      for (int i = 0; i < numProducers; i++) {
        try {
          completionService.take().get();
        } catch (ExecutionException ex) {
          logger.error("Got execution exception running producer. Marking production as failed", ex);
          buffer.markAsFailed((Exception)ex.getCause());
          throw (Exception)ex.getCause();
        } catch (Exception e) {
          logger.error("Got error running producer. Marking production as failed", e);
          buffer.markAsFailed(e);
          throw e;
        }
      }
      // Mark production as done
      buffer.stopProducing();
      logger.info("Completed marking production as done as producers have completed");
      return null;
    });
  }

  /**
   * Async API to start producing entries to memory bounded buffer.
   *
   * @param executorService               Executor Service to run async
   * @param onStartingAsyncThreadCallback Callback for environment specific setup
   */
  abstract void asyncStartProduction(MemoryBoundedBuffer<I, ?> buffer,
      ExecutorCompletionService<Void> executorService,
      Callable<Void> onStartingAsyncThreadCallback);
}
