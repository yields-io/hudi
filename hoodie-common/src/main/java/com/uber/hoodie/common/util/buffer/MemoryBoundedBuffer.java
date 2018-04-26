/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util.buffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.uber.hoodie.common.util.SizeEstimator;
import com.uber.hoodie.exception.HoodieException;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Used for buffering input records. Buffer limit is controlled by {@link #bufferMemoryLimit}. It
 * internally samples every {@link #RECORD_SAMPLING_RATE}th record and adjusts number of records in
 * buffer accordingly. This is done to ensure that we don't OOM.
 *
 * This buffer supports multiple producer single consumer pattern.
 *
 * @param <I> input payload data type
 * @param <O> output payload data type
 */
public class MemoryBoundedBuffer<I, O> implements Iterable<O> {

  // interval used for polling records in the queue.
  public static final int RECORD_POLL_INTERVAL_SEC = 5;
  // rate used for sampling records to determine avg record size in bytes.
  public static final int RECORD_SAMPLING_RATE = 64;
  // maximum records that will be cached
  private static final int RECORD_CACHING_LIMIT = 128 * 1024;
  private static Logger logger = LogManager.getLogger(MemoryBoundedBuffer.class);
  // It indicates number of records to cache. We will be using sampled record's average size to
  // determine how many
  // records we should cache and will change (increase/decrease) permits accordingly.
  @VisibleForTesting
  public final Semaphore rateLimiter = new Semaphore(1);
  // used for sampling records with "RECORD_SAMPLING_RATE" frequency.
  public final AtomicLong samplingRecordCounter = new AtomicLong(-1);
  // internal buffer to cache buffered records.
  private final LinkedBlockingQueue<Optional<O>> buffer = new
      LinkedBlockingQueue<>();
  // maximum amount of memory to be used for buffering records.
  private final long bufferMemoryLimit;
  // it holds the root cause of the exception in case either buffering records (reading from
  // inputIterator) fails or
  // thread reading records from buffer fails.
  private final AtomicReference<Exception> hasFailed = new AtomicReference(null);
  // used for indicating that all the records from buffer are read successfully.
  private final AtomicBoolean isDone = new AtomicBoolean(false);
  // indicates rate limit (number of records to cache). it is updated whenever there is a change
  // in avg record size.
  @VisibleForTesting
  public int currentRateLimit = 1;
  // indicates avg record size in bytes. It is updated whenever a new record is sampled.
  @VisibleForTesting
  public long avgRecordSizeInBytes = 0;
  // indicates number of samples collected so far.
  private long numSamples = 0;
  // Function to transform the input payload to the expected output payload
  private final Function<I, O> bufferedIteratorTransform;

  // Payload Size Estimator
  private final SizeEstimator<O> payloadSizeEstimator;

  // Singleton (w.r.t this instance) Iterator for this buffer
  private final BufferIterator iterator;

  /**
   * Construct MemoryBoundedBuffer with default SizeEstimator
   * @param bufferMemoryLimit MemoryLimit in bytes
   * @param bufferedIteratorTransform Transformer Function to convert input payload type to stored payload type
   */
  public MemoryBoundedBuffer(final long bufferMemoryLimit, final Function<I, O> bufferedIteratorTransform) {
    this(bufferMemoryLimit, bufferedIteratorTransform, new SizeEstimator<O>() {
    });
  }

  /**
   * Construct MemoryBoundedBuffer with passed in size estimator
   * @param bufferMemoryLimit MemoryLimit in bytes
   * @param bufferedIteratorTransform Transformer Function to convert input payload type to stored payload type
   * @param payloadSizeEstimator Payload Size Estimator
   */
  public MemoryBoundedBuffer(
      final long bufferMemoryLimit,
      final Function<I, O> bufferedIteratorTransform,
      final SizeEstimator<O> payloadSizeEstimator) {
    this.bufferMemoryLimit = bufferMemoryLimit;
    this.bufferedIteratorTransform = bufferedIteratorTransform;
    this.payloadSizeEstimator = payloadSizeEstimator;
    this.iterator = new BufferIterator();
  }

  @VisibleForTesting
  public int size() {
    return this.buffer.size();
  }

  /**
   * Samples records with "RECORD_SAMPLING_RATE" frequency and computes average record size in bytes. It is used
   * for determining how many maximum records to buffer. Based on change in avg size it ma increase or decrease
   * available permits.
   *
   * @param payload Payload to size
   */
  private void adjustBufferSizeIfNeeded(final O payload) throws InterruptedException {
    if (this.samplingRecordCounter.incrementAndGet() % RECORD_SAMPLING_RATE != 0) {
      return;
    }

    final long recordSizeInBytes = payloadSizeEstimator.sizeEstimate(payload);
    final long newAvgRecordSizeInBytes = Math
        .max(1, (avgRecordSizeInBytes * numSamples + recordSizeInBytes) / (numSamples + 1));
    final int newRateLimit = (int) Math
        .min(RECORD_CACHING_LIMIT, Math.max(1, this.bufferMemoryLimit / newAvgRecordSizeInBytes));

    // If there is any change in number of records to cache then we will either release (if it increased) or acquire
    // (if it decreased) to adjust rate limiting to newly computed value.
    if (newRateLimit > currentRateLimit) {
      rateLimiter.release(newRateLimit - currentRateLimit);
    } else if (newRateLimit < currentRateLimit) {
      rateLimiter.acquire(currentRateLimit - newRateLimit);
    }
    currentRateLimit = newRateLimit;
    avgRecordSizeInBytes = newAvgRecordSizeInBytes;
    numSamples++;
  }

  /**
   * Inserts record into buffer after applying transformation
   *
   * @param t Item to be buffered
   */
  public void insertRecord(I t) throws Exception {
    // We need to stop buffering if buffer-reader has failed and exited.
    throwExceptionIfFailed();

    rateLimiter.acquire();
    // We are retrieving insert value in the record buffering thread to offload computation
    // around schema validation
    // and record creation to it.
    final O payload = bufferedIteratorTransform.apply(t);
    adjustBufferSizeIfNeeded(payload);
    buffer.put(Optional.of(payload));
  }

  /**
   * Reader interface but never exposed to outside world as this is a single consumer buffer.
   * Reading is done through a singleton iterator for this buffer.
   */
  private Optional<O> readNextRecord() {
    if (this.isDone.get()) {
      return Optional.empty();
    }

    rateLimiter.release();
    Optional<O> newRecord = Optional.empty();
    while (true) {
      try {
        throwExceptionIfFailed();
        newRecord = buffer.poll(RECORD_POLL_INTERVAL_SEC, TimeUnit.SECONDS);
        if (newRecord != null) {
          break;
        }
      } catch (InterruptedException e) {
        logger.error("error reading records from BufferedIterator", e);
        throw new HoodieException(e);
      }
    }
    if (newRecord.isPresent()) {
      return newRecord;
    } else {
      // We are done reading all the records from internal iterator.
      this.isDone.set(true);
      return Optional.empty();
    }
  }

  /**
   * Puts an empty entry to buffer to denote termination
   */
  public void stopProducing() throws InterruptedException {
    // done buffering records notifying buffer-reader.
    buffer.put(Optional.empty());
  }

  private void throwExceptionIfFailed() {
    if (this.hasFailed.get() != null) {
      throw new HoodieException("operation has failed", this.hasFailed.get());
    }
  }

  /**
   * API to allow producers and consumer to communicate termination due to failure
   */
  public void markAsFailed(Exception e) {
    this.hasFailed.set(e);
    // release the permits so that if the buffering thread is waiting for permits then it will
    // get it.
    this.rateLimiter.release(RECORD_CACHING_LIMIT + 1);
  }

  @Override
  public Iterator<O> iterator() {
    return iterator;
  }

  /**
   * Iterator for the memory bounded buffer
   */
  private final class BufferIterator implements Iterator<O> {

    // next record to be read from buffer.
    private O nextRecord;

    @Override
    public boolean hasNext() {
      if (this.nextRecord == null) {
        Optional<O> res = readNextRecord();
        this.nextRecord = res.orElse(null);
      }
      return this.nextRecord != null;
    }

    @Override
    public O next() {
      Preconditions.checkState(hasNext() && this.nextRecord != null);
      final O ret = this.nextRecord;
      this.nextRecord = null;
      return ret;
    }
  }
}