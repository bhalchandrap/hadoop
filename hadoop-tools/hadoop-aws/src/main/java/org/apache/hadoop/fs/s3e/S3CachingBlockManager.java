/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3e;

import org.apache.hadoop.fs.common.BlockCache;
import org.apache.hadoop.fs.common.BlockData;
import org.apache.hadoop.fs.common.BlockOperations;
import org.apache.hadoop.fs.common.BufferData;
import org.apache.hadoop.fs.common.BufferPool;
import org.apache.hadoop.fs.common.Io;
import org.apache.hadoop.fs.common.Retryer;
import org.apache.hadoop.fs.common.SingleFilePerBlockCache;
import org.apache.hadoop.fs.common.Validate;

import com.twitter.util.Await;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides access to S3 file one block at a time.
 */
public class S3CachingBlockManager extends S3BlockManager {
  private static final Logger LOG = LoggerFactory.getLogger(S3CachingBlockManager.class);

  // Asynchronous tasks are performed in this pool.
  protected FuturePool futurePool;

  // Pool of shared ByteBuffer instances.
  protected BufferPool bufferPool;

  // Size of the in-memory cache in terms of number of blocks.
  // Total memory consumption is up to bufferPoolSize * blockSize.
  protected int bufferPoolSize;

  // Local block cache.
  private BlockCache cache;

  // Error counts. For testing purposes.
  private AtomicInteger numCachingErrors;
  private AtomicInteger numReadErrors;

  // Operations performed by this block manager.
  private BlockOperations ops;

  private boolean closed;

  // If a single caching operation takes more than this time (in seconds),
  // we disable caching to prevent further perf degradation due to caching.
  private static final int SLOW_CACHING_THRESHOLD = 5;

  // Once set to true, any further caching requests will be ignored.
  private AtomicBoolean cachingDisabled;

  /**
   * Constructs an instance of a {@code S3CachingBlockManager}.
   *
   * @param futurePool asynchronous tasks are performed in this pool.
   * @param reader reader that reads from S3 file.
   * @param blockData information about each block of the S3 file.
   * @param bufferPoolSize size of the in-memory cache in terms of number of blocks.
   */
  public S3CachingBlockManager(
      FuturePool futurePool,
      S3Reader reader,
      BlockData blockData,
      int bufferPoolSize) {
    super(reader, blockData);

    Validate.checkNotNull(futurePool, "futurePool");
    Validate.checkPositiveInteger(bufferPoolSize, "bufferPoolSize");

    this.futurePool = futurePool;
    this.bufferPoolSize = bufferPoolSize;
    this.numCachingErrors = new AtomicInteger();
    this.numReadErrors = new AtomicInteger();
    this.cachingDisabled = new AtomicBoolean();

    if (this.blockData.fileSize > 0) {
      this.bufferPool = new BufferPool(bufferPoolSize, this.blockData.blockSize);
      this.cache = this.createCache();
    }

    this.ops = new BlockOperations();
    this.ops.setDebug(false);
  }

  /**
   * Gets the block having the given {@code blockNumber}.
   */
  @Override
  public BufferData get(int blockNumber) throws IOException {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    BufferData data = null;
    final int maxRetryDelayMs = this.bufferPoolSize * 120 * 1000;
    final int statusUpdateDelayMs = 120 * 1000;
    Retryer retryer = new Retryer(10, maxRetryDelayMs, statusUpdateDelayMs);
    boolean done;

    do {
      if (this.closed) {
        throw new IOException("this stream is already closed");
      }

      data = this.bufferPool.acquire(blockNumber);
      done = this.getInternal(data);

      if (retryer.updateStatus()) {
        LOG.warn("waiting to get block: {}", blockNumber);
        LOG.info("state = {}", this.toString());
      }
    }
    while (!done && retryer.continueRetry());

    if (done) {
      return data;
    } else {
      String message = String.format("Wait failed for get(%d)", blockNumber);
      throw new IllegalStateException(message);
    }
  }

  private boolean getInternal(BufferData data) throws IOException {
    Validate.checkNotNull(data, "data");

    // Opportunistic check without locking.
    if (data.stateEqualsOneOf(
        BufferData.State.PREFETCHING,
        BufferData.State.CACHING,
        BufferData.State.DONE)) {
      return false;
    }

    synchronized (data) {
      // Reconfirm state after locking.
      if (data.stateEqualsOneOf(
          BufferData.State.PREFETCHING,
          BufferData.State.CACHING,
          BufferData.State.DONE)) {
        return false;
      }

      int blockNumber = data.blockNumber;
      if (data.getState() == BufferData.State.READY) {
        BlockOperations.Operation op = this.ops.getPrefetched(blockNumber);
        this.ops.end(op);
        return true;
      }

      data.throwIfStateIncorrect(BufferData.State.BLANK);
      this.read(data);
      return true;
    }
  }

  /**
   * Releases resources allocated to the given block.
   */
  @Override
  public void release(BufferData data) {
    if (this.closed) {
      return;
    }

    Validate.checkNotNull(data, "data");

    BlockOperations.Operation op = this.ops.release(data.blockNumber);
    this.bufferPool.release(data);
    this.ops.end(op);
  }

  @Override
  public synchronized void close() {
    if (this.closed) {
      return;
    }

    this.closed = true;

    final BlockOperations.Operation op = this.ops.close();

    this.reader.close();

    // Cancel any prefetches in progress.
    this.cancelPrefetches();

    Io.closeIgnoringIoException(this.cache);

    this.ops.end(op);
    LOG.info(this.ops.getSummary(false));

    this.bufferPool.close();
    this.bufferPool = null;
  }

  /**
   * Requests optional prefetching of the given block.
   * The block is prefetched only if we can acquire a free buffer.
   */
  @Override
  public void requestPrefetch(int blockNumber) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    if (this.closed) {
      return;
    }

    // We initiate a prefetch only if we can acquire a buffer from the shared pool.
    BufferData data = this.bufferPool.tryAcquire(blockNumber);
    if (data == null) {
      return;
    }

    // Opportunistic check without locking.
    if (!data.stateEqualsOneOf(BufferData.State.BLANK)) {
      // The block is ready or being prefetched/cached.
      return;
    }

    synchronized (data) {
      // Reconfirm state after locking.
      if (!data.stateEqualsOneOf(BufferData.State.BLANK)) {
        // The block is ready or being prefetched/cached.
        return;
      }

      BlockOperations.Operation op = this.ops.requestPrefetch(blockNumber);
      PrefetchTask prefetchTask = new PrefetchTask(data, this);
      Future<Void> prefetchFuture = this.futurePool.apply(prefetchTask);
      data.setPrefetch(prefetchFuture);
      this.ops.end(op);
    }
  }

  /**
   * Requests cancellation of any previously issued prefetch requests.
   */
  @Override
  public void cancelPrefetches() {
    BlockOperations.Operation op = this.ops.cancelPrefetches();

    for (BufferData data : this.bufferPool.getAll()) {
      // We add blocks being prefetched to the local cache so that the prefetch is not wasted.
      if (data.stateEqualsOneOf(BufferData.State.PREFETCHING, BufferData.State.READY)) {
        this.requestCaching(data);
      }
    }

    this.ops.end(op);
  }

  // @VisibleForTesting
  protected int read(ByteBuffer buffer, long offset, int size) throws IOException {
    return this.reader.read(buffer, offset, size);
  }

  private void read(BufferData data) throws IOException {
    synchronized (data) {
      this.readBlock(data, false, BufferData.State.BLANK);
    }
  }

  private void prefetch(BufferData data) throws IOException {
    synchronized (data) {
      this.readBlock(
          data,
          true,
          BufferData.State.PREFETCHING,
          BufferData.State.CACHING);
    }
  }

  private void readBlock(BufferData data, boolean isPrefetch, BufferData.State... expectedState)
      throws IOException {

    if (this.closed) {
      return;
    }

    BlockOperations.Operation op = null;

    synchronized (data) {
      try {
        if (data.stateEqualsOneOf(BufferData.State.DONE, BufferData.State.READY)) {
          // DONE  : Block was released, likely due to caching being disabled on slow perf.
          // READY : Block was already fetched by another thread. No need to re-read.
          return;
        }

        data.throwIfStateIncorrect(expectedState);
        int blockNumber = data.blockNumber;

        // Prefer reading from cache over reading from S3.
        if (this.cache.containsBlock(blockNumber)) {
          op = this.ops.getCached(blockNumber);
          this.cache.get(blockNumber, data.getBuffer());
          data.setReady(expectedState);
          return;
        }

        if (isPrefetch) {
          op = this.ops.prefetch(data.blockNumber);
        } else {
          op = this.ops.getRead(data.blockNumber);
        }

        long offset = this.blockData.getStartOffset(data.blockNumber);
        int size = this.blockData.getSize(data.blockNumber);
        ByteBuffer buffer = data.getBuffer();
        buffer.clear();
        this.read(buffer, offset, size);
        buffer.flip();
        data.setReady(expectedState);
      } catch (Exception e) {
        String message = String.format("error during readBlock(%s)", data.blockNumber);
        LOG.error(message, e);
        this.numReadErrors.incrementAndGet();
        data.setDone();
        throw e;
      } finally {
        if (op != null) {
          this.ops.end(op);
        }
      }
    }
  }

  /**
   * Read task that is submitted to the future pool.
   */
  private static class PrefetchTask extends ExceptionalFunction0<Void> {
    private final BufferData data;
    private final S3CachingBlockManager blockManager;

    public PrefetchTask(BufferData data, S3CachingBlockManager blockManager) {
      this.data = data;
      this.blockManager = blockManager;
    }

    @Override
    public Void applyE() {
      try {
        this.blockManager.prefetch(data);
      } catch (Exception e) {
        LOG.error("error during prefetch", e);
      }
      return null;
    }
  }

  private static final BufferData.State[] EXPECTED_STATE_AT_CACHING =
      new BufferData.State[] {
        BufferData.State.PREFETCHING, BufferData.State.READY
      };

  /**
   * Requests that the given block should be copied to the local cache.
   * The block must not be accessed by the caller after calling this method
   * because it will released asynchronously relative to the caller.
   */
  @Override
  public void requestCaching(BufferData data) {
    if (this.closed) {
      return;
    }

    if (this.cachingDisabled.get()) {
      data.setDone();
      return;
    }

    Validate.checkNotNull(data, "data");

    // Opportunistic check without locking.
    if (!data.stateEqualsOneOf(EXPECTED_STATE_AT_CACHING)) {
      return;
    }

    synchronized (data) {
      // Reconfirm state after locking.
      if (!data.stateEqualsOneOf(EXPECTED_STATE_AT_CACHING)) {
        return;
      }

      if (this.cache.containsBlock(data.blockNumber)) {
        data.setDone();
        return;
      }

      BufferData.State state = data.getState();

      BlockOperations.Operation op = this.ops.requestCaching(data.blockNumber);
      Future<Void> blockFuture;
      if (state == BufferData.State.PREFETCHING) {
        blockFuture = data.getActionFuture();
      } else {
        blockFuture = Future.value(null);
      }

      CachePutTask task = new CachePutTask(data, blockFuture, this);
      Future<Void> actionFuture = this.futurePool.apply(task);
      data.setCaching(actionFuture);
      this.ops.end(op);
    }
  }

  private void addToCacheAndRelease(BufferData data, Future<Void> blockFuture) {
    if (this.closed) {
      return;
    }

    if (this.cachingDisabled.get()) {
      data.setDone();
      return;
    }

    try {
      Await.result(blockFuture);
      if (data.stateEqualsOneOf(BufferData.State.DONE)) {
        // There was an error during prefetch.
        return;
      }
    } catch (Exception e) {
      String message = String.format("error waitng on blockFuture: %s", data);
      LOG.error(message, e);
      data.setDone();
      return;
    }

    if (this.cachingDisabled.get()) {
      data.setDone();
      return;
    }

    BlockOperations.Operation op = null;

    synchronized (data) {
      try {
        if (data.stateEqualsOneOf(BufferData.State.DONE)) {
          return;
        }

        if (this.cache.containsBlock(data.blockNumber)) {
          data.setDone();
          return;
        }

        op = this.ops.addToCache(data.blockNumber);
        ByteBuffer buffer = data.getBuffer().duplicate();
        buffer.rewind();
        this.cachePut(data.blockNumber, buffer);
        data.setDone();
      } catch (Exception e) {
        this.numCachingErrors.incrementAndGet();
        String message = String.format("error adding block to cache after wait: %s", data);
        LOG.error(message, e);
        data.setDone();
      }

      if (op != null) {
        BlockOperations.End endOp = (BlockOperations.End) this.ops.end(op);
        if (endOp.duration() > SLOW_CACHING_THRESHOLD) {
          if (!this.cachingDisabled.getAndSet(true)) {
            String message = String.format(
                "Caching disabled because of slow operation (%.1f sec)", endOp.duration());
            LOG.warn(message);
          }
        }
      }
    }
  }

  // @VisibleForTesting
  protected BlockCache createCache() {
    return new SingleFilePerBlockCache();
  }

  // @VisibleForTesting
  protected void cachePut(int blockNumber, ByteBuffer buffer) throws IOException {
    if (this.closed) {
      return;
    }

    this.cache.put(blockNumber, buffer);
  }

  private static class CachePutTask extends ExceptionalFunction0<Void> {
    private final BufferData data;

    // Block being asynchronously fetched.
    private final Future<Void> blockFuture;

    // Block manager that manages this block.
    private final S3CachingBlockManager blockManager;

    public CachePutTask(
        BufferData data,
        Future<Void> blockFuture,
        S3CachingBlockManager blockManager) {
      this.data = data;
      this.blockFuture = blockFuture;
      this.blockManager = blockManager;
    }

    @Override
    public Void applyE() {
      this.blockManager.addToCacheAndRelease(this.data, this.blockFuture);
      return null;
    }
  }

  // Number of ByteBuffers available to be acquired.
  // @VisibleForTesting
  int numAvailable() {
    return this.bufferPool.numAvailable();
  }

  // Number of caching operations completed.
  // @VisibleForTesting
  int numCached() {
    return this.cache.size();
  }

  // @VisibleForTesting
  int numCachingErrors() {
    return this.numCachingErrors.get();
  }

  // @VisibleForTesting
  int numReadErrors() {
    return this.numReadErrors.get();
  }

  // @VisibleForTesting
  BufferData getData(int blockNumber) {
    return this.bufferPool.tryAcquire(blockNumber);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("cache(");
    sb.append(this.cache.toString());
    sb.append("); ");

    sb.append("pool: ");
    sb.append(this.bufferPool.toString());

    return sb.toString();
  }
}
