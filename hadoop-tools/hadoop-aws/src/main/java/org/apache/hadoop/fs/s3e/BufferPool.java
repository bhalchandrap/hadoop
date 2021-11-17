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

import org.apache.hadoop.fs.s3e.Validate;

import com.twitter.util.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

/**
 * Manages a fixed pool of {@code ByteBuffer} instances.
 *
 * Avoids creating a new buffer if a previously created buffer is already available.
 */
public class BufferPool implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);

  // Max number of buffers in this pool.
  private final int size;

  // Size in bytes of each buffer.
  private final int bufferSize;

  // Invariants for internal state.
  // -- a buffer is either in this.pool or in this.allocated
  // -- transition between this.pool <==> this.allocated must be atomic
  // -- only one buffer allocated for a given blockNumber

  // Underlying bounded resource pool.
  private BoundedResourcePool<ByteBuffer> pool;

  // Allows associating metadata to each buffer in the pool.
  private Map<BufferData, ByteBuffer> allocated;

  public BufferPool(int size, int bufferSize) {
    Validate.checkPositiveInteger(size, "size");
    Validate.checkPositiveInteger(bufferSize, "bufferSize");

    this.size = size;
    this.bufferSize = bufferSize;
    this.allocated = new IdentityHashMap();
    this.pool = new BoundedResourcePool<ByteBuffer>(size) {
        @Override
        public ByteBuffer createNew() {
          return ByteBuffer.allocate(bufferSize);
        }
      };
  }

  public List<BufferData> getAll() {
    synchronized (this.allocated) {
      return Collections.unmodifiableList(new ArrayList(this.allocated.keySet()));
    }
  }

  /**
   * Acquires a {@code ByteBuffer}; blocking if necessary until one becomes available.
   */
  public synchronized BufferData acquire(int blockNumber) {
    BufferData data;
    final int maxRetryDelayMs = 600 * 1000;
    final int statusUpdateDelayMs = 120 * 1000;
    Retryer retryer = new Retryer(10, maxRetryDelayMs, statusUpdateDelayMs);

    do {
      if (retryer.updateStatus()) {
        LOG.warn("waiting to acquire block: {}", blockNumber);
        LOG.info("state = {}", this.toString());
        this.releaseReadyBlock(blockNumber);
      }
      data = this.tryAcquire(blockNumber);
    }
    while ((data == null) && retryer.continueRetry());

    if (data != null) {
      return data;
    } else {
      String message = String.format("Wait failed for acquire(%d)", blockNumber);
      throw new IllegalStateException(message);
    }
  }

  /**
   * Acquires a buffer if one is immediately available. Otherwise returns null.
   */
  public synchronized BufferData tryAcquire(int blockNumber) {
    return this.acquireHelper(blockNumber, false);
  }

  private synchronized BufferData acquireHelper(int blockNumber, boolean canBlock) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    this.releaseDoneBlocks();

    BufferData data = this.find(blockNumber);
    if (data != null) {
      return data;
    }

    ByteBuffer buffer = canBlock ? this.pool.acquire() : this.pool.tryAcquire();
    if (buffer == null) {
      return null;
    }

    buffer.clear();
    data = new BufferData(blockNumber, buffer.duplicate());

    synchronized (this.allocated) {
      Validate.checkState(this.find(blockNumber) == null, "buffer data already exists");

      this.allocated.put(data, buffer);
    }

    return data;
  }

  /**
   * Releases resources for any blocks marked as 'done'.
   */
  private synchronized void releaseDoneBlocks() {
    for (BufferData data : this.getAll()) {
      if (data.stateEqualsOneOf(BufferData.State.DONE)) {
        this.release(data);
      }
    }
  }

  /**
   * If no blocks were released after calling releaseDoneBlocks() a few times,
   * we may end up waiting forever. To avoid that situation, we try releasing
   * a 'ready' block farthest away from the given block.
   */
  private synchronized void releaseReadyBlock(int blockNumber) {
    BufferData releaseTarget = null;
    for (BufferData data : this.getAll()) {
      if (data.stateEqualsOneOf(BufferData.State.READY)) {
        if (releaseTarget == null) {
          releaseTarget = data;
        } else {
          if (distance(data, blockNumber) > distance(releaseTarget, blockNumber)) {
            releaseTarget = data;
          }
        }
      }
    }

    if (releaseTarget != null) {
      LOG.warn("releasing 'ready' block: {}", releaseTarget);
      releaseTarget.setDone();
    }
  }

  private int distance(BufferData data, int blockNumber) {
    return Math.abs(data.blockNumber - blockNumber);
  }

  /**
   * Releases a previously acquired resource.
   */
  public synchronized void release(BufferData data) {
    Validate.checkNotNull(data, "data");

    synchronized (data) {
      Validate.checkArgument(
          this.canRelease(data),
          String.format("Unable to release buffer: %s", data));

      ByteBuffer buffer = this.allocated.get(data);
      if (buffer == null) {
        // Likely released earlier.
        return;
      }
      buffer.clear();
      this.pool.release(buffer);
      this.allocated.remove(data);
    }

    this.releaseDoneBlocks();
  }

  @Override
  public synchronized void close() {
    for (BufferData data : this.getAll()) {
      Future<Void> actionFuture = data.getActionFuture();
      if (actionFuture != null) {
        actionFuture.raise(new CancellationException("BufferPool is closing."));
      }
    }

    this.pool.close();
    this.pool = null;

    this.allocated.clear();
    this.allocated = null;
  }

  // For debugging purposes.
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.pool.toString());
    sb.append("\n");
    List<BufferData> allData = new ArrayList<>(this.getAll());
    Collections.sort(allData, (d1, d2) -> d1.blockNumber - d2.blockNumber);
    for (BufferData data : allData) {
      sb.append(data.toString());
      sb.append("\n");
    }

    return sb.toString();
  }

  // Number of ByteBuffers created so far.
  // @VisibleForTesting
  synchronized int numCreated() {
    return this.pool.numCreated();
  }

  // Number of ByteBuffers available to be acquired.
  // @VisibleForTesting
  synchronized int numAvailable() {
    this.releaseDoneBlocks();
    return this.pool.numAvailable();
  }

  private BufferData find(int blockNumber) {
    synchronized (this.allocated) {
      for (BufferData data : this.allocated.keySet()) {
        if ((data.blockNumber == blockNumber) && !data.stateEqualsOneOf(BufferData.State.DONE)) {
          return data;
        }
      }
    }

    return null;
  }

  private boolean canRelease(BufferData data) {
    return data.stateEqualsOneOf(
        BufferData.State.DONE,
        BufferData.State.READY);
  }
}
