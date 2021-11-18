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

import org.apache.hadoop.fs.common.BlockData;
import org.apache.hadoop.fs.common.BufferData;
import org.apache.hadoop.fs.common.Validate;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Provides access to S3 file one block at a time.
 */
public class S3BlockManager implements Closeable {

  // Reader that reads from S3 file.
  protected S3Reader reader;

  // Information about each block of the S3 file.
  protected BlockData blockData;

  /**
   * Constructs an instance of {@code S3BlockManager}.
   *
   * @param reader a reader that reads from S3 file.
   * @param blockData information about each block of the S3 file.
   */
  public S3BlockManager(S3Reader reader, BlockData blockData) {
    Validate.checkNotNull(reader, "reader");
    Validate.checkNotNull(blockData, "blockData");

    this.reader = reader;
    this.blockData = blockData;
  }

  /**
   * Gets the block having the given {@code blockNumber}.
   * The entire block is read into memory and returned as a {@code BufferData}.
   * The blocks are treated as a limited resource and must be released when
   * one is done reading them.
   */
  public BufferData get(int blockNumber) throws IOException {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    int size = this.blockData.getSize(blockNumber);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    long startOffset = this.blockData.getStartOffset(blockNumber);
    this.reader.read(buffer, startOffset, size);
    buffer.flip();
    return new BufferData(blockNumber, buffer);
  }

  /**
   * Releases resources allocated to the given block.
   */
  public void release(BufferData data) {
    Validate.checkNotNull(data, "data");

    // Do nothing because we allocate a new buffer each time.
  }

  /**
   * Requests optional prefetching of the given block.
   */
  public void requestPrefetch(int blockNumber) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    // Do nothing because we do not support prefetches.
  }

  /**
   * Requests cancellation of any previously issued prefetch requests.
   */
  public void cancelPrefetches() {
    // Do nothing because we do not support prefetches.
  }

  /**
   * Requests that the given block should be copied to the cache. Optional operation.
   */
  public void requestCaching(BufferData data) {
    // Do nothing because we do not support caching.
  }

  @Override
  public void close() {
  }
}
