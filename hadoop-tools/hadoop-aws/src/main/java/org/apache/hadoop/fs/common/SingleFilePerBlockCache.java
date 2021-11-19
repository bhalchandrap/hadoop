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

package org.apache.hadoop.fs.common;

import org.apache.hadoop.fs.common.Validate;

import com.twitter.util.ExceptionalFunction0;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.IllegalStateException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides functionality necessary for caching blocks of data read from FileSystem.
 * Each cache block is stored on the local disk as a separate file.
 */
public class SingleFilePerBlockCache implements BlockCache {
  private static final Logger LOG = LoggerFactory.getLogger(SingleFilePerBlockCache.class);

  // Blocks stored in this cache.
  private Map<Integer, Entry> blocks = new ConcurrentHashMap<>();

  // Number of times a block was read from this cache.
  // Used for determining cache utilization factor.
  private int numGets = 0;

  private boolean closed;

  // Cache entry.
  // Each block is stored as a separate file.
  private static class Entry {
    public final int blockNumber;
    public final Path path;
    public final int size;
    public final long checksum;

    public Entry(int blockNumber, Path path, int size, long checksum) {
      this.blockNumber = blockNumber;
      this.path = path;
      this.size = size;
      this.checksum = checksum;
    }

    @Override
    public String toString() {
      return String.format(
          "([%03d] %s: size = %d, checksum = %d)",
          this.blockNumber, this.path, this.size, this.checksum);
    }
  }

  public SingleFilePerBlockCache() {
  }

  /**
   * Indicates whether the given block is in this cache.
   */
  @Override
  public boolean containsBlock(Integer blockNumber) {
    Validate.checkNotNull(blockNumber, "blockNumber");

    return this.blocks.containsKey(blockNumber);
  }

  /**
   * Gets the blocks in this cache.
   */
  @Override
  public Iterable<Integer> blocks() {
    List<Integer> blocksList = Collections.unmodifiableList(new ArrayList<Integer>(this.blocks.keySet()));
    return blocksList;
  }

  /**
   * Gets the number of blocks in this cache.
   */
  @Override
  public int size() {
    return this.blocks.size();
  }

  /**
   * Gets the block having the given {@code blockNumber}.
   */
  @Override
  public void get(Integer blockNumber, ByteBuffer buffer) throws IOException {
    if (this.closed) {
      return;
    }

    Validate.checkNotNull(buffer, "buffer");

    Entry entry = this.getEntry(blockNumber);
    buffer.clear();
    this.readFile(entry.path, buffer);
    buffer.rewind();

    validateEntry(entry, buffer);
  }

  protected int readFile(Path path, ByteBuffer buffer) throws IOException {
    int numBytesRead = 0;
    int numBytes;
    FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
    while ((numBytes = channel.read(buffer)) > 0) {
      numBytesRead += numBytes;
    }
    buffer.limit(buffer.position());
    channel.close();
    return numBytesRead;
  }

  private Entry getEntry(Integer blockNumber) {
    Validate.checkNotNull(blockNumber, "blockNumber");
    Validate.checkNotNegative(blockNumber, "blockNumber");

    Entry entry = this.blocks.get(blockNumber);
    if (entry == null) {
      throw new IllegalStateException(String.format("block %d not found in cache", blockNumber));
    }
    this.numGets++;
    return entry;
  }

  /**
   * Puts the given block in this cache.
   */
  @Override
  public void put(Integer blockNumber, ByteBuffer buffer) throws IOException {
    if (this.closed) {
      return;
    }

    Validate.checkNotNull(blockNumber, "blockNumber");
    Validate.checkNotNull(buffer, "buffer");

    if (this.blocks.containsKey(blockNumber)) {
      Entry entry = this.blocks.get(blockNumber);
      validateEntry(entry, buffer);
      return;
    }

    Validate.checkPositiveInteger(buffer.limit(), "buffer.limit()");

    Path blockFilePath = getCacheFilePath();
    long size = Files.size(blockFilePath);
    if (size != 0) {
      String message =
          String.format("[%d] temp file already has data. %s (%d)",
          blockNumber, blockFilePath, size);
      throw new IllegalStateException(message);
    }

    this.writeFile(blockFilePath, buffer);
    long checksum = BufferData.getChecksum(buffer);
    Entry entry = new Entry(blockNumber, blockFilePath, buffer.limit(), checksum);
    this.blocks.put(blockNumber, entry);
  }

  private static final Set<? extends OpenOption> CREATE_OPTIONS =
      EnumSet.of(StandardOpenOption.WRITE,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING);

  protected void writeFile(Path path, ByteBuffer buffer) throws IOException {
    buffer.rewind();
    WritableByteChannel writeChannel = Files.newByteChannel(path, CREATE_OPTIONS);
    while (buffer.hasRemaining()) {
      writeChannel.write(buffer);
    }
    writeChannel.close();
  }

  protected Path getCacheFilePath() throws IOException {
    return getTempFilePath();
  }

  @Override
  public void close() throws IOException {
    if (this.closed) {
      return;
    }

    this.closed = true;

    LOG.info(this.getStats());
    int numFilesDeleted = 0;

    for (Entry entry : this.blocks.values()) {
      try {
        Files.deleteIfExists(entry.path);
        numFilesDeleted++;
      } catch (IOException e) {
        // Ignore while closing so that we can delete as many cache files as possible.
      }
    }

    if (numFilesDeleted > 0) {
      LOG.info("Deleted {} cache files", numFilesDeleted);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("stats: ");
    sb.append(getStats());
    sb.append(", blocks:[");
    sb.append(this.getIntList(this.blocks()));
    sb.append("]");
    return sb.toString();
  }

  private void validateEntry(Entry entry, ByteBuffer buffer) {
    if (entry.size != buffer.limit()) {
      String message = String.format(
          "[%d] entry.size(%d) != buffer.limit(%d)",
          entry.blockNumber, entry.size, buffer.limit());
      throw new IllegalStateException(message);
    }

    long checksum = BufferData.getChecksum(buffer);
    if (entry.checksum != checksum) {
      String message = String.format(
          "[%d] entry.checksum(%d) != buffer checksum(%d)",
          entry.blockNumber, entry.checksum, checksum);
      throw new IllegalStateException(message);
    }
  }

  private String getIntList(Iterable<Integer> nums) {
    List<String> numList = new ArrayList<>();
    List<Integer> numbers = new ArrayList<Integer>();
    for (Integer n : nums) {
      numbers.add(n);
    }
    Collections.sort(numbers);

    int index = 0;
    while (index < numbers.size()) {
      int start = numbers.get(index);
      int prev = start;
      int end = start;
      while ((++index < numbers.size()) && ((end = numbers.get(index)) == prev + 1)) {
        prev = end;
      }

      if (start == prev) {
        numList.add(Integer.toString(start));
      } else {
        numList.add(String.format("%d~%d", start, prev));
      }
    }

    return String.join(", ", numList);
  }

  private String getStats() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(
        "#entries = %d, #gets = %d",
        this.blocks.size(), this.numGets));
    return sb.toString();
  }

  private static final String CACHE_FILE_PREFIX = "fs-cache-";

  public static boolean isCacheSpaceAvailable(long fileSize) {
    try {
      Path cacheFilePath = getTempFilePath();
      long freeSpace = new File(cacheFilePath.toString()).getUsableSpace();
      LOG.info("fileSize = {}, freeSpace = {}", fileSize, freeSpace);
      Files.deleteIfExists(cacheFilePath);
      return fileSize < freeSpace;
    } catch (IOException e) {
      LOG.error("isCacheSpaceAvailable", e);
      return false;
    }
  }

  // The suffix (file extension) of each serialized index file.
  private static final String BINARY_FILE_SUFFIX = ".bin";

  // File attributes attached to any intermediate temporary file created during index creation.
  private static final FileAttribute<Set<PosixFilePermission>> TEMP_FILE_ATTRS =
      PosixFilePermissions.asFileAttribute(EnumSet.of(PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE));

  private static Path getTempFilePath() throws IOException {
    return Files.createTempFile(
        CACHE_FILE_PREFIX,
        BINARY_FILE_SUFFIX,
        TEMP_FILE_ATTRS
    );
  }
}
