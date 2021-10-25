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

import org.apache.hadoop.fs.s3e.Clients;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.FuturePool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Provides S3 Enhanced (S3E) file system functionality.
 * The use of this implementation can be enabled in a Hadoop job by setting the value of
 * {@code fs.s3n.impl} to {@code com.pinterest.hadoop.fs.S3EFileSystem}.
 */
public class S3EFileSystem extends S3AFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(S3EFileSystem.class);

  public static final int MAJOR_VERSION = 1;
  public static final int MINOR_VERSION = 5;

  // If the default values are used, each file opened for reading will consume
  // 64 MB of heap space (8 blocks x 8 MB each).

  public static final String PREFETCH_BLOCK_SIZE_KEY = "fs.s3e.prefetch.block.size";
  public static final int PREFETCH_BLOCK_DEFAULT_SIZE = 8 * 1024 * 1024;

  public static final String PREFETCH_BLOCK_COUNT_KEY = "fs.s3e.prefetch.block.count";
  public static final int PREFETCH_BLOCK_DEFAULT_COUNT = 8;

  private AmazonS3 client;

  // S3 reads are prefetched asynchronously using this future pool.
  private FuturePool futurePool;
  private ExecutorService threadPool;

  // Size in bytes of a single prefetch block used by S3EInputStream.
  private int prefetchBlockSize;

  // Size of prefetch queue (in number of blocks) used by S3EInputStream.
  private int prefetchBlockCount;

  public S3EFileSystem() {
    super();
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    LOG.info("initializing version {}", getVersionString());
    this.client = Clients.getInstance().createDefaultRetryingClient();
    this.prefetchBlockSize =
        conf.getInt(PREFETCH_BLOCK_SIZE_KEY, PREFETCH_BLOCK_DEFAULT_SIZE);
    this.prefetchBlockCount =
        conf.getInt(PREFETCH_BLOCK_COUNT_KEY, PREFETCH_BLOCK_DEFAULT_COUNT);
    threadPool = Executors.newFixedThreadPool(2 * this.prefetchBlockCount);
    futurePool = new ExecutorServiceFuturePool(threadPool);
    LOG.info("initialization complete");
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    final FileStatus fileStatus = getFileStatus(f);
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException("Can't open " + f + " because it is a directory");
    }

    URI fUri = pathToUri(f);
    String bucket = fUri.getHost();
    String key = fUri.getPath().substring(1);

    long contentLength = fileStatus.getLen();

    S3EInputStream inputStream = new S3EInputStream(
        futurePool,
        prefetchBlockSize,
        prefetchBlockCount,
        bucket,
        key,
        contentLength,
        client,
        statistics);

    return new FSDataInputStream(inputStream);
  }

  @Override
  public void close() throws IOException {
    LOG.info("closing");
    super.close();
    futurePool = null;
    threadPool.shutdown();
    threadPool = null;
    LOG.info("closed");
  }

  public static String getVersionString() {
    return String.format("%d.%02d", MAJOR_VERSION, MINOR_VERSION);
  }

  private URI pathToUri(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(getWorkingDirectory(), path);
    }

    return path.toUri();
  }
}
