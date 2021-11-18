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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

class TestS3File extends S3File {
  private int size;
  private byte[] contents;

  // If true, throws IOException on open request just once.
  // That allows test code to validate behavior related to retries.
  private boolean throwExceptionOnOpen;

  public TestS3File(int size) {
    this(size, false);
  }

  public TestS3File(int size, boolean throwExceptionOnOpen) {
    super(Clients.getInstance().createDefaultRetryingClient(), "bucket", "key", size);

    this.size = size;
    this.throwExceptionOnOpen = throwExceptionOnOpen;
    this.contents = new byte[size];
    for (int b = 0; b < size; b++) {
      this.contents[b] = byteAtOffset(b);
    }
  }

  @Override
  public long size() throws IOException {
    return this.size;
  }

  @Override
  public InputStream openForRead(long offset, int size) throws IOException {
    Validate.checkLessOrEqual(offset, "offset", size(), "size()");
    Validate.checkLessOrEqual(size, "size", size() - offset, "size() - offset");

    if (this.throwExceptionOnOpen) {
      this.throwExceptionOnOpen = false;
      throw new IOException("Throwing because throwExceptionOnOpen is true ");
    }
    int bufSize = (int) Math.min(size, size() - offset);
    return new ByteArrayInputStream(contents, (int) offset, bufSize);
  }

  @Override
  public void close(InputStream inputStream) {
    // do nothing since we do not use a real S3 stream.
  }

  public static byte byteAtOffset(int offset) {
    return (byte) (offset % 128);
  }
}
