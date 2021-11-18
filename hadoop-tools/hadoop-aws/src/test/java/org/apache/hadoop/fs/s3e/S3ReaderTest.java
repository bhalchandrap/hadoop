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

import static org.junit.Assert.*;

import org.apache.hadoop.fs.common.ExceptionAsserts;

import com.amazonaws.services.s3.AmazonS3Client;
import org.junit.Test;

import java.nio.ByteBuffer;

public class S3ReaderTest {

  private final int FILE_SIZE = 9;
  private final int BUFFER_SIZE = 2;
  private S3File s3File = new TestS3File(FILE_SIZE, false);

  @Test
  public void testArgChecks() {
    // Should not throw.
    S3Reader reader = new S3Reader(s3File);

    // Verify it throws correctly.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'s3File' must not be null",
        () -> new S3Reader(null));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'buffer' must not be null",
        () -> reader.read(null, 10, 2));

    ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'offset' (-1) must be within the range [0, 9]",
        () -> reader.read(buffer, -1, 2));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'offset' (11) must be within the range [0, 9]",
        () -> reader.read(buffer, 11, 2));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'size' must be a positive integer",
        () -> reader.read(buffer, 1, 0));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'size' must be a positive integer",
        () -> reader.read(buffer, 1, -1));
  }

  @Test
  public void testGetWithOffset() throws Exception {
    for (int i = 0; i < FILE_SIZE; i++) {
      testGetHelper(false, i);  // no retry
      testGetHelper(true, i);   // with retry
    }
  }

  private void testGetHelper(boolean testWithRetry, long startOffset)
      throws Exception {
    int numBlocks = 0;
    ByteBuffer buffer;
    TestS3File s3File = new TestS3File(FILE_SIZE, testWithRetry);
    S3Reader reader = new S3Reader(s3File);
    int remainingSize = FILE_SIZE - (int) startOffset;
    for (int bufferSize = 0; bufferSize <= FILE_SIZE + 1; bufferSize++) {
      buffer = ByteBuffer.allocate(bufferSize);
      for (int readSize = 1; readSize <= FILE_SIZE; readSize++) {
        buffer.clear();
        int numBytesRead = reader.read(buffer, startOffset, readSize);
        int expectedNumBytesRead = Math.min(readSize, remainingSize);
        expectedNumBytesRead = Math.min(bufferSize, expectedNumBytesRead);
        assertEquals(expectedNumBytesRead, numBytesRead);

        byte[] bytes = buffer.array();
        for (int i = 0; i< expectedNumBytesRead; i++) {
          assertEquals(startOffset + i, bytes[i]);
        }
      }
    }
  }
}
