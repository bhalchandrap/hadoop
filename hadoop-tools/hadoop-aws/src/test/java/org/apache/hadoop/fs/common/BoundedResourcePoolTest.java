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

import static org.junit.Assert.*;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

public class BoundedResourcePoolTest {

  static class BufferPool extends BoundedResourcePool<ByteBuffer> {
    public BufferPool(int size) {
      super(size);
    }

    @Override
    protected ByteBuffer createNew() {
      return ByteBuffer.allocate(10);
    }
  }

  @Test
  public void testArgChecks() {

    // Should not throw.
    BufferPool pool = new BufferPool(5);

    // Verify it throws correctly.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'size' must be a positive integer",
        () -> new BufferPool(-1));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'size' must be a positive integer",
        () -> new BufferPool(0));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'item' must not be null",
        () -> pool.release(null));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "This item is not a part of this pool",
        () -> pool.release(ByteBuffer.allocate(4)));
  }

  @Test
  public void testAcquireReleaseSingle() {
    int NUM_BUFFERS = 5;
    BufferPool pool = new BufferPool(NUM_BUFFERS);

    assertEquals(0, pool.numCreated());
    assertEquals(NUM_BUFFERS, pool.numAvailable());

    ByteBuffer buffer1 = pool.acquire();
    assertNotNull(buffer1);
    assertEquals(1, pool.numCreated());
    assertEquals(NUM_BUFFERS - 1, pool.numAvailable());

    // Release and immediately reacquire => should not end up creating new buffer.
    pool.release(buffer1);
    assertEquals(1, pool.numCreated());

    ByteBuffer buffer2 = pool.acquire();
    assertNotNull(buffer2);
    assertSame(buffer1, buffer2);
    assertEquals(1, pool.numCreated());
  }

  @Test
  public void testAcquireReleaseMultiple() {
    int NUM_BUFFERS = 5;
    BufferPool pool = new BufferPool(NUM_BUFFERS);
    Set<ByteBuffer> buffers = Collections.newSetFromMap(new IdentityHashMap());

    assertEquals(0, pool.numCreated());

    // Acquire all one by one.
    for (int i = 0; i < NUM_BUFFERS; i++) {
      assertEquals(NUM_BUFFERS - i, pool.numAvailable());
      ByteBuffer buffer = pool.acquire();
      assertNotNull(buffer);
      assertFalse(buffers.contains(buffer));
      buffers.add(buffer);
      assertEquals(i + 1, pool.numCreated());
    }

    assertEquals(NUM_BUFFERS, pool.numCreated());
    assertEquals(0, pool.numAvailable());

    int releaseCount = 0;

    // Release all one by one.
    for (ByteBuffer buffer : buffers) {
      assertEquals(releaseCount, pool.numAvailable());
      releaseCount++;
      pool.release(buffer);
      assertEquals(releaseCount, pool.numAvailable());

      // Releasing the same buffer again should not have any ill effect.
      pool.release(buffer);
      assertEquals(releaseCount, pool.numAvailable());
      pool.release(buffer);
      assertEquals(releaseCount, pool.numAvailable());
    }

    // Acquire all one by one again to ensure that they are the same ones we got earlier.
    for (int i = 0; i < NUM_BUFFERS; i++) {
      ByteBuffer buffer = pool.acquire();
      assertTrue(buffers.contains(buffer));
    }

    assertEquals(NUM_BUFFERS, pool.numCreated());
    assertEquals(0, pool.numAvailable());
  }
}
