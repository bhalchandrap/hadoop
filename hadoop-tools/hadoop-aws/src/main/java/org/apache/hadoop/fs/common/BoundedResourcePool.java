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

import java.io.Closeable;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Manages a fixed pool of resources.
 *
 * Avoids creating a new resource if a previously created instance is already available.
 */
public abstract class BoundedResourcePool<T> extends ResourcePool<T> {
  // The size of this pool. Fixed at creation time.
  protected int size;

  // Items currently available in the pool.
  protected ArrayBlockingQueue<T> items;

  // Items that have been created so far (regardless of whether they are currently available).
  protected Set<T> createdItems;

  /**
   * Constructs a resource pool of the given size.
   *
   * @param size the size of this pool. Cannot be changed post creation.
   */
  public BoundedResourcePool(int size) {
    Validate.checkPositiveInteger(size, "size");

    this.size = size;
    this.items = new ArrayBlockingQueue<T>(size);

    // The created items are identified based on their object reference.
    this.createdItems = Collections.newSetFromMap(new IdentityHashMap());
  }

  /**
   * Acquires a resource blocking if necessary until one becomes available.
   */
  @Override
  public T acquire() {
    return this.acquireHelper(true);
  }

  /**
   * Acquires a resource blocking if one is immediately available. Otherwise returns null.
   */
  @Override
  public T tryAcquire() {
    return this.acquireHelper(false);
  }

  /**
   * Releases a previously acquired resource.
   */
  @Override
  public void release(T item) {
    Validate.checkNotNull(item, "item");

    synchronized (this.createdItems) {
      if (!this.createdItems.contains(item)) {
        throw new IllegalArgumentException("This item is not a part of this pool");
      }
    }

    // Return if this item was released earlier.
    // We cannot use this.items.contains() because that check is not based on reference equality.
    for (T entry : this.items) {
      if (entry == item) {
        return;
      }
    }

    while (true) {
      try {
        this.items.put(item);
        return;
      } catch (InterruptedException e) {
        throw new IllegalStateException("release() should never block");
      }
    }
  }

  @Override
  public synchronized void close() {
    for (T item : this.createdItems) {
      this.close(item);
    }

    this.items.clear();
    this.items = null;

    this.createdItems.clear();
    this.createdItems = null;
  }

  /**
   * Derived classes may implement a way to cleanup each item.
   */
  @Override
  protected synchronized void close(T item) {
    // Do nothing in this class. Allow overriding classes to take any cleanup action.
  }

  // Number of items created so far. Mostly for testing purposes.
  public int numCreated() {
    synchronized (this.createdItems) {
      return this.createdItems.size();
    }
  }

  // Number of items available to be acquired. Mostly for testing purposes.
  public synchronized int numAvailable() {
    return (this.size - this.numCreated()) + this.items.size();
  }

  // For debugging purposes.
  @Override
  public synchronized String toString() {
    return String.format(
        "size = %d, #created = %d, #in-queue = %d, #available = %d",
        this.size, this.numCreated(), this.items.size(), this.numAvailable());
  }

  /**
   * Derived classes must implement a way to create an instance of a resource.
   */
  protected abstract T createNew();

  private T acquireHelper(boolean canBlock) {

    // Prefer reusing an item if one is available.
    // That avoids unnecessarily creating new instances.
    T result = this.items.poll();
    if (result != null) {
      return result;
    }

    synchronized (this.createdItems) {
      // Create a new instance if allowed by the capacity of this pool.
      if (this.createdItems.size() < this.size) {
        T item = this.createNew();
        this.createdItems.add(item);
        return item;
      }
    }

    if (canBlock) {
      try {
        // Block for an instance to be available.
        return this.items.take();
      } catch (InterruptedException e) {
        return null;
      }
    } else {
      return null;
    }
  }
}
