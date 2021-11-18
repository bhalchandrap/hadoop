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

import org.apache.hadoop.fs.s3e.ExceptionAsserts;

import org.junit.Test;

import java.lang.reflect.Method;

public class BlockOperationsTest {

  @Test
  public void testArgChecks() {
    // Should not throw.
    BlockOperations ops = new BlockOperations();

    // Verify it throws correctly.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> ops.getPrefetched(-1));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> ops.getCached(-1));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> ops.getRead(-1));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> ops.release(-1));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> ops.requestPrefetch(-1));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockNumber' must not be negative",
        () -> ops.requestCaching(-1));
  }

  @Test
  public void testGetSummary() throws Exception {
    verifySummary("getPrefetched", "GP");
    verifySummary("getCached", "GC");
    verifySummary("getRead", "GR");
    verifySummary("release", "RL");
    verifySummary("requestPrefetch", "RP");
    verifySummary("prefetch", "PF");
    verifySummary("requestCaching", "RC");
    verifySummary("addToCache", "C+");

    verifySummaryNoArg("cancelPrefetches", "CP");
    verifySummaryNoArg("close", "CX");
  }

  private void verifySummary(String methodName, String shortName) throws Exception {
    int blockNumber = 42;
    BlockOperations ops = new BlockOperations();
    Method method = ops.getClass().getDeclaredMethod(methodName, int.class);
    BlockOperations.Operation op = (BlockOperations.Operation) method.invoke(ops, blockNumber);
    ops.end(op);
    String summary = ops.getSummary(false);
    String opSummary = String.format("%s(%d)", shortName, blockNumber);
    String expectedSummary = String.format("%s;E%s;", opSummary, opSummary);
    assertTrue(summary.startsWith(expectedSummary));
  }

  private void verifySummaryNoArg(String methodName, String shortName) throws Exception {
    BlockOperations ops = new BlockOperations();
    Method method = ops.getClass().getDeclaredMethod(methodName);
    BlockOperations.Operation op = (BlockOperations.Operation) method.invoke(ops);
    ops.end(op);
    String summary = ops.getSummary(false);
    String expectedSummary = String.format("%s;E%s;", shortName, shortName);
    assertTrue(summary.startsWith(expectedSummary));
  }
}
