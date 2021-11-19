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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Block level operations performed on S3 file.
 * This class is meant to be used by {@code S3BlockManager}.
 * It is separated out in its own file due to its size.
 */
public class BlockOperations {
  private static final Logger LOG = LoggerFactory.getLogger(BlockOperations.class);

  public enum Kind {
    UNKNOWN("??", "unknown", false),
    CANCEL_PREFETCHES("CP", "cancelPrefetches", false),
    CLOSE("CX", "close", false),
    CACHE_PUT("C+", "putC", true),
    GET_CACHED("GC", "getCached", true),
    GET_PREFETCHED("GP", "getPrefetched", true),
    GET_READ("GR", "getRead", true),
    PREFETCH("PF", "prefetch", true),
    RELEASE("RL", "release", true),
    REQUEST_CACHING("RC", "requestCaching", true),
    REQUEST_PREFETCH("RP", "requestPrefetch", true);

    String shortName;
    String name;
    boolean hasBlock;

    Kind(String shortName, String name, boolean hasBlock) {
      this.shortName = shortName;
      this.name = name;
      this.hasBlock = hasBlock;
    }

    private static Map<String, Kind> shortNameToKind = new HashMap<>();

    public static Kind fromShortName(String shortName) {
      if (shortNameToKind.size() == 0) {
        for (Kind kind : Kind.values()) {
          shortNameToKind.put(kind.shortName, kind);
        }
      }
      return shortNameToKind.get(shortName);
    }
  }

  public static class Operation {
    public final Kind kind;
    public final int blockNumber;
    public final long timestamp;

    public Operation(Kind kind, int blockNumber) {
      this.kind = kind;
      this.blockNumber = blockNumber;
      this.timestamp = System.nanoTime();
    }

    public void getSummary(StringBuilder sb) {
      if (this.kind.hasBlock) {
        sb.append(String.format("%s(%d)", this.kind.shortName, this.blockNumber));
      } else {
        sb.append(String.format("%s", this.kind.shortName));
      }
    }

    public String getDebugInfo() {
      if (this.kind.hasBlock) {
        return String.format("--- %s(%d)", this.kind.name, this.blockNumber);
      } else {
        return String.format("... %s()", this.kind.name);
      }
    }
  }

  public static class End extends Operation {
    private Operation op;

    public End(Operation op) {
      super(op.kind, op.blockNumber);
      this.op = op;
    }

    @Override
    public void getSummary(StringBuilder sb) {
      sb.append("E");
      super.getSummary(sb);
    }

    @Override
    public String getDebugInfo() {
      return "***" + super.getDebugInfo().substring(3);
    }

    public double duration() {
      return (this.timestamp - this.op.timestamp) / 1e9;
    }
  }

  private ArrayList<Operation> ops;
  private boolean debugMode;

  public BlockOperations() {
    this.ops = new ArrayList<>();
  }

  public void setDebug(boolean state) {
    this.debugMode = state;
  }

  private synchronized Operation add(Operation op) {
    if (this.debugMode) {
      LOG.info(op.getDebugInfo());
    }
    ops.add(op);
    return op;
  }

  public Operation getPrefetched(int blockNumber) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    return this.add(new Operation(Kind.GET_PREFETCHED, blockNumber));
  }

  public Operation getCached(int blockNumber) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    return this.add(new Operation(Kind.GET_CACHED, blockNumber));
  }

  public Operation getRead(int blockNumber) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    return this.add(new Operation(Kind.GET_READ, blockNumber));
  }

  public Operation release(int blockNumber) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    return this.add(new Operation(Kind.RELEASE, blockNumber));
  }

  public Operation requestPrefetch(int blockNumber) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    return this.add(new Operation(Kind.REQUEST_PREFETCH, blockNumber));
  }

  public Operation prefetch(int blockNumber) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    return this.add(new Operation(Kind.PREFETCH, blockNumber));
  }

  public Operation cancelPrefetches() {
    return this.add(new Operation(Kind.CANCEL_PREFETCHES, -1));
  }

  public Operation close() {
    return this.add(new Operation(Kind.CLOSE, -1));
  }

  public Operation requestCaching(int blockNumber) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    return this.add(new Operation(Kind.REQUEST_CACHING, blockNumber));
  }

  public Operation addToCache(int blockNumber) {
    Validate.checkNotNegative(blockNumber, "blockNumber");

    return this.add(new Operation(Kind.CACHE_PUT, blockNumber));
  }

  public Operation end(Operation op) {
    return this.add(new End(op));
  }

  private static void append(StringBuilder sb, String format, Object... args) {
    sb.append(String.format(format, args));
  }

  public synchronized String getSummary(boolean showDebugInfo) {
    StringBuilder sb = new StringBuilder();
    for (Operation op : this.ops) {
      if (op != null) {
        if (showDebugInfo) {
          sb.append(op.getDebugInfo());
          sb.append("\n");
        } else {
          op.getSummary(sb);
          sb.append(";");
        }
      }
    }

    sb.append("\n");
    this.getDurationInfo(sb);

    return sb.toString();
  }

  public synchronized void getDurationInfo(StringBuilder sb) {
    Map<Kind, DoubleSummaryStatistics> durations = new HashMap<>();
    for (Operation op : this.ops) {
      if (op instanceof End) {
        End endOp = (End) op;
        DoubleSummaryStatistics stats = durations.get(endOp.kind);
        if (stats == null) {
          stats = new DoubleSummaryStatistics();
          durations.put(endOp.kind, stats);
        }
        stats.accept(endOp.duration());
      }
    }

    List<Kind> kinds = Arrays.asList(
        Kind.GET_CACHED,
        Kind.GET_PREFETCHED,
        Kind.GET_READ,
        Kind.CACHE_PUT,
        Kind.PREFETCH,
        Kind.REQUEST_CACHING,
        Kind.REQUEST_PREFETCH,
        Kind.CANCEL_PREFETCHES,
        Kind.RELEASE,
        Kind.CLOSE
    );

    for (Kind kind : kinds) {
      append(sb, "%-18s : ", kind);
      DoubleSummaryStatistics stats = durations.get(kind);
      if (stats == null) {
        append(sb, "--\n");
      } else {
        append(
            sb,
            "#ops = %3d, total = %5.1f, min: %3.1f, avg: %3.1f, max: %3.1f\n",
            stats.getCount(),
            stats.getSum(),
            stats.getMin(),
            stats.getAverage(),
            stats.getMax());
      }
    }
  }

  public synchronized void analyze(StringBuilder sb) {
    Map<Integer, List<Operation>> blockOps = new HashMap<>();

    // Group-by block number.
    for (Operation op : this.ops) {
      if (op.blockNumber < 0) {
        continue;
      }

      List<Operation> perBlockOps;
      if (!blockOps.containsKey(op.blockNumber)) {
        perBlockOps = new ArrayList<>();
        blockOps.put(op.blockNumber, perBlockOps);
      }

      perBlockOps = blockOps.get(op.blockNumber);
      perBlockOps.add(op);
    }

    List<Integer> prefetchedNotUsed = new ArrayList<>();
    List<Integer> cachedNotUsed = new ArrayList<>();

    for (Integer blockNumber : blockOps.keySet()) {
      List<Operation> perBlockOps = blockOps.get(blockNumber);
      Map<Kind, Integer> kindCounts = new HashMap<>();
      Map<Kind, Integer> endKindCounts = new HashMap<>();

      for (Operation op : perBlockOps) {
        if (op instanceof End) {
          int endCount = endKindCounts.getOrDefault(op.kind, 0) + 1;
          endKindCounts.put(op.kind, endCount);
        } else {
          int count = kindCounts.getOrDefault(op.kind, 0) + 1;
          kindCounts.put(op.kind, count);
        }
      }

      for (Kind kind : kindCounts.keySet()) {
        int count = kindCounts.getOrDefault(kind, 0);
        int endCount = endKindCounts.getOrDefault(kind, 0);
        if (count != endCount) {
          append(sb, "[%d] %s : #ops(%d) != #end-ops(%d)\n", blockNumber, kind, count, endCount);
        }

        if (count > 1) {
          append(sb, "[%d] %s = %d\n", blockNumber, kind, count);
        }
      }

      int prefetchCount = kindCounts.getOrDefault(Kind.PREFETCH, 0);
      int getPrefetchedCount = kindCounts.getOrDefault(Kind.GET_PREFETCHED, 0);
      if ((prefetchCount > 0) && (getPrefetchedCount < prefetchCount)) {
        prefetchedNotUsed.add(blockNumber);
      }

      int cacheCount = kindCounts.getOrDefault(Kind.CACHE_PUT, 0);
      int getCachedCount = kindCounts.getOrDefault(Kind.GET_CACHED, 0);
      if ((cacheCount > 0) && (getCachedCount < cacheCount)) {
        cachedNotUsed.add(blockNumber);
      }
    }

    if (prefetchedNotUsed.size() > 0) {
      append(sb, "Prefetched but not used: %s\n", getIntList(prefetchedNotUsed));
    }

    if (cachedNotUsed.size() > 0) {
      append(sb, "Cached but not used: %s\n", getIntList(cachedNotUsed));
    }
  }

  private static String getIntList(Iterable<Integer> nums) {
    List<String> numList = new ArrayList<>();
    for (Integer n : nums) {
      numList.add(n.toString());
    }
    return String.join(", ", numList);
  }

  public static BlockOperations fromSummary(String summary) {
    BlockOperations ops = new BlockOperations();
    ops.setDebug(true);
    Pattern blockOpPattern = Pattern.compile("([A-Z+]+)(\\(([0-9]+)?\\))?");
    String[] tokens = summary.split(";");
    for (String token : tokens) {
      Matcher matcher = blockOpPattern.matcher(token);
      if (!matcher.matches()) {
        String message = String.format("Unknown summary format: %s", token);
        throw new IllegalArgumentException(message);
      }

      String shortName = matcher.group(1);
      String blockNumberStr = matcher.group(3);
      int blockNumber = (blockNumberStr == null) ? -1 : Integer.valueOf(blockNumberStr);
      Kind kind = Kind.fromShortName(shortName);
      Kind endKind = null;
      if (kind == null) {
        if (shortName.charAt(0) == 'E') {
          endKind = Kind.fromShortName(shortName.substring(1));
        }
      }

      if (kind == null && endKind == null) {
        String message = String.format("Unknown short name: %s (token = %s)", shortName, token);
        throw new IllegalArgumentException(message);
      }

      if (kind != null) {
        ops.add(new Operation(kind, blockNumber));
      } else {
        Operation op = null;
        for (int i = ops.ops.size() - 1; i >= 0; i--) {
          op = ops.ops.get(i);
          if ((op.blockNumber == blockNumber) && (op.kind == endKind) && !(op instanceof End)) {
            ops.add(new End(op));
            break;
          }
        }

        if (op == null) {
          LOG.warn("Start op not found: %s(%d)", endKind, blockNumber);
        }
      }
    }

    return ops;
  }
}
