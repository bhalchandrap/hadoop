# High Performance S3 InputStream

## Overview
This document describes how S3EInputStream works. There are two main goals of this document:
1. Make it easier to understand the current implementation in this folder.
1. Be useful in applying the underlying technique to other Hadoop file systems.

This document is divided into three high level areas. The first part explains important high level concepts. The second part describes components involved in the implementation in this folder. The components are further divided into two sub sections: those independent of S3 and those that are specific to S3. Finally, the last section brings everything together by describing how the components interact with one another to support sequential and random reads.

Moreover, this blog provides some performance numbers and addtional high level information: https://medium.com/pinterest-engineering/improving-efficiency-and-reducing-runtime-using-s3-read-optimization-b31da4b60fa0

## Motivation

The main motivation behind implementing this feature is to improve performance when reading data stored on S3. This situation arises often when big data stored on S3 is to be processed by a job. Many data processing jobs spend most of their time waiting for data to arrive. If we can increase the speed at which a job reads data, the job will finish sooner and save us considerable time and money in the process. Given that processing is costly, these savings can add up quickly to a substantial amount.

## Basic Concepts

File
: A binary blob of data stored on some storage device.

- **Split** : A contiguous portion of a file. Each file is made up of one or more splits. Each split is a contiguous blob of data processed by a single job.
- **Block** : Similar to a split but much smaller. Each split is made up of a number of blocks. The size of the first n-1 blocks is same. The size of the last block may be same or smaller.
- **Block based reading** : The granularity of read is one block. That is, we read an entire block and return or none at all. Multiple blocks may be read in parallel.
- **Position within a file** : The current read position in a split is a 0 based offset relative to the start of the split. Given that the first n-1 blocks of a split are guaranteed to be of the same size, the current position can also be denoted as the 0 based number of the current block and the 0 based offset within the current block.

## Components

This section lists the components in this implementation.

### Components independent of a file system.

The following components are not really tied to S3 file system. They can be used with any other underlying storage.

#### Block related functionality

- `BlockData.java` : Holds metadata about blocks of data in a file. For example, number of blocks in a given file, the start offset of each block, etc. Also holds the state of the block at a glance (whether it is yet to be read, already cached locally, etc).
- `BufferData.java` : Holds the state of a ByteBuffer that is currently in use. Each such buffer holds exactly one block in memory. The maximum number of such buffers and the size of each buffer is fixed during runtime (but configurable at start).
- `FilePosition.java` : Functionality related to tracking the position within a file (as relative offset from current block start).
- `S3BlockCache.java` : Despite the S3 in its name, this is simply an interface for a cache of blocks.
- `S3FilePerBlockCache.java` : Despite the S3 in its name, this class implements local disk based block cache.


#### Resource management
- `ResourcePool.java` : Abstracat base class for a pool of resources of generic type T
- `BoundedResourcePool.java` : Abstracat base class for a fixed sized pool of resources of generic type T. A fixed sized pool allows reuse of resources instead of having to create new resource each time.
- `BufferPool.java` : Fixed sized pool of ByteBuffer instances. This pool holds the prefetched blocks of data in memory.

#### Misc functionality not specific to a file system
- `Retryer.java` : Helper for implementing a max delay fixed interval retrying. Used by BufferPool and S3CachingBlockManager to retry certain operations.

### Supporting functionality (not really a part of the main feature)

- README.md : This file.
- `BlockOperations.java` : A helper for tracking and logging block level operations on a file. Mostly used for debugging. Can also be used (in future) for keep statistic of block accesses.
- `Clients.java` : Provides a way to create S3 clients with predefined configuration.
- `Io.java` : Provides misc functionality related to IO.
- `Validate.java` : A superset of Validate class in Apache commons lang3.


### Components specific to S3 file system.

- `S3AccessRetryer.java` : Provides retry related functionality when accessing S3.
- `S3CachingInputStream.java` : Heavily relies on S3CachingBlockManager to provide the desired functionality. That said, the decision on when to prefetch and when to cache is handled by this class.
- `S3EFileSystem.java` : Very thin wrapper over S3AFileSystem just enough to handle S3E specific configuration and overriding of initialize() and open() methods.
- `S3EInputStream.java` : Chooses between S3InMemoryInputStream and S3CachingInputStream depending upon a configurable threshold. That is, any file smaller than the threshold is fully read in-memory.
- `S3File.java` : Ecapsulates low level interactions with S3 object on AWS.
- `S3InputStream.java` : base class of S3EInputStream. could be merged into S3EInputStream. I had originally created to compare many different implementations.
- `S3Reader.java` : Provides functionality to read S3 file one block at a time.

Although the following files are currently S3 specific, they can be easily made S3 independent by passing in a reader.

- `S3BlockManager.java` : A naive implementation of a block manager that provides no prefetching or caching. Useful for comparing performance difference between this baseline and S3CachingBlockManager.
- `S3InMemoryInputStream.java` : reads entire file in memory and returns an InputStream over it.
- `S3CachingBlockManager.java` : The core functionality of this feature. This class provides prefetched + local cached access to an S3 file.

## Operation

### Sequential reads
