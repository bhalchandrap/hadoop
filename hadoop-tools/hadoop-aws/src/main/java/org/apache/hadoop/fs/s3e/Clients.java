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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.twitter.util.ExceptionalFunction0;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Provides a way to create S3 clients with predefined configuration.
 */
public final class Clients {
  private static final Logger LOG = LoggerFactory.getLogger(Clients.class);

  private Clients() {}

  private static Clients instance;

  public static synchronized Clients getInstance() {
    if (instance == null) {
      instance = new Clients();
    }

    return instance;
  }

  // Given that rate limiting error counter resets every second, we retry only once
  // during the current second. Otherwise, we try again with exponential increase
  // until we reach the retry delay limit.
  public static final int RETRY_BASE_DELAY_MS = 500;
  public static final int RETRY_MAX_BACKOFF_TIME_MS = 5000;

  public ClientConfiguration createRetryingClientConfig() {
    // Checks for various error conditions in the following order:
    // -- Retry on client exceptions caused by IOException;
    // -- Retry on service exceptions that are:
    //    -- 500 internal server errors,
    //    -- 503 service unavailable errors,
    //    -- service throttling errors or
    //    -- clock skew errors.
    RetryPolicy.RetryCondition retryCondition =
        new PredefinedRetryPolicies.SDKDefaultRetryCondition();

    RetryPolicy.BackoffStrategy backoffStrategy =
        new PredefinedBackoffStrategies.ExponentialBackoffStrategy(
            RETRY_BASE_DELAY_MS,
            RETRY_MAX_BACKOFF_TIME_MS);

    RetryPolicy retryPolicy = new RetryPolicy(
        retryCondition,
        backoffStrategy,
        5,                      // maxErrorRetry
        false);                 // honorMaxErrorRetryInClientConfig

    return PredefinedClientConfigurations
        .defaultConfig()
        .withRetryPolicy(retryPolicy)
        .withThrottledRetries(false);
  }

  public AmazonS3 createDefaultRetryingClient() {
    return AmazonS3ClientBuilder
        .standard()
        .withClientConfiguration(createRetryingClientConfig())
        .build();
  }
}
