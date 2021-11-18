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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.Test;

public class ClientsTest {
  @Test
  public void testCreateRetryingClientConfig() {
    ClientConfiguration config = Clients.getInstance().createRetryingClientConfig();
    assertNotNull(config);

    RetryPolicy retryPolicy = config.getRetryPolicy();
    assertNotNull(retryPolicy);

    RetryPolicy.RetryCondition retryCondition = retryPolicy.getRetryCondition();
    assertNotNull(retryCondition);

    assertTrue(retryCondition instanceof PredefinedRetryPolicies.SDKDefaultRetryCondition);

    RetryPolicy.BackoffStrategy backoffStrategy = retryPolicy.getBackoffStrategy();
    assertNotNull(backoffStrategy);

    assertTrue(backoffStrategy instanceof PredefinedBackoffStrategies.ExponentialBackoffStrategy);

    assertEquals(5, retryPolicy.getMaxErrorRetry());
    assertEquals(false, retryPolicy.isMaxErrorRetryInClientConfigHonored());

  }
  @Test
  public void testCreateDefaultRetryingClient() {
    assertNotNull(Clients.getInstance().createDefaultRetryingClient());
  }
}
