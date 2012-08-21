/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.comm;

import com.google.common.collect.Maps;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Generate different request ids based on the address of the well known
 * port on the workers
 */
public class AddressRequestIdGenerator {
  /** Address request generator map */
  private final Map<InetSocketAddress, Long> addressRequestGeneratorMap =
      Maps.newHashMap();

  /**
   * Get the next request id for a given destination.  Not thread-safe.
   *
   * @param address Address of the worker (consistent during a superstep)
   * @return Valid request id
   */
  public Long getNextRequestId(InetSocketAddress address) {
    Long requestGenerator = addressRequestGeneratorMap.get(address);
    if (requestGenerator == null) {
      requestGenerator = Long.valueOf(0);
      if (addressRequestGeneratorMap.put(address, requestGenerator) != null) {
        throw new IllegalStateException("getNextRequestId: Illegal put for " +
            "address " + address);
      }
      return requestGenerator;
    }

    requestGenerator = requestGenerator + 1;
    addressRequestGeneratorMap.put(address, requestGenerator);
    return requestGenerator;
  }
}
