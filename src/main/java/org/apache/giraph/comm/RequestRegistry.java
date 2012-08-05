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

import java.util.EnumMap;
import java.util.Map;

/**
 * Registry of the requests that are supported.
 */
public class RequestRegistry {
  /** Mapping of enum type to class type */
  @SuppressWarnings("rawtypes")
  private final Map<Type, Class<? extends WritableRequest>> requestMap =
      new EnumMap<Type, Class<? extends WritableRequest>>(Type.class);
  /** If finalized, nothing can be added. */
  private boolean shutdown = false;

  /**
   * Type of the request
   */
  public enum Type {
    /** Sending vertices request */
    SEND_VERTEX_REQUEST,
    /** Sending a partition of messages for next superstep */
    SEND_PARTITION_MESSAGES_REQUEST,
    /**
     * Sending a partition of messages for current superstep
     * (used during partition exchange)
     */
    SEND_PARTITION_CURRENT_MESSAGES_REQUEST,
    /** Send a partition of mutations */
    SEND_PARTITION_MUTATIONS_REQUEST,
    /** Sending messages request */
    SEND_MESSAGES_REQUEST,
  }

  /**
   * Register a writable request by type and class.
   *
   * @param writableRequest Request to be registered.
   */
  public void registerClass(WritableRequest<?, ?, ?, ?> writableRequest) {
    if (shutdown) {
      throw new IllegalStateException(
          "registerClass: Cannot call this after shutting down!");
    }
    if (requestMap.put(writableRequest.getType(),
        writableRequest.getClass()) != null) {
      throw new IllegalArgumentException("registerClass: Class " +
          writableRequest.getClass() + " already exists!");
    }
  }

  /**
   * Get a class (must be finalized)
   *
   * @param type Type of the request to get
   * @return Class of the type
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends WritableRequest> getClass(Type type) {
    if (!shutdown) {
      throw new IllegalStateException(
          "getClass: Illegal to get class before finalized");
    }

    Class<? extends WritableRequest> writableRequestClass =
        requestMap.get(type);
    if (writableRequestClass == null) {
      throw new IllegalArgumentException(
          "getClass: Couldn't find type " + type);
    }

    return writableRequestClass;
  }

  /**
   * No more requests can be registered.
   */
  public void shutdown() {
    shutdown = true;
  }
}
