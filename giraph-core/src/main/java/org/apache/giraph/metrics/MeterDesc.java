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

package org.apache.giraph.metrics;

import java.util.concurrent.TimeUnit;

/**
 * Description for Meters used in Giraph.
 */
public enum MeterDesc {
  /** Number of requests received */
  RECEIVED_REQUESTS("requests-received", "requests", TimeUnit.SECONDS),
  /** Number of requests sent */
  SENT_REQUESTS("requests-sent", "requests", TimeUnit.SECONDS),
  /** Total edges loaded */
  EDGES_LOADED("edges-loaded", "edges", TimeUnit.SECONDS),
  /** Total vertices loaded */
  VERTICES_LOADED("vertices-loaded", "vertices", TimeUnit.SECONDS);

  /** Name of meter */
  private final String name;
  /** Type this meter tracks */
  private final String type;
  /** TimeUnit this meter tracks in */
  private final TimeUnit timeUnit;

  /**
   * Constructor
   * @param name String name of meter
   * @param type String type of meter
   * @param timeUnit TimeUnit meter tracks
   */
  private MeterDesc(String name, String type, TimeUnit timeUnit) {
    this.name = name;
    this.type = type;
    this.timeUnit = timeUnit;
  }

  /**
   * Get name of meter
   * @return String name
   */
  public String getName() {
    return name;
  }

  /**
   * Get TimeUnit of meter
   * @return TimeUnit
   */
  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  /**
   * Get type of meter
   * @return String type
   */
  public String getType() {
    return type;
  }
}
