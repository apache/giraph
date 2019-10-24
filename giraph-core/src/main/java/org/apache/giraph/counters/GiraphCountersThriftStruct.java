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

package org.apache.giraph.counters;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import java.util.HashMap;
import java.util.Map;

/**
 * Stores the information about the counter names and values
 */
@ThriftStruct
public final class GiraphCountersThriftStruct {
  /** Singleton instance for everyone to use */
  private static final GiraphCountersThriftStruct INSTANCE =
          new GiraphCountersThriftStruct();

  /** Map of counter names and values */
  private Map<String, Map<String, Long>> counters = new HashMap<>();

  /**
   * Public constructor for thrift to create us.
   * Please use GiraphCountersThriftStruct.get() to get the static instance.
   */
  public GiraphCountersThriftStruct() {
  }

  /**
   * Get singleton instance of GiraphCountersThriftStruct.
   *
   * @return GiraphCountersThriftStruct singleton instance
   */
  public static GiraphCountersThriftStruct get() {
    return INSTANCE;
  }

  @ThriftField(1)
  public Map<String, Map<String, Long>> getCounters() {
    return counters;
  }

  @ThriftField
  public void setCounters(Map<String, Map<String, Long>> counters) {
    this.counters = counters;
  }
}
