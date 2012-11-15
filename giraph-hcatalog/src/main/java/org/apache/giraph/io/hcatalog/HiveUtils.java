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

package org.apache.giraph.io.hcatalog;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Utilities and helpers for working with Hive tables.
 */
public class HiveUtils {
  // TODO use Hive util class if this is already provided by it

  /**
   * Private constructor for helper class.
   */
  private HiveUtils() {
    // Do nothing.
  }

  /**
  * @param outputTablePartitionString table partition string
  * @return Map
  */
  public static Map<String, String> parsePartitionValues(
      String outputTablePartitionString) {
    if (outputTablePartitionString == null) {
      return null;
    }
    Splitter commaSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
    Splitter equalSplitter = Splitter.on('=').omitEmptyStrings().trimResults();
    Map<String, String> partitionValues = Maps.newHashMap();
    for (String keyValStr : commaSplitter.split(outputTablePartitionString)) {
      List<String> keyVal = Lists.newArrayList(equalSplitter.split(keyValStr));
      if (keyVal.size() != 2) {
        throw new IllegalArgumentException(
            "Unrecognized partition value format: " +
            outputTablePartitionString);
      }
      partitionValues.put(keyVal.get(0), keyVal.get(1));
    }
    return partitionValues;
  }
}
