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
package org.apache.giraph.hive;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Helpers {
  public static Map<Integer, Double> parseIntDoubleResults(Iterable<String> results) {
    Map<Integer, Double> values = Maps.newHashMap();
    for (String line : results) {
      String[] tokens = line.split("\\s+");
      int id = Integer.valueOf(tokens[0]);
      double value = Double.valueOf(tokens[1]);
      values.put(id, value);
    }
    return values;
  }

  public static Map<Integer, Integer> parseIntIntResults(Iterable<String> results) {
    Map<Integer, Integer> values = Maps.newHashMap();
    for (String line : results) {
      String[] tokens = line.split("\\s+");
      int id = Integer.valueOf(tokens[0]);
      int value = Integer.valueOf(tokens[1]);
      values.put(id, value);
    }
    return values;
  }

  public static void silenceDataNucleusLogger() {
    Logger logger = Logger.getLogger("org.datanucleus");
    logger.setLevel(Level.INFO);
  }
}
