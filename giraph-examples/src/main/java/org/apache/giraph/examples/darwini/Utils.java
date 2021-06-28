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
package org.apache.giraph.examples.darwini;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.Arrays;

/**
 * Util class to compute super-community information
 * for each vertex.
 */
public class Utils {

  /**
   * No public constructor.
   */
  private Utils() {
  }

  /**
   * Returns boundaries of super communities.
   * @param conf job configuration.
   * @return array with boundaries
   */
  public static long[] getBoundaries(
      ImmutableClassesGiraphConfiguration<LongWritable,
          VertexData, NullWritable> conf) {
    String[] bds = Constants.BOUNDARIES.get(conf).split(",");
    long[] res = new long[bds.length];
    for (int i = 0; i < bds.length; i++) {
      res[i] = Long.parseLong(bds[i]);
    }
    return res;
  }

  /**
   * Returns super-community id for specified vertex id.
   * @param id vertex id
   * @param boundaries array of boundaries
   * @return super community id.
   */
  public static int superCommunity(long id, long[] boundaries) {
    int country = Arrays.binarySearch(boundaries, id);
    if (country >= 0) {
      country++;
    } else {
      country = - country - 1;
    }
    return country;
  }

}
