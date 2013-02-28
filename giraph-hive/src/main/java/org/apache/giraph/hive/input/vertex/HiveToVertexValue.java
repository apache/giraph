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

package org.apache.giraph.hive.input.vertex;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.giraph.hive.HiveReadableRecord;

/**
 * Interface for creating vertices from a Hive record.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 */
public interface HiveToVertexValue<I extends WritableComparable,
    V extends Writable> {
  /**
   * Read the Vertex's ID from the HiveRecord given.
   *
   * @param record HiveRecord to read from.
   * @return Vertex ID
   */
  I getVertexId(HiveReadableRecord record);

  /**
   * Read the Vertex's Value from the HiveRecord given.
   *
   * @param record HiveRecord to read from.
   * @return Vertex Value
   */
  V getVertexValue(HiveReadableRecord record);
}
