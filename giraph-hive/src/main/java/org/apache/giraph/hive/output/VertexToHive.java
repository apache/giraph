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

package org.apache.giraph.hive.output;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.giraph.hive.record.HiveRecord;

import java.io.IOException;

/**
 * Interface for writing vertices to Hive.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public interface VertexToHive<I extends WritableComparable, V extends Writable,
    E extends Writable> {
  /**
   * Save vertex to the output. One vertex can be stored to multiple rows in
   * the output.
   *
   * Record you get here is reusable, and the protocol to follow is:
   * - fill the reusableRecord with your data
   * - call recordSaver.save(reusableRecord)
   * - repeat
   * If you don't call save() at all then there won't be any output for this
   * vertex.
   *
   * @param vertex Vertex which we want to save.
   * @param reusableRecord Record to use for writing data to it.
   * @param recordSaver Saver of records
   */
  void saveVertex(Vertex<I, V, E, ?> vertex, HiveRecord reusableRecord,
      HiveRecordSaver recordSaver) throws IOException, InterruptedException;
}
