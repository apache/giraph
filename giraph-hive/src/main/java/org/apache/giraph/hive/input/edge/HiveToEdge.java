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

package org.apache.giraph.hive.input.edge;

import org.apache.giraph.hive.input.HiveInputChecker;
import org.apache.giraph.io.iterables.EdgeWithSource;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.record.HiveReadableRecord;

import java.util.Iterator;

/**
 * An interface used to create edges from Hive records.
 *
 * It gets initialized with HiveRecord iterator, and it needs to provide an
 * iterator over edges, so it's possible to skip some rows from the input,
 * combine several rows together, etc.
 *
 * @param <I> Vertex ID
 * @param <E> Edge Value
 */
public interface HiveToEdge<I extends WritableComparable,
    E extends Writable> extends Iterator<EdgeWithSource<I, E>>,
    HiveInputChecker {
  /**
   * Set the records which contain edge input data
   *
   * @param records Hive records
   */
  void initializeRecords(Iterator<HiveReadableRecord> records);
}
