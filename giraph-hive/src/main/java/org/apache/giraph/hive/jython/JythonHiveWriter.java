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
package org.apache.giraph.hive.jython;

import org.apache.giraph.hive.column.HiveWritableColumn;
import org.apache.hadoop.io.Writable;

/**
 * Interface for writing user graph types to Hive columns
 *
 * @param <W> writable value type
 */
public interface JythonHiveWriter<W extends Writable> {
  /**
   * Write object's data to Hive
   *
   * @param value object to write
   * @param column Hive column to write to
   */
  void writeToHive(W value, HiveWritableColumn column);
}
