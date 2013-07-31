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
package org.apache.giraph.hive.values;

import org.apache.hadoop.io.Writable;

import com.facebook.hiveio.record.HiveReadableRecord;

/**
 * Interface for reading Vertex / Edge values from Hive records
 *
 * @param <T> Value type
 */
public interface HiveValueReader<T extends Writable> {
  /**
   * Read value from record
   *
   * @param value graph value to read into
   * @param record Hive record
   */
  void readFields(T value, HiveReadableRecord record);

  /**
   * Null implementation that return NullWritable
   *
   * @param <W> Writable type
   */
  public class Null<W extends Writable> implements HiveValueReader<W> {
    /** Singleton */
    private static final Null INSTANCE = new Null();

    /**
     * Get singleton
     *
     * @return singleton instance
     */
    public static Null get() {
      return INSTANCE;
    }

    @Override
    public void readFields(W value, HiveReadableRecord record) { }
  }
}
