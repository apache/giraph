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

package org.apache.giraph.hive.input;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordReader;

import com.google.common.collect.AbstractIterator;

import java.io.IOException;

/**
 * Wraps {@link RecordReader} into {@link java.util.Iterator}
 *
 * @param <T> Data of record reader
 */
public class RecordReaderWrapper<T> extends AbstractIterator<T> {
  /** Wrapped {@link RecordReader} */
  private final RecordReader<WritableComparable, T> recordReader;

  /**
   * Constructor
   *
   * @param recordReader {@link RecordReader} to wrap
   */
  public RecordReaderWrapper(RecordReader<WritableComparable, T> recordReader) {
    this.recordReader = recordReader;
  }

  @Override
  protected T computeNext() {
    try {
      if (!recordReader.nextKeyValue()) {
        endOfData();
        return null;
      }
      return recordReader.getCurrentValue();
    } catch (IOException | InterruptedException e) {
      throw new IllegalStateException(
          "computeNext: Unexpected exception occurred", e);
    }
  }
}
