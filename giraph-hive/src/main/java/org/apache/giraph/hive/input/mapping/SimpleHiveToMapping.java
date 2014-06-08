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

package org.apache.giraph.hive.input.mapping;

import com.facebook.hiveio.record.HiveReadableRecord;
import org.apache.giraph.mapping.MappingEntry;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Iterator;

/**
 * SimpleHiveToMapping - convenient class for HiveToMapping
 *
 * @param <I> vertexId type
 * @param <B> mappingTarget type
 */
@SuppressWarnings("unchecked")
public abstract class SimpleHiveToMapping<I extends WritableComparable,
  B extends Writable> extends AbstractHiveToMapping<I, B> {
  /** Hive records which we are reading from */
  private Iterator<HiveReadableRecord> records;

  /** Reusable entry object */
  private  MappingEntry<I, B> reusableEntry;

  /** Reusable vertex id */
  private I reusableVertexId;
  /** Reusable mapping target */
  private B reusableMappingTarget;

  /**
   * Read vertexId from hive record
   *
   * @param record HiveReadableRecord
   * @return vertexId
   */
  public abstract I getVertexId(HiveReadableRecord record);

  /**
   * Read mappingTarget from hive record
   *
   * @param record HiveReadableRecord
   * @return mappingTarget
   */
  public abstract B getMappingTarget(HiveReadableRecord record);

  @Override
  public void initializeRecords(Iterator<HiveReadableRecord> records) {
    this.records = records;
    reusableVertexId = getConf().createVertexId();
    reusableMappingTarget = (B) getConf().createMappingTarget();
    reusableEntry = new MappingEntry<>(reusableVertexId,
        reusableMappingTarget);
  }

  @Override
  public boolean hasNext() {
    return records.hasNext();
  }

  @Override
  public MappingEntry<I, B> next() {
    HiveReadableRecord record = records.next();
    I id = getVertexId(record);
    B target = getMappingTarget(record);
    reusableEntry.setVertexId(id);
    reusableEntry.setMappingTarget(target);
    return reusableEntry;
  }

  /**
   * Returns reusableVertexId for use in other methods
   *
   * @return reusableVertexId
   */
  public I getReusableVertexId() {
    return reusableVertexId;
  }

  /**
   * Returns reusableMappingTarget for use in other methods
   *
   * @return reusableMappingTarget
   */
  public B getReusableMappingTarget() {
    return reusableMappingTarget;
  }
}
