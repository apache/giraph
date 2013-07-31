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
package org.apache.giraph.hive.column;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.record.HiveWritableRecord;

import java.util.List;
import java.util.Map;

/**
 * A single column to write to a Hive record.
 */
public class HiveWritableColumn {
  /** The Hive record */
  private HiveWritableRecord record;
  /** The column index to use */
  private int index;

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public HiveWritableRecord getRecord() {
    return record;
  }

  public void setRecord(HiveWritableRecord record) {
    this.record = record;
  }

  /**
   * Set value for column.
   *
   * @param value Data for column
   * @deprecated
   *  use {@link #set(Object, com.facebook.hiveio.common.HiveType)}
   *  or one of the setX() methods
   */
  @Deprecated
  public void set(Object value) {
    record.set(index, value);
  }

  /**
   * Set value with type for column.
   *
   * @param value data for column
   * @param hiveType expected hive type
   */
  public void set(Object value, HiveType hiveType) {
    record.set(index, value, hiveType);
  }

  /**
   * Set boolean value for column.
   *
   * @param value Data for column
   */
  public void setBoolean(boolean value) {
    record.setBoolean(index, value);
  }

  /**
   * Set byte value for column.
   *
   * @param value Data for column
   */
  public void setByte(byte value) {
    record.setByte(index, value);
  }

  /**
   * Set short value for column.
   *
   * @param value Data for column
   */
  public void setShort(short value) {
    record.setShort(index, value);
  }

  /**
   * Set int value for column.
   *
   * @param value Data for column
   */
  public void setInt(int value) {
    record.setInt(index, value);
  }

  /**
   * Set long value for column.
   *
   * @param value Data for column
   */
  public void setLong(long value) {
    record.setLong(index, value);
  }

  /**
   * Set float value for column.
   *
   * @param value Data for column
   */
  public void setFloat(float value) {
    record.setFloat(index, value);
  }

  /**
   * Set double value for column.
   *
   * @param value Data for column
   */
  public void setDouble(double value) {
    record.setDouble(index, value);
  }

  /**
   * Set double value for column.
   *
   * @param value Data for column
   */
  public void setString(String value) {
    record.setString(index, value);
  }

  /**
   * Set List value for column.
   *
   * @param data Data for column
   */
  public void setList(List data) {
    record.setList(index, data);
  }

  /**
   * Set Map value for column.
   *
   * @param data Data for column
   */
  public void setMap(Map data) {
    record.setMap(index, data);
  }
}
