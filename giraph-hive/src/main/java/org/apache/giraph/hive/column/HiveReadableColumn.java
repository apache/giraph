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
import com.facebook.hiveio.record.HiveReadableRecord;

import java.util.List;
import java.util.Map;

/**
 * A single column from a Hive record.
 */
public class HiveReadableColumn {
  /** The Hive record */
  private HiveReadableRecord record;
  /** The column index to use */
  private int index;

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public HiveReadableRecord getRecord() {
    return record;
  }

  public void setRecord(HiveReadableRecord record) {
    this.record = record;
  }

  public boolean isNull() {
    return record.isNull(index);
  }

  /**
   * Get type for this column
   *
   * @return {@link HiveType}
   */
  public HiveType hiveType() {
    return record.columnType(index);
  }

  /**
   * Get column value
   *
   * Regular data columns from the tables should always be placed first, and
   * then partition value columns.
   *
   * If you know the type of the column and it is a primitive you should use
   * one of the calls below as it will likely be more efficient.
   *
   * @return Object for column
   * @deprecated use {@link #get(com.facebook.hiveio.common.HiveType)}
   *             or one of the getX() methods
   */
  @Deprecated
  public Object get() {
    return record.get(index);
  }

  /**
   * Get column value
   *
   * Regular data columns from the tables should always be placed first, and
   * then partition value columns.
   *
   * You should probably be using one of getX() methods below instead.
   *
   * @param hiveType HiveType
   * @return Object for column
   */
  public Object get(HiveType hiveType) {
    return record.get(index, hiveType);
  }

  /**
   * Get boolean value
   *
   * @return boolean at index
   */
  public boolean getBoolean() {
    return record.getBoolean(index);
  }

  /**
   * Get byte value
   *
   * @return byte at index
   */
  public byte getByte() {
    return record.getByte(index);
  }

  /**
   * Get short value
   *
   * @return short at index
   */
  public short getShort() {
    return record.getShort(index);
  }

  /**
   * Get int value
   *
   * @return int at index
   */
  public int getInt() {
    return record.getInt(index);
  }

  /**
   * Get long value
   *
   * @return long at index
   */
  public long getLong() {
    return record.getLong(index);
  }

  /**
   * Get float value
   *
   * @return float at index
   */
  public float getFloat() {
    return record.getFloat(index);
  }

  /**
   * Get double value
   *
   * @return double at index
   */
  public double getDouble() {
    return record.getDouble(index);
  }

  /**
   * Get String column value
   * Note that partition values are all strings.
   *
   * @return String at index
   */
  public String getString() {
    return record.getString(index);
  }

  /**
   * Get List column value
   * Note that partition values are all strings.
   *
   * @param <T> item type
   * @return List at index
   */
  public <T> List<T> getList() {
    return record.getList(index);
  }

  /**
   * Get Map column value
   * Note that partition values are all strings.
   *
   * @param <K> key type
   * @param <V> value type
   * @return Map at index
   */
  public <K, V> Map<K, V> getMap() {
    return record.getMap(index);
  }
}
