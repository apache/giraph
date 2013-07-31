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

import org.apache.giraph.hive.column.HiveReadableColumn;
import org.python.core.Py;
import org.python.core.PyBoolean;
import org.python.core.PyDictionary;
import org.python.core.PyFloat;
import org.python.core.PyInteger;
import org.python.core.PyList;
import org.python.core.PyLong;
import org.python.core.PyObject;
import org.python.core.PyString;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.record.HiveReadableRecord;

/**
 * A single column from a Hive record which reads Jython types
 */
public class JythonReadableColumn {
  /** Hive column */
  private final HiveReadableColumn column = new HiveReadableColumn();

  /**
   * Set Hive record
   *
   * @param record Hive record
   */
  public void setRecord(HiveReadableRecord record) {
    column.setRecord(record);
  }

  /**
   * Set column index
   *
   * @param index column index
   */
  public void setIndex(int index) {
    column.setIndex(index);
  }

  /**
   * Get PyBoolean from a boolean column
   *
   * @return PyBoolean
   */
  public PyBoolean getBoolean() {
    return new PyBoolean(column.getBoolean());
  }

  /**
   * Get PyInteger from a byte, short, or integer column
   *
   * @return PyInteger
   */
  public PyInteger getByte() {
    return getInt();
  }

  /**
   * Get PyInteger from a byte, short, or integer column
   *
   * @return PyInteger
   */
  public PyInteger getShort() {
    return getInt();
  }

  /**
   * Get PyInteger from a byte, short, or integer column
   *
   * @return PyInteger
   */
  public PyInteger getInt() {
    int value;
    if (column.hiveType() == HiveType.BYTE) {
      value = column.getByte();
    } else if (column.hiveType() == HiveType.SHORT) {
      value = column.getShort();
    } else if (column.hiveType() == HiveType.LONG) {
      value = column.getInt();
    } else {
      throw new IllegalArgumentException(
          "Column is not a byte/short/int, is " + column.hiveType());
    }
    return new PyInteger(value);
  }

  /**
   * Get PyLong as long
   *
   * @return PyLong
   */
  public PyLong getLong() {
    return new PyLong(column.getLong());
  }

  /**
   * Get PyFloat from a double or float column
   *
   * @return PyFloat
   */
  public PyFloat getDouble() {
    return getFloat();
  }

  /**
   * Get double or float column as PyFloat
   *
   * @return PyFloat
   */
  public PyFloat getFloat() {
    double value;
    if (column.hiveType() == HiveType.FLOAT) {
      value = column.getFloat();
    } else if (column.hiveType() == HiveType.DOUBLE) {
      value = column.getDouble();
    } else {
      throw new IllegalArgumentException("Column is not a float/double, is " +
          column.hiveType());
    }
    return new PyFloat(value);
  }

  /**
   * Get PyString from a string column
   *
   * @return PyString
   */
  public PyString getString() {
    return new PyString(column.getString());
  }

  /**
   * Get PyList from a list column
   *
   * @return PyList
   */
  public PyList getList() {
    return new PyList(column.getList());
  }

  /**
   * Get PyMap from a map column
   *
   * @return PyMap
   */
  public PyDictionary getMap() {
    PyDictionary dict = new PyDictionary();
    dict.putAll(column.getMap());
    return dict;
  }

  /**
   * Get arbitrary PyObject
   *
   * @return PyObject
   */
  public PyObject get() {
    return Py.java2py(column.get());
  }
}
