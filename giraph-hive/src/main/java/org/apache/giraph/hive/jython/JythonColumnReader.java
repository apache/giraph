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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.hive.common.HiveUtils;
import org.apache.giraph.hive.values.HiveValueReader;
import org.apache.giraph.jython.JythonUtils;
import org.apache.hadoop.io.Writable;
import org.python.core.PyObject;

import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

/**
 * A Vertex ID reader from a single Hive column.
 *
 * @param <T> Vertex ID
 */
public class JythonColumnReader<T extends Writable>
    implements HiveValueReader<T> {
  /** The column to read from Hive */
  private JythonReadableColumn column = new JythonReadableColumn();
  /** User's jython column reader */
  private final JythonHiveReader jythonHiveReader;

  /**
   * Constructor
   *
   * @param columnIndex index for column
   * @param jythonHiveReader jython column reader
   */
  public JythonColumnReader(int columnIndex,
      JythonHiveReader jythonHiveReader) {
    column.setIndex(columnIndex);
    this.jythonHiveReader = jythonHiveReader;
  }

  /**
   * Create a new value reader
   *
   * @param <T> value type
   * @param conf {@link ImmutableClassesGiraphConfiguration}
   * @param jythonClassOption option for jython class name
   * @param columnOption {@link StrConfOption}
   * @param schema {@link HiveTableSchema}
   * @return new {@link JythonColumnReader}
   */
  public static <T extends Writable> JythonColumnReader<T>
  create(ImmutableClassesGiraphConfiguration conf,
      StrConfOption jythonClassOption, StrConfOption columnOption,
      HiveTableSchema schema) {
    PyObject pyClass =
        JythonUtils.getInterpreter().get(jythonClassOption.get(conf));
    JythonHiveReader jythonHiveReader = (JythonHiveReader)
        pyClass.__call__().__tojava__(JythonHiveReader.class);
    int columnIndex = HiveUtils.columnIndexOrThrow(schema, conf, columnOption);
    return new JythonColumnReader<T>(columnIndex, jythonHiveReader);
  }

  @Override
  public void readFields(T value, HiveReadableRecord record) {
    column.setRecord(record);
    jythonHiveReader.readFromHive(value, column);
  }
}
