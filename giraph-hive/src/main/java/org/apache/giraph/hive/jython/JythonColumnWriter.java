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
import org.apache.giraph.hive.column.HiveWritableColumn;
import org.apache.giraph.hive.common.HiveUtils;
import org.apache.giraph.hive.values.HiveValueWriter;
import org.apache.giraph.jython.JythonUtils;
import org.apache.hadoop.io.Writable;
import org.python.core.PyObject;

import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

/**
 * A Hive type writer that uses {@link JythonHiveWriter}
 *
 * @param <W> graph value type
 */
public class JythonColumnWriter<W extends Writable>
    implements HiveValueWriter<W> {
  /** The column to read from Hive */
  private HiveWritableColumn column = new HiveWritableColumn();
  /** The user's Jython column writer */
  private final JythonHiveWriter jythonHiveWriter;

  /**
   * Constructor
   *
   * @param columnIndex column index
   * @param jythonHiveWriter user's Jython column writer
   */
  public JythonColumnWriter(int columnIndex,
      JythonHiveWriter jythonHiveWriter) {
    column.setIndex(columnIndex);
    this.jythonHiveWriter = jythonHiveWriter;
  }

  /**
   * Create a new vertex ID reader
   *
   * @param conf {@link ImmutableClassesGiraphConfiguration}
   * @param jythonClassOption Option for name of Jython class
   * @param columnOption {@link StrConfOption}
   * @param schema {@link HiveTableSchema}
   * @param <W> Graph value type
   * @return new {@link JythonColumnWriter}
   */
  public static <W extends Writable> JythonColumnWriter<W>
  create(ImmutableClassesGiraphConfiguration conf,
      StrConfOption jythonClassOption, StrConfOption columnOption,
      HiveTableSchema schema) {
    String className = jythonClassOption.get(conf);
    PyObject pyClass = JythonUtils.getInterpreter().get(className);
    JythonHiveWriter jythonColumnWritable = (JythonHiveWriter)
        pyClass.__call__().__tojava__(JythonHiveWriter.class);
    int columnIndex = HiveUtils.columnIndexOrThrow(schema, conf, columnOption);
    return new JythonColumnWriter<W>(columnIndex, jythonColumnWritable);
  }

  @Override
  public void write(W value, HiveWritableRecord record) {
    column.setRecord(record);
    jythonHiveWriter.writeToHive(value, column);
  }
}
