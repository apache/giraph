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

package org.apache.giraph.hive.common;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemaAware;

/**
 * Default implementation of {@link HiveTableSchemaAware} and
 * {@link org.apache.giraph.conf.ImmutableClassesGiraphConfigurable}
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public class DefaultConfigurableAndTableSchemaAware<
    I extends WritableComparable, V extends Writable, E extends Writable>
    extends DefaultImmutableClassesGiraphConfigurable<I, V, E>
    implements HiveTableSchemaAware {
  /** Schema stored here */
  private HiveTableSchema tableSchema;

  @Override public void setTableSchema(HiveTableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  @Override public HiveTableSchema getTableSchema() {
    return tableSchema;
  }
}
