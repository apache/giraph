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
package org.apache.giraph.block_app.framework;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public abstract class TestLongNullNullBlockFactory extends AbstractBlockFactory<Object> {
  @Override
  protected Class<? extends WritableComparable> getVertexIDClass(GiraphConfiguration conf) {
    return LongWritable.class;
  }

  @Override
  protected Class<? extends Writable> getVertexValueClass(GiraphConfiguration conf) {
    return NullWritable.class;
  }

  @Override
  protected Class<? extends Writable> getEdgeValueClass(GiraphConfiguration conf) {
    return NullWritable.class;
  }

  @Override
  public Object createExecutionStage(GiraphConfiguration conf) {
    return new Object();
  }
}