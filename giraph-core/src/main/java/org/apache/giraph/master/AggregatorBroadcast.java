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
package org.apache.giraph.master;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

/**
 * Writable representation of aggregated value
 *
 * @param <A> Aggregation object type
 */
public class AggregatorBroadcast<A extends Writable>
  extends DefaultImmutableClassesGiraphConfigurable
  implements Writable {
  /** Aggregator class */
  private Class<? extends Aggregator<A>> aggregatorClass;
  /** Aggregated value */
  private A value;

  /** Constructor */
  public AggregatorBroadcast() {
  }

  /**
   * Constructor
   * @param aggregatorClass Aggregator class
   * @param value Aggregated value
   */
  public AggregatorBroadcast(
      Class<? extends Aggregator<A>> aggregatorClass, A value) {
    this.aggregatorClass = aggregatorClass;
    this.value = value;
  }

  public A getValue() {
    return value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeClass(aggregatorClass, out);
    value.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    aggregatorClass = WritableUtils.readClass(in);
    value = ReflectionUtils.newInstance(aggregatorClass, getConf())
        .createInitialValue();
    value.readFields(in);
  }
}
