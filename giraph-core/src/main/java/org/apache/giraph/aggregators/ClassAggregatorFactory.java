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
package org.apache.giraph.aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableFactory;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * Aggregator factory based on aggregatorClass.
 *
 * @param <T> Aggregated value type
 */
public class ClassAggregatorFactory<T extends Writable>
    extends DefaultImmutableClassesGiraphConfigurable
    implements WritableFactory<Aggregator<T>> {
  /** Aggregator class */
  private Class<? extends Aggregator<T>> aggregatorClass;

  /** Constructor */
  public ClassAggregatorFactory() {
  }

  /**
   * Constructor
   * @param aggregatorClass Aggregator class
   */
  public ClassAggregatorFactory(
      Class<? extends Aggregator<T>> aggregatorClass) {
    this(aggregatorClass, null);

  }

  /**
   * Constructor
   * @param aggregatorClass Aggregator class
   * @param conf Configuration
   */
  public ClassAggregatorFactory(Class<? extends Aggregator<T>> aggregatorClass,
      ImmutableClassesGiraphConfiguration conf) {
    Preconditions.checkNotNull(aggregatorClass,
        "aggregatorClass cannot be null in ClassAggregatorFactory");
    this.aggregatorClass = aggregatorClass;
    setConf(conf);
  }

  @Override
  public Aggregator<T> create() {
    return ReflectionUtils.newInstance(aggregatorClass, getConf());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    aggregatorClass = WritableUtils.readClass(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Preconditions.checkNotNull(aggregatorClass,
        "aggregatorClass cannot be null in ClassAggregatorFactory");
    WritableUtils.writeClass(aggregatorClass, out);
  }
}
