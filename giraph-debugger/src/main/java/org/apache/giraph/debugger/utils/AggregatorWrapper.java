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
package org.apache.giraph.debugger.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.debugger.GiraphAggregator.AggregatedValue;
import org.apache.giraph.debugger.GiraphAggregator.Aggregator.Builder;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.GeneratedMessage;

/**
 * Wrapper class around
 * {@link org.apache.giraph.debugger.GiraphAggregator.Aggregator} protocol
 * buffer.
 *
 * author: semihsalihoglu
 */
@SuppressWarnings("rawtypes")
public class AggregatorWrapper extends BaseWrapper {

  /**
   * Key of the aggregator.
   */
  private String key;
  /**
   * The aggregator object.
   */
  private final Aggregator<Writable> aggregator;

  /**
   * Constructor.
   * @param key key of the aggregator.
   * @param aggregator the aggregator object.
   */
  @SuppressWarnings("unchecked")
  public AggregatorWrapper(String key, Aggregator aggregator) {
    this.key = key;
    this.aggregator = aggregator;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    Builder aggregatorProtoBuilder =
      org.apache.giraph.debugger.GiraphAggregator.Aggregator.newBuilder();
    aggregatorProtoBuilder.setAggregatorClass(aggregator.getClass().getName());
    aggregatorProtoBuilder
      .setAggregatedValue((AggregatedValue) new AggregatedValueWrapper(key,
        aggregator.getAggregatedValue()).buildProtoObject());
    return aggregatorProtoBuilder.build();
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
    throws IOException {
    return org.apache.giraph.debugger.GiraphAggregator.Aggregator
      .parseFrom(inputStream);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void loadFromProto(GeneratedMessage protoObject)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException {
    org.apache.giraph.debugger.GiraphAggregator.Aggregator aggregatorProto =
      (org.apache.giraph.debugger.GiraphAggregator.Aggregator) protoObject;
    Aggregator<Writable> giraphAggregator =
      (org.apache.giraph.aggregators.Aggregator<Writable>) Class
      .forName(aggregatorProto.getAggregatorClass()).newInstance();
    AggregatedValue aggregatedValueProto = aggregatorProto.getAggregatedValue();
    this.key = aggregatedValueProto.getKey();
    Writable giraphAggregatedValue = (Writable) Class.forName(
      aggregatedValueProto.getWritableClass()).newInstance();
    WritableUtils.readFieldsFromByteArray(aggregatedValueProto.getValue()
      .toByteArray(), giraphAggregatedValue);
    giraphAggregator.setAggregatedValue(giraphAggregatedValue);
  }

  public String getKey() {
    return key;
  }

  public Aggregator<Writable> getAggregator() {
    return aggregator;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("\nkey: " + key);
    stringBuilder
      .append(" aggregatorClass: " + aggregator.getClass().getName());
    stringBuilder.append(" aggregatedValueClass: " +
      aggregator.getAggregatedValue().getClass().getName());
    stringBuilder.append(" value: " + aggregator.getAggregatedValue());
    return stringBuilder.toString();
  }
}
