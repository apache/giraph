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

import org.apache.giraph.debugger.GiraphAggregator.AggregatedValue;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

/**
 * Wrapper class around
 * {@link org.apache.giraph.debugger.GiraphAggregator.AggregatedValue} protocol
 * buffer.
 *
 * author: semihsalihoglu
 */
public class AggregatedValueWrapper extends BaseWrapper {
  /**
   * Key of the aggregator.
   */
  private String key;
  /**
   * Value of the aggregator.
   */
  private Writable value;

  /**
   * Public constructor, initializing an empty aggregator. Intended to be used
   * when reading an aggregator from a protobuf.
   */
  public AggregatedValueWrapper() { }

  /**
   * Constructor. Intended to be used by Graft when it's intercepting
   * computations during debugging.
   * @param key key of the aggregator.
   * @param value value of the aggregator.
   */
  public AggregatedValueWrapper(String key, Writable value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    AggregatedValue.Builder aggregatedValueBuilder = AggregatedValue
      .newBuilder();
    aggregatedValueBuilder.setWritableClass(value.getClass().getName());
    aggregatedValueBuilder.setKey(key);
    aggregatedValueBuilder.setValue(ByteString.copyFrom(WritableUtils
      .writeToByteArray(value)));
    return aggregatedValueBuilder.build();
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
    throws IOException {
    return AggregatedValue.parseFrom(inputStream);
  }

  @Override
  public void loadFromProto(GeneratedMessage protoObject)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException {
    AggregatedValue aggregatedValueProto = (AggregatedValue) protoObject;
    this.value = (Writable) Class.forName(
      aggregatedValueProto.getWritableClass()).newInstance();
    WritableUtils.readFieldsFromByteArray(aggregatedValueProto.getValue()
      .toByteArray(), this.value);
    this.key = aggregatedValueProto.getKey();
  }

  public String getKey() {
    return key;
  }

  public Writable getValue() {
    return value;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("\nkey: " + key);
    stringBuilder
      .append(" aggregatedValueClass: " + value.getClass().getName());
    stringBuilder.append(" value: " + value);
    return stringBuilder.toString();
  }
}
