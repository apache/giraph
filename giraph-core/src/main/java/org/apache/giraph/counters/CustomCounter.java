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
package org.apache.giraph.counters;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An object of this class represents a counter with a group name,
 * counter name, value, and type of aggregation to be performed
 *
 */
@ThriftStruct
public class CustomCounter implements Writable, Comparable {

  /** Counter group name */
  private String groupName;

  /** Counter name */
  private String counterName;

  /** Type of aggregation for counter */
  private Aggregation aggregation;

  /** Counter value */
  private long value;

  /** To store the type of aggregation to be done on the counters */
  public enum Aggregation {
    /** Sum */
    SUM,
    /** Max */
    MAX,
    /** Min */
    MIN,
    /** No op */
    NOOP
  }

  /**
   * Create a default custom counter
   *
   */
  public CustomCounter() {
    groupName = "";
    counterName = "";
    aggregation = Aggregation.NOOP;
    value = 0;
  }

  /**
   * Create a counter with a given group, name, and aggregation type
   * The default value is 0
   *
   * @param groupName Group name
   * @param counterName Counter name
   * @param aggregation Aggregation type
   */
  public CustomCounter(String groupName, String counterName,
                       Aggregation aggregation) {
    this(groupName, counterName, aggregation, 0);
  }

  /**
   * Create a counter with a group, name, aggregation type, and value
   * @param groupName Group name
   * @param counterName Counter name
   * @param aggregation Aggregation type
   * @param value Value
   */
  public CustomCounter(String groupName, String counterName,
                       Aggregation aggregation, long value) {
    this.groupName = groupName;
    this.counterName = counterName;
    this.aggregation = aggregation;
    this.value = value;
  }

  @ThriftField(1)
  public String getGroupName() {
    return groupName;
  }

  @ThriftField
  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  @ThriftField(2)
  public String getCounterName() {
    return counterName;
  }

  @ThriftField
  public void setCounterName(String counterName) {
    this.counterName = counterName;
  }

  @ThriftField(3)
  public Aggregation getAggregation() {
    return aggregation;
  }

  @ThriftField
  public void setAggregation(Aggregation aggregation) {
    this.aggregation = aggregation;
  }

  @ThriftField(4)
  public long getValue() {
    return value;
  }

  @ThriftField
  public void setValue(long value) {
    this.value = value;
  }

  @Override
  public int hashCode() {
    final int prime = 29;
    int result = 1;
    result = prime * result + getGroupName().hashCode();
    result = prime * result + getCounterName().hashCode();
    result = prime * result + getAggregation().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof CustomCounter) {
      CustomCounter customCounter = (CustomCounter) other;
      if (getGroupName().equalsIgnoreCase(customCounter.getGroupName()) &&
            getCounterName().equalsIgnoreCase(customCounter.getCounterName()) &&
            getAggregation().name().equalsIgnoreCase(
                    customCounter.getAggregation().name())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(groupName);
    output.writeUTF(counterName);
    output.writeUTF(aggregation.name());
    output.writeLong(value);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    groupName = input.readUTF();
    counterName = input.readUTF();
    aggregation = Aggregation.valueOf(input.readUTF());
    value = input.readLong();
  }

  @Override
  public int compareTo(Object o) {
    CustomCounter c2 = (CustomCounter) o;
    int compare = getGroupName().compareToIgnoreCase(c2.getGroupName());
    if (compare == 0) {
      return getCounterName().compareToIgnoreCase(c2.getCounterName());
    }
    return compare;
  }
}
