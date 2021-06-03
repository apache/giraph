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
package org.apache.giraph.debugger.mock;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Set;

import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Utility class to help format some of the strings used in the generated unit
 * test. E.g., if an a float is initialized to 1.3 and we read that from
 * a protocolbuffer and write it again, it will be written as 1.299999....
 * So we need to truncate it when we generate the unittest and this class
 * contains such methods.
 */
public class FormatHelper {

  /**
   * {@link DecimalFormat} to use when formatting decimals.
   */
  private final DecimalFormat decimalFormat = new DecimalFormat("#.#####");

  /**
   * A set of complex writable object, i.e., user-defined ones that are not like
   * IntWritable, FloatWritable, etc..used by this compute function.
   */
  @SuppressWarnings("rawtypes")
  private Set<Class> complexWritables;

  /**
   * Default constructor.
   */
  public FormatHelper() {
  }

  /**
   * @param complexWritables complex writable objects to register.
   */
  @SuppressWarnings("rawtypes")
  public FormatHelper(Set<Class> complexWritables) {
    registerComplexWritableClassList(complexWritables);
  }

  /**
   * @param complexWritables complex writable objects to register.
   */
  @SuppressWarnings("rawtypes")
  public void registerComplexWritableClassList(Set<Class> complexWritables) {
    this.complexWritables = complexWritables;
  }

  /**
   * Generates the line that constructs the given writable.
   * @param writable writable object to construct in the unit test.
   * @return string generating the line that constructs the given writable.
   */
  public String formatWritable(Writable writable) {
    if (writable instanceof NullWritable) {
      return "NullWritable.get()";
    } else if (writable instanceof BooleanWritable) {
      return String.format("new BooleanWritable(%s)",
        format(((BooleanWritable) writable).get()));
    } else if (writable instanceof ByteWritable) {
      return String.format("new ByteWritable(%s)",
        format(((ByteWritable) writable).get()));
    } else if (writable instanceof IntWritable) {
      return String.format("new IntWritable(%s)",
        format(((IntWritable) writable).get()));
    } else if (writable instanceof LongWritable) {
      return String.format("new LongWritable(%s)",
        format(((LongWritable) writable).get()));
    } else if (writable instanceof FloatWritable) {
      return String.format("new FloatWritable(%s)",
        format(((FloatWritable) writable).get()));
    } else if (writable instanceof DoubleWritable) {
      return String.format("new DoubleWritable(%s)",
        format(((DoubleWritable) writable).get()));
    } else if (writable instanceof Text) {
      return String.format("new Text(%s)", ((Text) writable).toString());
    } else {
      if (complexWritables != null) {
        complexWritables.add(writable.getClass());
      }
      String str = toByteArrayString(WritableUtils.writeToByteArray(writable));
      return String.format("(%s)read%sFromByteArray(new byte[] {%s})", writable
        .getClass().getSimpleName(), writable.getClass().getSimpleName(), str);
    }
  }

  /**
   * Generates the line that constructs the given object, which can be
   * an int, boolean, char, byte, short, etc.
   * @param input object to construct in the unit test.
   * @return string generating the line that constructs the given object.
   */
  public String format(Object input) {
    if (input instanceof Boolean || input instanceof Byte ||
      input instanceof Character || input instanceof Short ||
      input instanceof Integer) {
      return input.toString();
    } else if (input instanceof Long) {
      return input.toString() + "l";
    } else if (input instanceof Float) {
      return decimalFormat.format(input) + "f";
    } else if (input instanceof Double) {
      double val = ((Double) input).doubleValue();
      if (val == Double.MAX_VALUE) {
        return "Double.MAX_VALUE";
      } else if (val == Double.MIN_VALUE) {
        return "Double.MIN_VALUE";
      } else {
        BigDecimal bd = new BigDecimal(val);
        return bd.toEngineeringString() + "d";
      }
    } else {
      return input.toString();
    }
  }

  /**
   * Generates a line constructing a byte array.
   * @param byteArray byte array to construct in the unit test.
   * @return string generating the line that constructs the given byte array.
   */
  private String toByteArrayString(byte[] byteArray) {
    StringBuilder strBuilder = new StringBuilder();
    for (int i = 0; i < byteArray.length; i++) {
      if (i != 0) {
        strBuilder.append(',');
      }
      strBuilder.append(Byte.toString(byteArray[i]));
    }
    return strBuilder.toString();
  }

}
