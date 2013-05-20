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

import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.List;

/**
 * Holds Computation and Combiner class.
 */
public class SuperstepClasses implements Writable {
  /** Computation class to be used in the following superstep */
  private Class<? extends Computation> computationClass;
  /** Combiner class to be used in the following superstep */
  private Class<? extends Combiner> combinerClass;

  /**
   * Default constructor
   */
  public SuperstepClasses() {
  }

  /**
   * Constructor
   *
   * @param conf Configuration
   */
  @SuppressWarnings("unchecked")
  public SuperstepClasses(ImmutableClassesGiraphConfiguration conf) {
    this(conf.getComputationClass(), conf.getCombinerClass());
  }

  /**
   * Constructor
   *
   * @param computationClass Computation class
   * @param combinerClass Combiner class
   */
  public SuperstepClasses(Class<? extends Computation> computationClass,
      Class<? extends Combiner> combinerClass) {
    this.computationClass = computationClass;
    this.combinerClass = combinerClass;
  }

  public Class<? extends Computation> getComputationClass() {
    return computationClass;
  }

  public Class<? extends Combiner> getCombinerClass() {
    return combinerClass;
  }

  public void setComputationClass(
      Class<? extends Computation> computationClass) {
    this.computationClass = computationClass;
  }

  public void setCombinerClass(Class<? extends Combiner> combinerClass) {
    this.combinerClass = combinerClass;
  }

  /**
   * Verify that types of current Computation and Combiner are valid. If types
   * don't match an {@link IllegalStateException} will be thrown.
   *
   * @param conf Configuration to verify this with
   */
  public void verifyTypesMatch(ImmutableClassesGiraphConfiguration conf) {
    List<Class<?>> computationTypes = ReflectionUtils.getTypeArguments(
        Computation.class, computationClass);
    verifyTypes(conf.getVertexIdClass(), computationTypes.get(0),
        "Vertex id", computationClass);
    verifyTypes(conf.getVertexValueClass(), computationTypes.get(1),
        "Vertex value", computationClass);
    verifyTypes(conf.getEdgeValueClass(), computationTypes.get(2),
        "Edge value", computationClass);
    verifyTypes(conf.getOutgoingMessageValueClass(), computationTypes.get(3),
        "Previous outgoing and new incoming message", computationClass);
    Class<?> outgoingMessageType = computationTypes.get(4);
    if (outgoingMessageType.isInterface()) {
      throw new IllegalStateException("verifyTypesMatch: " +
          "Message type must be concrete class " + outgoingMessageType);
    }
    if (Modifier.isAbstract(outgoingMessageType.getModifiers())) {
      throw new IllegalStateException("verifyTypesMatch: " +
          "Message type can't be abstract class" + outgoingMessageType);
    }
    if (combinerClass != null) {
      List<Class<?>> combinerTypes = ReflectionUtils.getTypeArguments(
          Combiner.class, combinerClass);
      verifyTypes(conf.getVertexIdClass(), combinerTypes.get(0),
          "Vertex id", combinerClass);
      verifyTypes(outgoingMessageType, combinerTypes.get(1),
          "Outgoing message", combinerClass);
    }
  }

  /**
   * Verify that found type matches the expected type. If types don't match an
   * {@link IllegalStateException} will be thrown.
   *
   * @param expected Expected type
   * @param actual Actual type
   * @param typeDesc String description of the type (for exception description)
   * @param mainClass Class in which the actual type was found (for exception
   *                  description)
   */
  private void verifyTypes(Class<?> expected, Class<?> actual,
      String typeDesc, Class<?> mainClass) {
    if (!expected.equals(actual)) {
      throw new IllegalStateException("verifyTypes: " + typeDesc + " types " +
          "don't match, in " + mainClass.getName() + " " + expected +
          " expected, but " + actual + " found");
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    WritableUtils.writeClass(computationClass, output);
    WritableUtils.writeClass(combinerClass, output);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    computationClass = WritableUtils.readClass(input);
    combinerClass = WritableUtils.readClass(input);
  }

  @Override
  public String toString() {
    return "(computation=" + computationClass.getName() + ",combiner=" +
        ((combinerClass == null) ? "null" : combinerClass.getName()) + ")";
  }
}
