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

import static org.apache.giraph.conf.GiraphConstants.COMPUTATION_LANGUAGE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Language;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Holds Computation and MessageCombiner class.
 */
public class SuperstepClasses implements Writable {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(SuperstepClasses.class);

  /** Computation class to be used in the following superstep */
  private Class<? extends Computation> computationClass;
  /** MessageCombiner class to be used in the following superstep */
  private Class<? extends MessageCombiner> messageCombinerClass;
  /** Incoming message class to be used in the following superstep */
  private Class<? extends Writable> incomingMessageClass;
  /** Outgoing message class to be used in the following superstep */
  private Class<? extends Writable> outgoingMessageClass;

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
    this(conf.getComputationClass(), conf.getMessageCombinerClass());
  }

  /**
   * Constructor
   *
   * @param computationClass Computation class
   * @param messageCombinerClass MessageCombiner class
   */
  public SuperstepClasses(Class<? extends Computation> computationClass,
      Class<? extends MessageCombiner> messageCombinerClass) {
    this.computationClass = computationClass;
    this.messageCombinerClass =
        messageCombinerClass;
  }

  public Class<? extends Computation> getComputationClass() {
    return computationClass;
  }

  public Class<? extends MessageCombiner> getMessageCombinerClass() {
    return messageCombinerClass;
  }

  /**
   * Get incoming message class, either set directly, or through Computation
   * @return incoming message class
   */
  public Class<? extends Writable> getIncomingMessageClass() {
    if (incomingMessageClass != null) {
      return incomingMessageClass;
    }
    if (computationClass == null) {
      return null;
    }
    Class[] computationTypes = ReflectionUtils.getTypeArguments(
        TypesHolder.class, computationClass);
    return computationTypes[3];
  }

  /**
   * Get outgoing message class, either set directly, or through Computation
   * @return outgoing message class
   */
  public Class<? extends Writable> getOutgoingMessageClass() {
    if (outgoingMessageClass != null) {
      return outgoingMessageClass;
    }
    if (computationClass == null) {
      return null;
    }
    Class[] computationTypes = ReflectionUtils.getTypeArguments(
        TypesHolder.class, computationClass);
    return computationTypes[4];
  }

  public void setComputationClass(
      Class<? extends Computation> computationClass) {
    this.computationClass = computationClass;
  }

  public void setMessageCombinerClass(
      Class<? extends MessageCombiner> messageCombinerClass) {
    this.messageCombinerClass = messageCombinerClass;
  }

  public void setIncomingMessageClass(
      Class<? extends Writable> incomingMessageClass) {
    this.incomingMessageClass = incomingMessageClass;
  }

  public void setOutgoingMessageClass(
      Class<? extends Writable> outgoingMessageClass) {
    this.outgoingMessageClass = outgoingMessageClass;
  }

  /**
   * Verify that types of current Computation and MessageCombiner are valid.
   * If types don't match an {@link IllegalStateException} will be thrown.
   *
   * @param conf Configuration to verify this with
   * @param checkMatchingMesssageTypes Check that the incoming/outgoing
   *                                   message types match
   */
  public void verifyTypesMatch(ImmutableClassesGiraphConfiguration conf,
                               boolean checkMatchingMesssageTypes) {
    // In some cases, for example when using Jython, the Computation class may
    // not be set. This is because it is created by a ComputationFactory
    // dynamically and not known ahead of time. In this case there is nothing to
    // verify here so we bail.
    if (COMPUTATION_LANGUAGE.get(conf) == Language.JYTHON) {
      return;
    }

    Class<?>[] computationTypes = ReflectionUtils.getTypeArguments(
        TypesHolder.class, computationClass);
    verifyTypes(conf.getVertexIdClass(), computationTypes[0],
        "Vertex id", computationClass);
    verifyTypes(conf.getVertexValueClass(), computationTypes[1],
        "Vertex value", computationClass);
    verifyTypes(conf.getEdgeValueClass(), computationTypes[2],
        "Edge value", computationClass);

    Class<?> incomingMessageType = getIncomingMessageClass();
    Class<?> outgoingMessageType = getOutgoingMessageClass();

    if (checkMatchingMesssageTypes) {
      verifyTypes(incomingMessageType, conf.getOutgoingMessageValueClass(),
          "New incoming and previous outgoing message", computationClass);
    }
    if (outgoingMessageType.isInterface()) {
      throw new IllegalStateException("verifyTypesMatch: " +
          "Message type must be concrete class " + outgoingMessageType);
    }
    if (Modifier.isAbstract(outgoingMessageType.getModifiers())) {
      throw new IllegalStateException("verifyTypesMatch: " +
          "Message type can't be abstract class" + outgoingMessageType);
    }
    if (messageCombinerClass != null) {
      Class<?>[] combinerTypes = ReflectionUtils.getTypeArguments(
          MessageCombiner.class, messageCombinerClass);
      verifyTypes(conf.getVertexIdClass(), combinerTypes[0],
          "Vertex id", messageCombinerClass);
      verifyTypes(outgoingMessageType, combinerTypes[1],
          "Outgoing message", messageCombinerClass);
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
      if (actual.isAssignableFrom(expected)) {
        LOG.warn("verifyTypes: proceeding with assignable types : " +
          typeDesc + " types, in " + mainClass.getName() + " " + expected +
          " expected, but " + actual + " found");
      } else {
        throw new IllegalStateException("verifyTypes: " + typeDesc +
            " types " + "don't match, in " + mainClass.getName() + " " +
            expected + " expected, but " + actual + " found");
      }
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    WritableUtils.writeClass(computationClass, output);
    WritableUtils.writeClass(messageCombinerClass, output);
    WritableUtils.writeClass(incomingMessageClass, output);
    WritableUtils.writeClass(outgoingMessageClass, output);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    computationClass = WritableUtils.readClass(input);
    messageCombinerClass = WritableUtils.readClass(input);
    incomingMessageClass = WritableUtils.readClass(input);
    outgoingMessageClass = WritableUtils.readClass(input);
  }

  @Override
  public String toString() {
    String computationName = computationClass == null ? "_not_set_" :
        computationClass.getName();
    String combinerName = (messageCombinerClass == null) ? "null" :
        messageCombinerClass.getName();
    String incomingName = (incomingMessageClass == null) ? "null" :
      incomingMessageClass.getName();
    String outgoingName = (outgoingMessageClass == null) ? "null" :
      outgoingMessageClass.getName();

    return "(computation=" + computationName + ",combiner=" + combinerName +
        ",incoming=" + incomingName + ",outgoing=" + outgoingName + ")";
  }
}
