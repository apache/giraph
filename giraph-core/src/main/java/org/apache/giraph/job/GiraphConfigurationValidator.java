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

package org.apache.giraph.job;

import static org.apache.giraph.conf.GiraphConstants.VERTEX_EDGES_CLASS;
import static org.apache.giraph.conf.GiraphConstants.VERTEX_RESOLVER_CLASS;
import static org.apache.giraph.utils.ReflectionUtils.getTypeArguments;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.factories.DefaultVertexValueFactory;
import org.apache.giraph.factories.VertexValueFactory;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * GiraphConfigurationValidator attempts to verify the consistency of
 * user-chosen InputFormat, OutputFormat, and Vertex generic type
 * parameters as well as the general Configuration settings
 * before the job run actually begins.
 *
 * @param <I> the Vertex ID type
 * @param <V> the Vertex Value type
 * @param <E> the Edge Value type
 * @param <M1> the incoming Message type
 * @param <M2> the outgoing Message type
 */
public class GiraphConfigurationValidator<I extends WritableComparable,
    V extends Writable, E extends Writable, M1 extends Writable,
    M2 extends Writable> {
  /**
   * Class logger object.
   */
  private static Logger LOG =
    Logger.getLogger(GiraphConfigurationValidator.class);

  /** I param vertex index in classList */
  private static final int ID_PARAM_INDEX = 0;
  /** V param vertex index in classList */
  private static final int VALUE_PARAM_INDEX = 1;
  /** E param vertex index in classList */
  private static final int EDGE_PARAM_INDEX = 2;
  /** M param vertex combiner index in classList */
  private static final int MSG_COMBINER_PARAM_INDEX = 1;
  /** E param edge input format index in classList */
  private static final int EDGE_PARAM_EDGE_INPUT_FORMAT_INDEX = 1;
  /** E param vertex edges index in classList */
  private static final int EDGE_PARAM_OUT_EDGES_INDEX = 1;
  /** V param vertex value factory index in classList */
  private static final int VALUE_PARAM_VERTEX_VALUE_FACTORY_INDEX = 0;
  /** V param vertex value combiner index in classList */
  private static final int VALUE_PARAM_VERTEX_VALUE_COMBINER_INDEX = 0;

  /**
   * The Configuration object for use in the validation test.
   */
  private final ImmutableClassesGiraphConfiguration conf;

  /**
   * Constructor to execute the validation test, throws
   * unchecked exception to end job run on failure.
   *
   * @param conf the Configuration for this run.
   */
  public GiraphConfigurationValidator(Configuration conf) {
    this.conf = new ImmutableClassesGiraphConfiguration(conf);
  }

  /**
   * Get vertex id type
   *
   * @return vertex id type
   */
  private Class<? extends WritableComparable> vertexIndexType() {
    return conf.getGiraphTypes().getVertexIdClass();
  }

  /**
   * Get vertex value type
   *
   * @return vertex value type
   */
  private Class<? extends Writable> vertexValueType() {
    return conf.getGiraphTypes().getVertexValueClass();
  }

  /**
   * Get edge value type
   *
   * @return edge value type
   */
  private Class<? extends Writable> edgeValueType() {
    return conf.getGiraphTypes().getEdgeValueClass();
  }

  /**
   * Get outgoing message value type
   *
   * @return outgoing message value type
   */
  private Class<? extends Writable> outgoingMessageValueType() {
    return conf.getOutgoingMessageValueClass();
  }

  /**
   * Make sure that all registered classes have matching types.  This
   * is a little tricky due to type erasure, cannot simply get them from
   * the class type arguments.  Also, set the vertex index, vertex value,
   * edge value and message value classes.
   */
  public void validateConfiguration() {
    checkConfiguration();
    verifyOutEdgesGenericTypes();
    verifyVertexInputFormatGenericTypes();
    verifyEdgeInputFormatGenericTypes();
    verifyVertexOutputFormatGenericTypes();
    verifyEdgeOutputFormatGenericTypes();
    verifyVertexResolverGenericTypes();
    verifyVertexValueCombinerGenericTypes();
    verifyMessageCombinerGenericTypes();
    verifyVertexValueFactoryGenericTypes();
  }

  /**
   * Make sure the configuration is set properly by the user prior to
   * submitting the job.
   */
  private void checkConfiguration() {
    if (conf.getMaxWorkers() < 0) {
      throw new RuntimeException("checkConfiguration: No valid " +
          GiraphConstants.MAX_WORKERS);
    }
    if (conf.getMinPercentResponded() <= 0.0f ||
        conf.getMinPercentResponded() > 100.0f) {
      throw new IllegalArgumentException(
          "checkConfiguration: Invalid " + conf.getMinPercentResponded() +
              " for " + GiraphConstants.MIN_PERCENT_RESPONDED.getKey());
    }
    if (conf.getMinWorkers() < 0) {
      throw new IllegalArgumentException("checkConfiguration: No valid " +
          GiraphConstants.MIN_WORKERS);
    }
    conf.createComputationFactory().checkConfiguration(conf);
    if (conf.getVertexInputFormatClass() == null &&
        conf.getEdgeInputFormatClass() == null) {
      throw new IllegalArgumentException("checkConfiguration: One of " +
          GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.getKey() + " and " +
          GiraphConstants.EDGE_INPUT_FORMAT_CLASS.getKey() +
          " must be non-null");
    }
    if (conf.getVertexResolverClass() == null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("checkConfiguration: No class found for " +
            VERTEX_RESOLVER_CLASS.getKey() +
            ", defaulting to " +
            VERTEX_RESOLVER_CLASS.getDefaultClass().getCanonicalName());
      }
    }
    if (conf.getOutEdgesClass() == null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("checkConfiguration: No class found for " +
            VERTEX_EDGES_CLASS.getKey() + ", defaulting to " +
            VERTEX_EDGES_CLASS.getDefaultClass().getCanonicalName());
      }
    }
  }

  /**
   * Verify matching generic types for a specific OutEdges class.
   *
   * @param outEdgesClass {@link org.apache.giraph.edge.OutEdges} class to check
   */
  private void verifyOutEdgesGenericTypesClass(
      Class<? extends OutEdges<I, E>> outEdgesClass) {
    Class<?>[] classList = getTypeArguments(OutEdges.class, outEdgesClass);
    checkAssignable(classList, ID_PARAM_INDEX, vertexIndexType(),
        OutEdges.class, "vertex index");
    checkAssignable(classList, EDGE_PARAM_OUT_EDGES_INDEX, edgeValueType(),
        OutEdges.class, "edge value");
  }

  /** Verify matching generic types in OutEdges. */
  private void verifyOutEdgesGenericTypes() {
    Class<? extends OutEdges<I, E>> outEdgesClass =
        conf.getOutEdgesClass();
    Class<? extends OutEdges<I, E>> inputOutEdgesClass =
        conf.getInputOutEdgesClass();
    verifyOutEdgesGenericTypesClass(outEdgesClass);
    verifyOutEdgesGenericTypesClass(inputOutEdgesClass);
  }

  /** Verify matching generic types in VertexInputFormat. */
  private void verifyVertexInputFormatGenericTypes() {
    Class<? extends VertexInputFormat<I, V, E>> vertexInputFormatClass =
      conf.getVertexInputFormatClass();
    if (vertexInputFormatClass != null) {
      Class<?>[] classList =
          getTypeArguments(VertexInputFormat.class, vertexInputFormatClass);
      checkAssignable(classList, ID_PARAM_INDEX, vertexIndexType(),
          VertexInputFormat.class, "vertex index");
      checkAssignable(classList, VALUE_PARAM_INDEX, vertexValueType(),
          VertexInputFormat.class, "vertex value");
      checkAssignable(classList, EDGE_PARAM_INDEX, edgeValueType(),
          VertexInputFormat.class, "edge value");
    }
  }

  /** Verify matching generic types in EdgeInputFormat. */
  private void verifyEdgeInputFormatGenericTypes() {
    Class<? extends EdgeInputFormat<I, E>> edgeInputFormatClass =
        conf.getEdgeInputFormatClass();
    if (edgeInputFormatClass != null) {
      Class<?>[] classList =
          getTypeArguments(EdgeInputFormat.class, edgeInputFormatClass);
      checkAssignable(classList, ID_PARAM_INDEX, vertexIndexType(),
          EdgeInputFormat.class, "vertex index");
      checkAssignable(classList, EDGE_PARAM_EDGE_INPUT_FORMAT_INDEX,
          edgeValueType(), EdgeInputFormat.class, "edge value");
    }
  }

  /**
   * If there is a vertex value combiner type, verify its
   * generic params match the job.
   */
  private void verifyVertexValueCombinerGenericTypes() {
    Class<? extends VertexValueCombiner<V>> vertexValueCombiner =
        conf.getVertexValueCombinerClass();
    if (vertexValueCombiner != null) {
      Class<?>[] classList =
          getTypeArguments(VertexValueCombiner.class, vertexValueCombiner);
      checkAssignable(classList, VALUE_PARAM_VERTEX_VALUE_COMBINER_INDEX,
          vertexValueType(), VertexValueCombiner.class, "vertex value");
    }
  }

  /**
   * If there is a message combiner type, verify its
   * generic params match the job.
   */
  private void verifyMessageCombinerGenericTypes() {
    MessageCombiner<I, M2> messageCombiner =
      conf.createOutgoingMessageCombiner();
    if (messageCombiner != null) {
      Class<?>[] classList =
          getTypeArguments(MessageCombiner.class, messageCombiner.getClass());
      checkEquals(classList, ID_PARAM_INDEX, vertexIndexType(),
          MessageCombiner.class, "vertex index");
      checkEquals(classList, MSG_COMBINER_PARAM_INDEX,
          outgoingMessageValueType(), MessageCombiner.class, "message value");
    }
  }

  /** Verify that the vertex output format's generic params match the job. */
  private void verifyVertexOutputFormatGenericTypes() {
    Class<? extends EdgeOutputFormat<I, V, E>>
      edgeOutputFormatClass = conf.getEdgeOutputFormatClass();
    if (conf.hasEdgeOutputFormat()) {
      Class<?>[] classList =
        getTypeArguments(EdgeOutputFormat.class, edgeOutputFormatClass);
      checkAssignable(classList, ID_PARAM_INDEX, vertexIndexType(),
          VertexOutputFormat.class, "vertex index");
      checkAssignable(classList, VALUE_PARAM_INDEX, vertexValueType(),
          VertexOutputFormat.class, "vertex value");
      checkAssignable(classList, EDGE_PARAM_INDEX, edgeValueType(),
          VertexOutputFormat.class, "edge value");
    }
  }

  /** Verify that the edge output format's generic params match the job. */
  private void verifyEdgeOutputFormatGenericTypes() {
    Class<? extends VertexOutputFormat<I, V, E>>
      vertexOutputFormatClass = conf.getVertexOutputFormatClass();
    if (conf.hasVertexOutputFormat()) {
      Class<?>[] classList =
        getTypeArguments(VertexOutputFormat.class, vertexOutputFormatClass);
      checkAssignable(classList, ID_PARAM_INDEX, vertexIndexType(),
          VertexOutputFormat.class, "vertex index");
      checkAssignable(classList, VALUE_PARAM_INDEX, vertexValueType(),
          VertexOutputFormat.class, "vertex value");
      checkAssignable(classList, EDGE_PARAM_INDEX, edgeValueType(),
          VertexOutputFormat.class, "edge value");
    }
  }

  /** Verify that the vertex value factory's type matches the job */
  private void verifyVertexValueFactoryGenericTypes() {
    Class<? extends VertexValueFactory<V>>
        vvfClass = conf.getVertexValueFactoryClass();
    if (DefaultVertexValueFactory.class.equals(vvfClass)) {
      return;
    }
    Class<?>[] classList = getTypeArguments(VertexValueFactory.class, vvfClass);
    checkEquals(classList, VALUE_PARAM_VERTEX_VALUE_FACTORY_INDEX,
        vertexValueType(), VertexValueFactory.class, "vertex value");
  }

  /**
   * If there is a vertex resolver,
   * validate the generic parameter types.
   * */
  private void verifyVertexResolverGenericTypes() {
    Class<? extends VertexResolver<I, V, E>>
        vrClass = conf.getVertexResolverClass();
    if (DefaultVertexResolver.class.equals(vrClass)) {
      return;
    }
    Class<?>[] classList =
        getTypeArguments(VertexResolver.class, vrClass);
    checkEquals(classList, ID_PARAM_INDEX, vertexIndexType(),
        VertexResolver.class, "vertex index");
    checkEquals(classList, VALUE_PARAM_INDEX, vertexValueType(),
        VertexResolver.class, "vertex value");
    checkEquals(classList, EDGE_PARAM_INDEX, edgeValueType(),
        VertexResolver.class, "edge value");
  }

  /**
   * Check that the type from computation equals the type from the class.
   *
   * @param classList classes from type
   * @param index array index of class to check
   * @param classFromComputation class from computation
   * @param klass Class type we're checking, only used for printing name
   * @param typeName Name of type we're checking
   */
  private static void checkEquals(Class<?>[] classList, int index,
      Class<?> classFromComputation, Class klass, String typeName) {
    if (classList[index] == null) {
      LOG.warn(klass.getSimpleName() + " " + typeName + " type is not known");
    } else if (!classList[index].equals(classFromComputation)) {
      throw new IllegalStateException(
          "checkClassTypes: " + typeName + " types not equal, " +
              "computation - " + classFromComputation +
              ", " + klass.getSimpleName() + " - " +
              classList[index]);
    }
  }

  /**
   * Check that the type from computation is assignable to type from the class.
   *
   * @param classList classes from type
   * @param index array index of class to check
   * @param classFromComputation class from computation
   * @param klass Class type we're checking, only used for printing name
   * @param typeName Name of type we're checking
   */
  private static void checkAssignable(Class<?>[] classList, int index,
      Class<?> classFromComputation, Class klass, String typeName) {
    if (classList[index] == null) {
      LOG.warn(klass.getSimpleName() + " " + typeName + " type is not known");
    } else if (!classList[index].isAssignableFrom(classFromComputation)) {
      throw new IllegalStateException(
          "checkClassTypes: " + typeName + " types not assignable, " +
              "computation - " + classFromComputation +
              ", " + klass.getSimpleName() + " - " +
              classList[EDGE_PARAM_EDGE_INPUT_FORMAT_INDEX]);
    }
  }
}

