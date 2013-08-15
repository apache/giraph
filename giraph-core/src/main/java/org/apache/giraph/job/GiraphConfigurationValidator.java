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

import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.DefaultVertexValueFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.VertexValueFactory;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.util.List;

import static org.apache.giraph.conf.GiraphConstants.VERTEX_EDGES_CLASS;
import static org.apache.giraph.conf.GiraphConstants.VERTEX_RESOLVER_CLASS;

/**
 * GiraphConfigurationValidator attempts to verify the consistency of
 * user-chosen InputFormat, OutputFormat, and Vertex generic type
 * parameters as well as the general Configuration settings
 * before the job run actually begins.
 *
 * @param <I> the Vertex ID type
 * @param <V> the Vertex Value type
 * @param <E> the Edge Value type
 * @param <M> the Message type
 */
public class GiraphConfigurationValidator<I extends WritableComparable,
  V extends Writable, E extends Writable, M extends Writable> {
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
  /** M param vertex index in classList */
  private static final int MSG_PARAM_INDEX = 3;
  /** M param vertex combiner index in classList */
  private static final int MSG_COMBINER_PARAM_INDEX = 1;
  /** E param edge input format index in classList */
  private static final int EDGE_PARAM_EDGE_INPUT_FORMAT_INDEX = 1;
  /** E param vertex edges index in classList */
  private static final int EDGE_PARAM_VERTEX_EDGES_INDEX = 1;
  /** V param vertex value factory index in classList */
  private static final int VALUE_PARAM_VERTEX_VALUE_FACTORY_INDEX = 0;

  /** Vertex Index Type */
  private Type vertexIndexType;
  /** Vertex Index Type */
  private Type vertexValueType;
  /** Vertex Index Type */
  private Type edgeValueType;
  /** Vertex Index Type */
  private Type messageValueType;

  /**
   * The Configuration object for use in the validation test.
   */
  private ImmutableClassesGiraphConfiguration conf;

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
   * Make sure that all registered classes have matching types.  This
   * is a little tricky due to type erasure, cannot simply get them from
   * the class type arguments.  Also, set the vertex index, vertex value,
   * edge value and message value classes.
   */
  public void validateConfiguration() {
    checkConfiguration();
    Class<? extends Vertex<I, V, E, M>> vertexClass =
      conf.getVertexClass();
    List<Class<?>> classList = ReflectionUtils.getTypeArguments(
      Vertex.class, vertexClass);
    vertexIndexType = classList.get(ID_PARAM_INDEX);
    vertexValueType = classList.get(VALUE_PARAM_INDEX);
    edgeValueType = classList.get(EDGE_PARAM_INDEX);
    messageValueType = classList.get(MSG_PARAM_INDEX);
    verifyOutEdgesGenericTypes();
    verifyVertexInputFormatGenericTypes();
    verifyEdgeInputFormatGenericTypes();
    verifyVertexOutputFormatGenericTypes();
    verifyVertexResolverGenericTypes();
    verifyVertexCombinerGenericTypes();
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
    if (conf.getVertexClass() == null) {
      throw new IllegalArgumentException("checkConfiguration: Null " +
          GiraphConstants.VERTEX_CLASS.getKey());
    }
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
    List<Class<?>> classList = ReflectionUtils.getTypeArguments(
        OutEdges.class, outEdgesClass);
    // OutEdges implementations can be generic, in which case there are no
    // types to check.
    if (classList.isEmpty()) {
      return;
    }
    if (classList.get(ID_PARAM_INDEX) != null &&
        !vertexIndexType.equals(classList.get(ID_PARAM_INDEX))) {
      throw new IllegalArgumentException(
          "checkClassTypes: Vertex index types don't match, " +
              "vertex - " + vertexIndexType +
              ", vertex edges - " + classList.get(ID_PARAM_INDEX));
    }
    if (classList.get(EDGE_PARAM_VERTEX_EDGES_INDEX) != null &&
        !edgeValueType.equals(classList.get(EDGE_PARAM_VERTEX_EDGES_INDEX))) {
      throw new IllegalArgumentException(
          "checkClassTypes: Edge value types don't match, " +
              "vertex - " + edgeValueType +
              ", vertex edges - " +
              classList.get(EDGE_PARAM_VERTEX_EDGES_INDEX));
    }
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
      List<Class<?>> classList =
          ReflectionUtils.getTypeArguments(
              VertexInputFormat.class, vertexInputFormatClass);
      if (classList.get(ID_PARAM_INDEX) == null) {
        LOG.warn("Input format vertex index type is not known");
      } else if (!vertexIndexType.equals(classList.get(ID_PARAM_INDEX))) {
        throw new IllegalArgumentException(
            "checkClassTypes: Vertex index types don't match, " +
                "vertex - " + vertexIndexType +
                ", vertex input format - " + classList.get(ID_PARAM_INDEX));
      }
      if (classList.get(VALUE_PARAM_INDEX) == null) {
        LOG.warn("Input format vertex value type is not known");
      } else if (!vertexValueType.equals(classList.get(VALUE_PARAM_INDEX))) {
        throw new IllegalArgumentException(
            "checkClassTypes: Vertex value types don't match, " +
                "vertex - " + vertexValueType +
                ", vertex input format - " + classList.get(VALUE_PARAM_INDEX));
      }
      if (classList.get(EDGE_PARAM_INDEX) == null) {
        LOG.warn("Input format edge value type is not known");
      } else if (!edgeValueType.equals(classList.get(EDGE_PARAM_INDEX))) {
        throw new IllegalArgumentException(
            "checkClassTypes: Edge value types don't match, " +
                "vertex - " + edgeValueType +
                ", vertex input format - " + classList.get(EDGE_PARAM_INDEX));
      }
    }
  }

  /** Verify matching generic types in EdgeInputFormat. */
  private void verifyEdgeInputFormatGenericTypes() {
    Class<? extends EdgeInputFormat<I, E>> edgeInputFormatClass =
        conf.getEdgeInputFormatClass();
    if (edgeInputFormatClass != null) {
      List<Class<?>> classList =
          ReflectionUtils.getTypeArguments(
              EdgeInputFormat.class, edgeInputFormatClass);
      if (classList.get(ID_PARAM_INDEX) == null) {
        LOG.warn("Input format vertex index type is not known");
      } else if (!vertexIndexType.equals(classList.get(ID_PARAM_INDEX))) {
        throw new IllegalArgumentException(
            "checkClassTypes: Vertex index types don't match, " +
                "vertex - " + vertexIndexType +
                ", edge input format - " + classList.get(ID_PARAM_INDEX));
      }
      if (classList.get(EDGE_PARAM_EDGE_INPUT_FORMAT_INDEX) == null) {
        LOG.warn("Input format edge value type is not known");
      } else if (!edgeValueType.equals(
          classList.get(EDGE_PARAM_EDGE_INPUT_FORMAT_INDEX))) {
        throw new IllegalArgumentException(
            "checkClassTypes: Edge value types don't match, " +
                "vertex - " + edgeValueType +
                ", edge input format - " +
                classList.get(EDGE_PARAM_EDGE_INPUT_FORMAT_INDEX));
      }
    }
  }

  /** If there is a combiner type, verify its generic params match the job. */
  private void verifyVertexCombinerGenericTypes() {
    Class<? extends Combiner<I, M>> vertexCombinerClass =
      conf.getCombinerClass();
    if (vertexCombinerClass != null) {
      List<Class<?>> classList =
        ReflectionUtils.getTypeArguments(
          Combiner.class, vertexCombinerClass);
      if (!vertexIndexType.equals(classList.get(ID_PARAM_INDEX))) {
        throw new IllegalArgumentException(
          "checkClassTypes: Vertex index types don't match, " +
            "vertex - " + vertexIndexType +
            ", vertex combiner - " + classList.get(ID_PARAM_INDEX));
      }
      if (!messageValueType.equals(classList.get(MSG_COMBINER_PARAM_INDEX))) {
        throw new IllegalArgumentException(
          "checkClassTypes: Message value types don't match, " +
            "vertex - " + messageValueType +
            ", vertex combiner - " + classList.get(MSG_COMBINER_PARAM_INDEX));
      }
    }
  }

  /** Verify that the output format's generic params match the job. */
  private void verifyVertexOutputFormatGenericTypes() {
    Class<? extends VertexOutputFormat<I, V, E>>
      vertexOutputFormatClass = conf.getVertexOutputFormatClass();
    if (vertexOutputFormatClass != null) {
      List<Class<?>> classList =
        ReflectionUtils.getTypeArguments(
          VertexOutputFormat.class, vertexOutputFormatClass);
      if (classList.get(ID_PARAM_INDEX) == null) {
        LOG.warn("Output format vertex index type is not known");
      } else if (!vertexIndexType.equals(classList.get(ID_PARAM_INDEX))) {
        throw new IllegalArgumentException(
          "checkClassTypes: Vertex index types don't match, " +
            "vertex - " + vertexIndexType +
            ", vertex output format - " + classList.get(ID_PARAM_INDEX));
      }
      if (classList.get(VALUE_PARAM_INDEX) == null) {
        LOG.warn("Output format vertex value type is not known");
      } else if (!vertexValueType.equals(classList.get(VALUE_PARAM_INDEX))) {
        throw new IllegalArgumentException(
          "checkClassTypes: Vertex value types don't match, " +
            "vertex - " + vertexValueType +
            ", vertex output format - " + classList.get(VALUE_PARAM_INDEX));
      }
      if (classList.get(EDGE_PARAM_INDEX) == null) {
        LOG.warn("Output format edge value type is not known");
      } else if (!edgeValueType.equals(classList.get(EDGE_PARAM_INDEX))) {
        throw new IllegalArgumentException(
          "checkClassTypes: Edge value types don't match, " +
            "vertex - " + edgeValueType +
            ", vertex output format - " + classList.get(EDGE_PARAM_INDEX));
      }
    }
  }

  /** Verify that the vertex value factory's type matches the job */
  private void verifyVertexValueFactoryGenericTypes() {
    Class<? extends VertexValueFactory<V>>
        vvfClass = conf.getVertexValueFactoryClass();
    if (DefaultVertexValueFactory.class.equals(vvfClass)) {
      return;
    }
    List<Class<?>> classList = ReflectionUtils.getTypeArguments(
        VertexValueFactory.class, vvfClass);
    if (classList.get(VALUE_PARAM_VERTEX_VALUE_FACTORY_INDEX) != null &&
        !vertexValueType.equals(
            classList.get(VALUE_PARAM_VERTEX_VALUE_FACTORY_INDEX))) {
      throw new IllegalArgumentException(
          "checkClassTypes: Vertex value types don't match, " +
              "vertex - " + vertexValueType +
              ", vertex value factory - " +
              classList.get(VALUE_PARAM_VERTEX_VALUE_FACTORY_INDEX));
    }
  }

  /** If there is a vertex resolver,
   * validate the generic parameter types. */
  private void verifyVertexResolverGenericTypes() {
    Class<? extends VertexResolver<I, V, E, M>>
      vrClass = conf.getVertexResolverClass();
    if (!DefaultVertexResolver.class.isAssignableFrom(vrClass)) {
      return;
    }
    Class<? extends DefaultVertexResolver<I, V, E, M>>
      dvrClass =
        (Class<? extends DefaultVertexResolver<I, V, E, M>>) vrClass;
    List<Class<?>> classList =
      ReflectionUtils.getTypeArguments(
          DefaultVertexResolver.class, dvrClass);
    if (classList.get(ID_PARAM_INDEX) != null &&
      !vertexIndexType.equals(classList.get(ID_PARAM_INDEX))) {
      throw new IllegalArgumentException(
        "checkClassTypes: Vertex index types don't match, " +
          "vertex - " + vertexIndexType +
          ", vertex resolver - " + classList.get(ID_PARAM_INDEX));
    }
    if (classList.get(VALUE_PARAM_INDEX) != null &&
      !vertexValueType.equals(classList.get(VALUE_PARAM_INDEX))) {
      throw new IllegalArgumentException(
        "checkClassTypes: Vertex value types don't match, " +
          "vertex - " + vertexValueType +
          ", vertex resolver - " + classList.get(VALUE_PARAM_INDEX));
    }
    if (classList.get(EDGE_PARAM_INDEX) != null &&
      !edgeValueType.equals(classList.get(EDGE_PARAM_INDEX))) {
      throw new IllegalArgumentException(
        "checkClassTypes: Edge value types don't match, " +
          "vertex - " + edgeValueType +
          ", vertex resolver - " + classList.get(EDGE_PARAM_INDEX));
    }
    if (classList.get(MSG_PARAM_INDEX) != null &&
      !messageValueType.equals(classList.get(MSG_PARAM_INDEX))) {
      throw new IllegalArgumentException(
        "checkClassTypes: Message value types don't match, " +
          "vertex - " + messageValueType +
          ", vertex resolver - " + classList.get(MSG_PARAM_INDEX));
    }
  }
}

