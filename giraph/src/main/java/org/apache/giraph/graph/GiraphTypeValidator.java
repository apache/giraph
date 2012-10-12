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

package org.apache.giraph.graph;

import org.apache.giraph.utils.ReflectionUtils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Type;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * GiraphTypeValidator attempts to verify the consistency of
 * user-chosen InputFormat, OutputFormat, and Vertex type
 * parameters before the job run actually begins.
 *
 * @param <I> the Vertex ID type
 * @param <V> the Vertex Value type
 * @param <E> the Edge Value type
 * @param <M> the Message type
 */
public class GiraphTypeValidator<I extends WritableComparable,
  V extends Writable, E extends Writable, M extends Writable> {
  /**
   * Class logger object.
   */
  private static Logger LOG =
    Logger.getLogger(GiraphTypeValidator.class);

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
  private Configuration conf;

  /**
   * Constructor to execute the validation test, throws
   * unchecked exception to end job run on failure.
   *
   * @param conf the Configuration for this run.
   */
  public GiraphTypeValidator(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Make sure that all registered classes have matching types.  This
   * is a little tricky due to type erasure, cannot simply get them from
   * the class type arguments.  Also, set the vertex index, vertex value,
   * edge value and message value classes.
   */
  public void validateClassTypes() {
    Class<? extends Vertex<I, V, E, M>> vertexClass =
      BspUtils.<I, V, E, M>getVertexClass(conf);
    List<Class<?>> classList = ReflectionUtils.<Vertex>getTypeArguments(
      Vertex.class, vertexClass);
    vertexIndexType = classList.get(ID_PARAM_INDEX);
    vertexValueType = classList.get(VALUE_PARAM_INDEX);
    edgeValueType = classList.get(EDGE_PARAM_INDEX);
    messageValueType = classList.get(MSG_PARAM_INDEX);
    verifyVertexInputFormatGenericTypes();
    verifyVertexOutputFormatGenericTypes();
    verifyVertexResolverGenericTypes();
    verifyVertexCombinerGenericTypes();
  }

  /** Verify matching generic types in VertexInputFormat. */
  private void verifyVertexInputFormatGenericTypes() {
    Class<? extends VertexInputFormat<I, V, E, M>> vertexInputFormatClass =
      BspUtils.<I, V, E, M>getVertexInputFormatClass(conf);
    List<Class<?>> classList =
      ReflectionUtils.<VertexInputFormat>getTypeArguments(
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

  /** If there is a combiner type, verify its generic params match the job. */
  private void verifyVertexCombinerGenericTypes() {
    Class<? extends VertexCombiner<I, M>> vertexCombinerClass =
      BspUtils.<I, M>getVertexCombinerClass(conf);
    if (vertexCombinerClass != null) {
      List<Class<?>> classList =
        ReflectionUtils.<VertexCombiner>getTypeArguments(
          VertexCombiner.class, vertexCombinerClass);
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
      vertexOutputFormatClass =
      BspUtils.<I, V, E>getVertexOutputFormatClass(conf);
    if (vertexOutputFormatClass != null) {
      List<Class<?>> classList =
        ReflectionUtils.<VertexOutputFormat>getTypeArguments(
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

  /** If there is a vertex resolver,
   * validate the generic parameter types. */
  private void verifyVertexResolverGenericTypes() {
    Class<? extends VertexResolver<I, V, E, M>>
      vertexResolverClass =
      BspUtils.<I, V, E, M>getVertexResolverClass(conf);
    List<Class<?>> classList =
      ReflectionUtils.<VertexResolver>getTypeArguments(
        VertexResolver.class, vertexResolverClass);
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

