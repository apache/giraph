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

package org.apache.giraph.io.formats.multi;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Description of the vertex input format - holds vertex input format class and
 * all parameters specifically set for that vertex input format.
 *
 * Used only with {@link MultiVertexInputFormat}
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class VertexInputFormatDescription<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends InputFormatDescription<VertexInputFormat<I, V, E>> {
  /**
   * VertexInputFormats description - JSON array containing a JSON array for
   * each vertex input. Vertex input JSON arrays contain one or two elements -
   * first one is the name of vertex input class, and second one is JSON object
   * with all specific parameters for this vertex input. For example:
   * [["VIF1",{"p":"v1"}],["VIF2",{"p":"v2","q":"v"}]]
   */
  public static final StrConfOption VERTEX_INPUT_FORMAT_DESCRIPTIONS =
      new StrConfOption("giraph.multiVertexInput.descriptions", null,
          "VertexInputFormats description - JSON array containing a JSON " +
          "array for each vertex input. Vertex input JSON arrays contain " +
          "one or two elements - first one is the name of vertex input " +
          "class, and second one is JSON object with all specific parameters " +
          "for this vertex input. For example: [[\"VIF1\",{\"p\":\"v1\"}]," +
          "[\"VIF2\",{\"p\":\"v2\",\"q\":\"v\"}]]\"");

  /**
   * Constructor with vertex input format class
   *
   * @param vertexInputFormatClass Vertex input format class
   */
  public VertexInputFormatDescription(
      Class<? extends VertexInputFormat<I, V, E>> vertexInputFormatClass) {
    super(vertexInputFormatClass);
  }

  /**
   * Constructor with json string describing this input format
   *
   * @param description Json string describing this input format
   */
  public VertexInputFormatDescription(String description) {
    super(description);
  }

  /**
   * Create a copy of configuration which additionally has all parameters for
   * this input format set
   *
   * @param conf Configuration which we want to create a copy from
   * @return Copy of configuration
   */
  private ImmutableClassesGiraphConfiguration<I, V, E>
  createConfigurationCopy(
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    ImmutableClassesGiraphConfiguration<I, V, E> confCopy =
        new ImmutableClassesGiraphConfiguration<I, V, E>(conf);
    confCopy.setVertexInputFormatClass(getInputFormatClass());
    putParametersToConfiguration(confCopy);
    return confCopy;
  }

  /**
   * Get descriptions of vertex input formats from configuration.
   *
   * @param conf Configuration
   * @param <I>  Vertex id
   * @param <V>  Vertex data
   * @param <E>  Edge data
   * @return List of vertex input format descriptions
   */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable>
  List<VertexInputFormatDescription<I, V, E>> getVertexInputFormatDescriptions(
      Configuration conf) {
    String vertexInputFormatDescriptions =
        VERTEX_INPUT_FORMAT_DESCRIPTIONS.get(conf);
    if (vertexInputFormatDescriptions == null) {
      return Lists.newArrayList();
    }
    try {
      JSONArray inputFormatsJson = new JSONArray(vertexInputFormatDescriptions);
      List<VertexInputFormatDescription<I, V, E>> descriptions =
          Lists.newArrayListWithCapacity(inputFormatsJson.length());
      for (int i = 0; i < inputFormatsJson.length(); i++) {
        descriptions.add(new VertexInputFormatDescription<I, V, E>(
            inputFormatsJson.getJSONArray(i).toString()));
      }
      return descriptions;
    } catch (JSONException e) {
      throw new IllegalStateException("getVertexInputFormatDescriptions: " +
          "JSONException occurred while trying to process " +
          vertexInputFormatDescriptions, e);
    }
  }

  /**
   * Create all vertex input formats
   *
   * @param conf Configuration
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @return List with all vertex input formats
   */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable>
  List<VertexInputFormat<I, V, E>> createVertexInputFormats(
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    List<VertexInputFormatDescription<I, V, E>> descriptions =
        getVertexInputFormatDescriptions(conf);
    List<VertexInputFormat<I, V, E>> vertexInputFormats =
        Lists.newArrayListWithCapacity(descriptions.size());
    for (VertexInputFormatDescription<I, V, E> description : descriptions) {
      ImmutableClassesGiraphConfiguration<I, V, E> confCopy =
          description.createConfigurationCopy(conf);
      vertexInputFormats.add(confCopy.createWrappedVertexInputFormat());
    }
    return vertexInputFormats;
  }
}
