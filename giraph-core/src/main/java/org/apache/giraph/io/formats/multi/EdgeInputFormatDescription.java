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
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Description of the edge input format - holds edge input format class and all
 * parameters specifically set for that edge input format.
 *
 * Used only with {@link MultiEdgeInputFormat}
 *
 * @param <I> Vertex id
 * @param <E> Edge data
 */
public class EdgeInputFormatDescription<I extends WritableComparable,
    E extends Writable> extends InputFormatDescription<EdgeInputFormat<I, E>> {
  /**
   * EdgeInputFormats description - JSON array containing a JSON array for
   * each edge input. Edge input JSON arrays contain one or two elements -
   * first one is the name of edge input class, and second one is JSON object
   * with all specific parameters for this edge input. For example:
   * [["EIF1",{"p":"v1"}],["EIF2",{"p":"v2","q":"v"}]]
   */
  public static final StrConfOption EDGE_INPUT_FORMAT_DESCRIPTIONS =
      new StrConfOption("giraph.multiEdgeInput.descriptions", null,
          "EdgeInputFormats description - JSON array containing a JSON array " +
          "for each edge input. Edge input JSON arrays contain one or two " +
          "elements - first one is the name of edge input class, and second " +
          "one is JSON object with all specific parameters for this edge " +
          "input. For example: [[\"EIF1\",{\"p\":\"v1\"}]," +
          "[\"EIF2\",{\"p\":\"v2\",\"q\":\"v\"}]]");

  /**
   * Constructor with edge input format class
   *
   * @param edgeInputFormatClass Edge input format class
   */
  public EdgeInputFormatDescription(
      Class<? extends EdgeInputFormat<I, E>> edgeInputFormatClass) {
    super(edgeInputFormatClass);
  }

  /**
   * Constructor with json string describing this input format
   *
   * @param description Json string describing this input format
   */
  public EdgeInputFormatDescription(String description) {
    super(description);
  }

  /**
   * Create a copy of configuration which additionally has all parameters for
   * this input format set
   *
   * @param conf Configuration which we want to create a copy from
   * @return Copy of configuration
   */
  private ImmutableClassesGiraphConfiguration<I, Writable, E>
  createConfigurationCopy(
      ImmutableClassesGiraphConfiguration<I, Writable, E> conf) {
    ImmutableClassesGiraphConfiguration<I, Writable, E> confCopy =
        new ImmutableClassesGiraphConfiguration<I, Writable, E>(conf);
    confCopy.setEdgeInputFormatClass(getInputFormatClass());
    putParametersToConfiguration(confCopy);
    return confCopy;
  }

  /**
   * Get descriptions of edge input formats from configuration.
   *
   * @param conf Configuration
   * @param <I>  Vertex id
   * @param <E>  Edge data
   * @return List of edge input format descriptions
   */
  public static <I extends WritableComparable, E extends Writable>
  List<EdgeInputFormatDescription<I, E>> getEdgeInputFormatDescriptions(
      Configuration conf) {
    String edgeInputFormatDescriptions =
        EDGE_INPUT_FORMAT_DESCRIPTIONS.get(conf);
    if (edgeInputFormatDescriptions == null) {
      return Lists.newArrayList();
    }
    try {
      JSONArray inputFormatsJson = new JSONArray(edgeInputFormatDescriptions);
      List<EdgeInputFormatDescription<I, E>> descriptions =
          Lists.newArrayListWithCapacity(inputFormatsJson.length());
      for (int i = 0; i < inputFormatsJson.length(); i++) {
        descriptions.add(new EdgeInputFormatDescription<I, E>(
            inputFormatsJson.getJSONArray(i).toString()));
      }
      return descriptions;
    } catch (JSONException e) {
      throw new IllegalStateException("getEdgeInputFormatDescriptions: " +
          "JSONException occurred while trying to process " +
          edgeInputFormatDescriptions, e);
    }
  }

  /**
   * Create all edge input formats
   *
   * @param conf Configuration
   * @param <I> Vertex id
   * @param <E> Edge data
   * @return List with all edge input formats
   */
  public static <I extends WritableComparable,
      E extends Writable> List<EdgeInputFormat<I, E>> createEdgeInputFormats(
      ImmutableClassesGiraphConfiguration<I, Writable, E> conf) {
    List<EdgeInputFormatDescription<I, E>> descriptions =
        getEdgeInputFormatDescriptions(conf);
    List<EdgeInputFormat<I, E>> edgeInputFormats =
        Lists.newArrayListWithCapacity(descriptions.size());
    for (EdgeInputFormatDescription<I, E> description : descriptions) {
      ImmutableClassesGiraphConfiguration<I, Writable, E> confCopy =
          description.createConfigurationCopy(conf);
      edgeInputFormats.add(confCopy.createWrappedEdgeInputFormat());
    }
    return edgeInputFormats;
  }
}
