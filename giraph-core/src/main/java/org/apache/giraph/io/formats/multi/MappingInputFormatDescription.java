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
import org.apache.giraph.io.MappingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Description of the mapping input format - holds mapping input format class
 * and all parameters specifically set for that mapping input format.
 *
 * Used only with {@link MultiMappingInputFormat}
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <B> Mapping target
 */
public class MappingInputFormatDescription<I extends WritableComparable,
    V extends Writable, E extends Writable, B extends Writable>
    extends InputFormatDescription<MappingInputFormat<I, V, E, B>> {
  /**
   * MappingInputFormats description - JSON array containing a JSON array for
   * each mapping input. Mapping input JSON arrays contain one or two elements -
   * first one is the name of mapping input class, and second one is JSON object
   * with all specific parameters for this mapping input. For example:
   * [["VIF1",{"p":"v1"}],["VIF2",{"p":"v2","q":"v"}]]
   */
  public static final StrConfOption MAPPING_INPUT_FORMAT_DESCRIPTIONS =
      new StrConfOption("giraph.multiMappingInput.descriptions", null,
          "MappingInputFormats description - JSON array containing a JSON " +
          "array for each mapping input. Mapping input JSON arrays contain " +
          "one or two elements - first one is the name of mapping input " +
          "class, and second one is JSON object with all specific parameters " +
          "for this mapping input. For example: [[\"VIF1\",{\"p\":\"v1\"}]," +
          "[\"VIF2\",{\"p\":\"v2\",\"q\":\"v\"}]]\"");

  /**
   * Constructor with mapping input format class
   *
   * @param mappingInputFormatClass Mapping input format class
   */
  public MappingInputFormatDescription(
    Class<? extends MappingInputFormat<I, V, E, B>> mappingInputFormatClass
  ) {
    super(mappingInputFormatClass);
  }

  /**
   * Constructor with json string describing this input format
   *
   * @param description Json string describing this input format
   */
  public MappingInputFormatDescription(String description) {
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
    confCopy.setMappingInputFormatClass(getInputFormatClass());
    putParametersToConfiguration(confCopy);
    return confCopy;
  }

  /**
   * Get descriptions of mapping input formats from configuration.
   *
   * @param conf Configuration
   * @param <I>  Vertex id
   * @param <V>  Vertex data
   * @param <E>  Edge data
   * @param <B>  Mapping target
   * @return List of mapping input format descriptions
   */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable, B extends Writable>
  List<MappingInputFormatDescription<I, V, E, B>>
  getMappingInputFormatDescriptions(Configuration conf) {
    String mappingInputFormatDescriptions =
        MAPPING_INPUT_FORMAT_DESCRIPTIONS.get(conf);
    if (mappingInputFormatDescriptions == null) {
      return Lists.newArrayList();
    }
    try {
      JSONArray inputFormatsJson = new JSONArray(
        mappingInputFormatDescriptions);
      List<MappingInputFormatDescription<I, V, E, B>> descriptions =
          Lists.newArrayListWithCapacity(inputFormatsJson.length());
      for (int i = 0; i < inputFormatsJson.length(); i++) {
        descriptions.add(new MappingInputFormatDescription<I, V, E, B>(
            inputFormatsJson.getJSONArray(i).toString()));
      }
      return descriptions;
    } catch (JSONException e) {
      throw new IllegalStateException("getMappingInputFormatDescriptions: " +
          "JSONException occurred while trying to process " +
          mappingInputFormatDescriptions, e);
    }
  }

  /**
   * Create all mapping input formats
   *
   * @param conf Configuration
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <B> Mapping target data
   * @return List with all mapping input formats
   */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable, B extends Writable>
  List<MappingInputFormat<I, V, E, B>> createMappingInputFormats(
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    List<MappingInputFormatDescription<I, V, E, B>> descriptions =
        getMappingInputFormatDescriptions(conf);
    List<MappingInputFormat<I, V, E, B>> mappingInputFormats =
        Lists.newArrayListWithCapacity(descriptions.size());
    for (MappingInputFormatDescription<I, V, E, B> description : descriptions) {
      ImmutableClassesGiraphConfiguration<I, V, E> confCopy =
          description.createConfigurationCopy(conf);
      mappingInputFormats.add((MappingInputFormat<I, V, E, B>)
                                confCopy.createWrappedMappingInputFormat());
    }
    return mappingInputFormats;
  }
}
