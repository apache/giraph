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

import org.apache.giraph.io.GiraphInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;

/**
 * Description of the input format - holds input format class and all
 * parameters specifically set for that input format.
 *
 * Used only with input formats which wrap several input formats into one
 * ({@link MultiVertexInputFormat} and {@link MultiEdgeInputFormat})
 *
 * @param <IF> Input format type
 */
public abstract class InputFormatDescription<IF extends GiraphInputFormat> {
  /** Input format class */
  private Class<? extends IF> inputFormatClass;
  /** Parameters set specifically for this input format */
  private final Map<String, String> parameters = Maps.newHashMap();

  /**
   * Constructor with input format class
   *
   * @param inputFormatClass Input format class
   */
  public InputFormatDescription(Class<? extends IF> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
  }

  /**
   * Constructor with json string describing this input format
   *
   * @param description Json string describing this input format
   */
  @SuppressWarnings("unchecked")
  public InputFormatDescription(String description) {
    try {
      JSONArray jsonArray = new JSONArray(description);
      inputFormatClass =
          (Class<? extends IF>) Class.forName(jsonArray.getString(0));
      if (jsonArray.length() > 1) {
        addParameters(jsonArray.getJSONObject(1));
      }
    } catch (JSONException e) {
      throw new IllegalStateException(
          "Failed to parse JSON " + description, e);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Couldn't find " +
          "input format class from description " + description, e);
    }
  }

  /**
   * Add parameter to this input format description
   *
   * @param name  Parameter name
   * @param value Parameter value
   */
  public void addParameter(String name, String value) {
    parameters.put(name, value);
  }

  /**
   * Add all parameters from json object
   *
   * @param parametersJson Json object to read parameters from
   */
  public void addParameters(JSONObject parametersJson) {
    Iterator<?> keys = parametersJson.keys();
    while (keys.hasNext()) {
      String key = (String) keys.next();
      try {
        addParameter(key, parametersJson.getString(key));
      } catch (JSONException e) {
        throw new IllegalStateException("addParameters: Failed to parse " +
            parametersJson, e);
      }
    }
  }

  /**
   * Convert input format description to json array
   *
   * @return Json array representing this input format description
   */
  public JSONArray toJsonArray() {
    JSONArray jsonArray = new JSONArray();
    jsonArray.put(inputFormatClass.getName());
    JSONObject jsonParameters = new JSONObject();
    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      try {
        jsonParameters.put(entry.getKey(), entry.getValue());
      } catch (JSONException e) {
        throw new IllegalStateException("toJsonArray: JSONException occurred " +
            "while trying to process (" + entry.getKey() + ", " +
            entry.getValue() + ")", e);
      }
    }
    jsonArray.put(jsonParameters);
    return jsonArray;
  }

  public Class<? extends IF> getInputFormatClass() {
    return inputFormatClass;
  }

  public void setInputFormatClass(Class<? extends IF> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
  }

  /**
   * Put parameters from this input format description to configuration
   *
   * @param conf Configuration to put parameters to
   */
  public void putParametersToConfiguration(Configuration conf) {
    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public String toString() {
    return toJsonArray().toString();
  }

  /**
   * Create JSON string for the InputFormatDescriptions
   *
   * @param descriptions InputFormatDescriptions
   * @return JSON string describing these InputFormatDescriptions
   */
  public static String toJsonString(
      Iterable<? extends InputFormatDescription> descriptions) {
    JSONArray jsonArray = new JSONArray();
    for (InputFormatDescription description : descriptions) {
      jsonArray.put(description.toJsonArray());
    }
    return jsonArray.toString();
  }
}
