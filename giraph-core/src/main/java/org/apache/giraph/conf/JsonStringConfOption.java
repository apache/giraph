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
package org.apache.giraph.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;

/**
 * JSON String configuration option
 */
public class JsonStringConfOption extends AbstractConfOption {
  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(JsonStringConfOption.class);

  /**
   * Constructor
   *
   * @param key String key name
   * @param description String description of option
   */
  public JsonStringConfOption(String key, String description) {
    super(key, description);
  }

  /**
   * Set JSON value
   *
   * @param conf Configuration
   * @param value Json value
   */
  public void set(Configuration conf, Object value) {
    ObjectMapper mapper = new ObjectMapper();
    String jsonStr;
    try {
      jsonStr = mapper.writeValueAsString(value);
      conf.set(getKey(), jsonStr);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to set " + getKey() +
          " with json value from " + value);
    }
  }

  /**
   * Get raw JSON string
   *
   * @param conf Configuration
   * @return raw JSON string value
   */
  public String getRaw(Configuration conf) {
    return conf.get(getKey());
  }

  /**
   * Get JSON value
   *
   * @param <T> JSON type
   * @param conf Configuration
   * @param klass Class to read into
   * @return JSON value
   */
  public <T> T get(Configuration conf, Class<T> klass) {
    String jsonStr = getRaw(conf);
    T value = null;
    if (jsonStr != null) {
      ObjectMapper mapper = new ObjectMapper();
      try {
        value = mapper.readValue(jsonStr, klass);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to read json from key " +
            getKey() + " with class " + klass);
      }
    }
    return value;
  }

  /**
   * Get JSON value
   *
   * @param <T> JSON type
   * @param conf Configuration
   * @param typeReference TypeReference for JSON type
   * @return JSON value
   */
  public <T> T get(Configuration conf, TypeReference<T> typeReference) {
    String jsonStr = getRaw(conf);
    T value = null;
    if (jsonStr != null) {
      ObjectMapper mapper = new ObjectMapper();
      try {
        value = mapper.<T>readValue(jsonStr, typeReference);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to read json from key " +
            getKey() + " with class " + typeReference);
      }
    }
    return value;
  }

  /**
   * Get JSON value, or default if not present
   *
   * @param <T> JSON type
   * @param klass Class to read into
   * @param conf Configuration
   * @param defaultValue Default value if not found
   * @return JSON value
   */
  public <T> T getWithDefault(Configuration conf, Class<T> klass,
      T defaultValue) {
    if (contains(conf)) {
      return get(conf, klass);
    } else {
      return defaultValue;
    }
  }

  @Override
  public String getDefaultValueStr() {
    return "null";
  }

  @Override
  public boolean isDefaultValue(Configuration conf) {
    return !contains(conf);
  }

  @Override
  public ConfOptionType getType() {
    return ConfOptionType.STRING;
  }
}
