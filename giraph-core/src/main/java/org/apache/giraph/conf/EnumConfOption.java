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

import com.google.common.base.Objects;

/**
 * Enum Configuration option
 *
 * @param <T> Enum class
 */
public class EnumConfOption<T extends Enum<T>> extends AbstractConfOption {
  /** Enum class */
  private final Class<T> klass;
  /** Default value */
  private final T defaultValue;

  /**
   * Constructor
   *
   * @param key Configuration key
   * @param klass Enum class
   * @param defaultValue default value
   * @param description description of the option
   */
  public EnumConfOption(String key, Class<T> klass, T defaultValue,
      String description) {
    super(key, description);
    this.klass = klass;
    this.defaultValue = defaultValue;
  }

  /**
   * Create new EnumConfOption
   *
   * @param key String configuration key
   * @param klass enum class
   * @param defaultValue default enum value
   * @param description description of the option
   * @param <X> enum type
   * @return EnumConfOption
   */
  public static <X extends Enum<X>> EnumConfOption<X>
  create(String key, Class<X> klass, X defaultValue, String description) {
    return new EnumConfOption<X>(key, klass, defaultValue, description);
  }

  @Override public boolean isDefaultValue(Configuration conf) {
    return Objects.equal(get(conf), defaultValue);
  }

  @Override public String getDefaultValueStr() {
    return defaultValue.name();
  }

  @Override public ConfOptionType getType() {
    return ConfOptionType.ENUM;
  }

  /**
   * Lookup value
   *
   * @param conf Configuration
   * @return enum value
   */
  public T get(Configuration conf) {
    String valueStr = conf.get(getKey(), getDefaultValueStr());
    return T.valueOf(klass, valueStr);
  }

  /**
   * Set value
   *
   * @param conf Configuration
   * @param value to set
   */
  public void set(Configuration conf, Enum<T> value) {
    conf.set(getKey(), value.name());
  }

  /**
   * Set value if it's not already present
   *
   * @param conf Configuration
   * @param value to set
   */
  public void setIfUnset(Configuration conf, Enum<T> value) {
    if (!contains(conf)) {
      set(conf, value);
    }
  }
}
