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

import org.apache.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 * Abstract base class of configuration options
 */
public abstract class AbstractConfOption
    implements Comparable<AbstractConfOption> {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(AbstractConfOption.class);

  /** Key for configuration */
  private final String key;

  /**
   * Constructor
   * @param key configuration key
   */
  public AbstractConfOption(String key) {
    this.key = key;
  }

  public String getKey() {
    return key;
  }

  @Override public int compareTo(AbstractConfOption o) {
    return ComparisonChain.start()
      .compare(getType(), o.getType())
      .compare(key, o.key)
      .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractConfOption)) {
      return false;
    }

    AbstractConfOption that = (AbstractConfOption) o;
    return Objects.equal(getType(), that.getType()) &&
        Objects.equal(key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key);
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder(30);
    sb.append("  ").append(key).append(" => ").append(getDefaultValueStr());
    sb.append(" (").append(getType().toString().toLowerCase()).append(")\n");
    return sb.toString();
  }

  /**
   * Get string representation of default value
   * @return String
   */
  public abstract String getDefaultValueStr();

  /**
   * Get type this option holds
   * @return ConfOptionType
   */
  public abstract ConfOptionType getType();
}
