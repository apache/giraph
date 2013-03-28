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

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static org.apache.giraph.conf.GiraphConstants.VERTEX_CLASS;

/**
 * Tracks all of the Giraph options
 */
public class AllOptions {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(AllOptions.class);

  /** Configuration options */
  private static final List<AbstractConfOption> CONF_OPTIONS =
      Lists.newArrayList();

  /** Don't construct */
  private AllOptions() { }

  /**
   * Add an option. Subclasses of {@link AbstractConfOption} should call this
   * at the end of their constructor.
   * @param confOption option
   */
  public static void add(AbstractConfOption confOption) {
    CONF_OPTIONS.add(confOption);
  }

  /**
   * String representation of all of the options stored
   * @return string
   */
  public static String allOptionsString() {
    Collections.sort(CONF_OPTIONS);
    StringBuilder sb = new StringBuilder(CONF_OPTIONS.size() * 30);
    sb.append("All Options:\n");
    ConfOptionType lastType = null;
    for (AbstractConfOption confOption : CONF_OPTIONS) {
      if (!confOption.getType().equals(lastType)) {
        sb.append(confOption.getType().toString().toLowerCase()).append(":\n");
        lastType = confOption.getType();
      }
      sb.append(confOption);
    }
    return sb.toString();
  }

  /**
   * Command line utility to dump all Giraph options
   * @param args cmdline args
   */
  public static void main(String[] args) {
    // This is necessary to trigger the static constants in GiraphConstants to
    // get loaded. Without it we get no output.
    VERTEX_CLASS.toString();

    LOG.info(allOptionsString());
  }
}
