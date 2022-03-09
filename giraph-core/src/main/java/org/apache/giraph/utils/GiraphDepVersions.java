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
package org.apache.giraph.utils;

import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

/**
 * Versions of Giraph dependencies. This pulls version information from a known
 * properties file. That file is created by maven when building the jar and
 * populated with versions from the pom. We put the properties file in a known
 * resources folder so that it is easy to pick up.
 *
 * See http://bit.ly/19LQyrK for more information.
 */
public class GiraphDepVersions {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(GiraphDepVersions.class);

  /** Path to resource */
  private static final String RESOURCE_NAME =
      "org/apache/giraph/versions.properties";

  /** Singleton */
  private static final GiraphDepVersions INSTANCE = new GiraphDepVersions();

  /** The properties read */
  private final Properties properties;

  /** Constructor */
  private GiraphDepVersions() {
    URL url = Resources.getResource(RESOURCE_NAME);
    properties = new Properties();
    try {
      properties.load(url.openStream());
    } catch (IOException e) {
      LOG.error("Could not read giraph versions from file " + RESOURCE_NAME);
    }
  }

  /**
   * Get singleton instance
   *
   * @return singleton
   */
  public static GiraphDepVersions get() {
    return INSTANCE;
  }

  public Properties getProperties() {
    return properties;
  }

  /**
   * Get version of the named dependency, or null if not found
   *
   * @param name dependency name
   * @return version, or null
   */
  public String versionOf(String name) {
    return properties.getProperty(name);
  }

  /** Log the dependency versions we're using */
  public void logVersionsUsed() {
    Map<String, String> sortedVersions = Maps.newTreeMap();
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      sortedVersions.put(entry.getKey().toString(),
          entry.getValue().toString());
    }
    StringBuilder sb = new StringBuilder(sortedVersions.size() * 20);
    for (Map.Entry<String, String> entry : sortedVersions.entrySet()) {
      sb.append("  ").append(entry.getKey()).append(": ").
          append(entry.getValue()).append("\n");
    }
    LOG.info("Versions of Giraph dependencies =>\n" + sb);
  }
}
