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
package org.apache.giraph.debugger.mock;

import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;

/**
 * Base class for {@link TestGenerator} and {@link TestGraphGenerator}.
 */
class VelocityBasedGenerator {

  /**
   * Default constructor.
   */
  protected VelocityBasedGenerator() {
    Velocity.setProperty(RuntimeConstants.RESOURCE_LOADER, "class");
    Velocity.setProperty(
      "class." + RuntimeConstants.RESOURCE_LOADER + ".class",
      PrefixedClasspathResourceLoader.class.getName());
    Velocity.setProperty("class." + RuntimeConstants.RESOURCE_LOADER +
      ".prefix", "/" +
      getClass().getPackage().getName().replaceAll("\\.", "/") + "/");
    Velocity.init();
  }

}
