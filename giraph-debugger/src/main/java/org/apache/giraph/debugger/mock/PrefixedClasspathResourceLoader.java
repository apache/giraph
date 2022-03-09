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

import java.io.InputStream;

import org.apache.commons.collections.ExtendedProperties;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

/**
 * @see http://stackoverflow.com/a/9693749/390044
 */
public class PrefixedClasspathResourceLoader extends ClasspathResourceLoader {

  /** Prefix to be added to any names */
  private String prefix = "";

  @Override
  public void init(ExtendedProperties configuration) {
    prefix = configuration.getString("prefix", "");
  }

  @Override
  public InputStream getResourceStream(String name) {
    return super.getResourceStream(prefix + name);
  }
}
