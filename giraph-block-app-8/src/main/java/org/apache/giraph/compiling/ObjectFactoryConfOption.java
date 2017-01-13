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
package org.apache.giraph.compiling;

import java.lang.reflect.Type;

import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.function.Supplier;
import org.apache.hadoop.conf.Configuration;

import com.google.common.reflect.TypeToken;

/**
 * Option in configuration which can create objects
 *
 * @param <T> Type of objects for this option
 */
public class ObjectFactoryConfOption<T> {
  /** Conf option which holds code snippet for creating objects */
  private final StrConfOption codeSnippetConfOption;
  /** Base interface for class */
  private final Type interfaceType;

  public ObjectFactoryConfOption(
      String key, Class<T> interfaceClass, String defaultValue,
      String description) {
    codeSnippetConfOption = new StrConfOption(key, defaultValue, description);
    this.interfaceType = interfaceClass;
  }

  public ObjectFactoryConfOption(
      String key, TypeToken<T> interfaceTypeToken, String defaultValue,
      String description) {
    codeSnippetConfOption = new StrConfOption(key, defaultValue, description);
    this.interfaceType = interfaceTypeToken.getType();
  }

  /**
   * Create object from configuration, or return null if conf option is empty.
   *
   * Return objects are NonKryoWritable.
   *
   * @param conf Configuration
   * @return New object, or null if conf option is empty
   */
  public final T createObject(Configuration conf) {
    final String codeSnippet = createFullCodeSnippet(
        codeSnippetConfOption.get(conf));
    if (codeSnippet == null || codeSnippet.isEmpty()) {
      return null;
    } else {
      return RuntimeObjectFactoryGenerator.<T>createFactory(
          codeSnippet, interfaceType).create();
    }
  }

  /**
   * Take user passed codeSnippet and create full code snippet to be given to
   * RuntimeObjectFactoryGenerator.
   *
   * Subclasses can override this method to customize it's logic.
   */
  protected String createFullCodeSnippet(String codeSnippet) {
    return codeSnippet;
  }

  /**
   * Create object cached supplier from configuration, which stores snippet for
   * serializaition, working around objects themselves being NonKryoWritable.
   *
   * Object is created only once per new or deserialized instance of returned
   * supplier.
   */
  public final Supplier<T> createSupplier(Configuration conf) {
    final String codeSnippet = createFullCodeSnippet(
        codeSnippetConfOption.get(conf));
    final Type type = this.interfaceType;
    if (codeSnippet == null || codeSnippet.isEmpty()) {
      return null;
    }
    return createNewSupplier(codeSnippet, type);
  }

  private static <T> Supplier<T> createNewSupplier(
      final String codeSnippet, final Type type) {
    return RuntimeObjectFactoryGenerator.createCachedSupplier(
        codeSnippet, type);
  }

  public void setCodeSnippet(Configuration conf, String codeSnippet) {
    codeSnippetConfOption.set(conf, codeSnippet);
  }

  public void setCodeSnippetIfUnset(Configuration conf, String codeSnippet) {
    codeSnippetConfOption.setIfUnset(conf, codeSnippet);
  }

  public boolean isDefaultValue(Configuration conf) {
    return codeSnippetConfOption.isDefaultValue(conf);
  }

  public boolean isEmpty(Configuration conf) {
    final String codeSnippet = createFullCodeSnippet(
        codeSnippetConfOption.get(conf));
    return codeSnippet == null || codeSnippet.isEmpty();
  }

  public String getCodeSnippet(Configuration conf) {
    return codeSnippetConfOption.get(conf);
  }

  public Type getInterfaceType() {
    return interfaceType;
  }

  public String getInterfaceName() {
    return FactoryCodeGenerator.getSimpleName(interfaceType);
  }
}
