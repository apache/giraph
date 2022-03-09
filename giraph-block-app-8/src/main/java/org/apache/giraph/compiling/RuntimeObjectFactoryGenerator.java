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

import org.apache.giraph.function.Supplier;
import org.apache.giraph.utils.Factory;

/**
 * Helper class for creating object factories at runtime
 */
public class RuntimeObjectFactoryGenerator extends RuntimeClassGenerator {
  private RuntimeObjectFactoryGenerator() {
  }

  public static <T> Factory<T> createFactory(
      String creationCode, Class<T> objectClass) {
    return createFactory(creationCode, (Type) objectClass);
  }

  public static <T> Factory<T> createFactory(
      String creationCode, Type objectType) {
    FactoryCodeGenerator factoryCodeGenerator =
        new FactoryCodeGenerator(objectType);
    Class<Factory<T>> factoryClass = getOrCreateClass(
        factoryCodeGenerator, creationCode);
    try {
      return factoryClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  public static <T> Supplier<T> createCachedSupplier(
      String creationCode, Type objectType) {
    Supplier<T> supplier = new Supplier<T>() {
      private transient T cachedValue;

      @Override
      public T get() {
        if (cachedValue == null) {
          cachedValue = RuntimeObjectFactoryGenerator.<T>createFactory(
              creationCode, objectType).create();
        }
        return cachedValue;
      }
    };
    // Compile in current scope, and fail fast if there are compile errors
    supplier.get();
    return supplier;
  }
}
