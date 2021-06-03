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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import net.openhft.compiler.CachedCompiler;

/**
 * Helper class for creating classes at runtime
 */
public class RuntimeClassGenerator {
  private static final Logger LOG = Logger.getLogger(
      RuntimeClassGenerator.class);

  /**
   * In order not to create multiple classes for exactly same Code generator
   * and String, this keeps all known classes
   */
  private static final
  Map<ClassCodeGenerator, Map<String, Class<?>>> KNOWN_CLASSES =
    new HashMap<>();

  /** Counter for number of created classes */
  private static final
  AtomicInteger CREATED_CLASSES_COUNTER = new AtomicInteger();

  protected RuntimeClassGenerator() {
  }

  public static synchronized <T> Class<T> getOrCreateClass(
      ClassCodeGenerator classCodeGenerator, String codeSnippet) {
    Map<String, Class<?>> knownClassCodeGenerators =
        KNOWN_CLASSES.get(classCodeGenerator);
    if (knownClassCodeGenerators == null) {
      knownClassCodeGenerators = new HashMap<>();
      KNOWN_CLASSES.put(classCodeGenerator, knownClassCodeGenerators);
    } else {
      Class<?> generatedClass = knownClassCodeGenerators.get(codeSnippet);
      if (generatedClass != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Reusing class for " + classCodeGenerator + " " + codeSnippet);
        }
        return (Class<T>) generatedClass;
      }
    }
    String packageName = classCodeGenerator.getClass().getPackage().getName();
    String className = classCodeGenerator.getClass().getSimpleName() +
        CREATED_CLASSES_COUNTER.getAndIncrement();
    String fullClassName = packageName + "." + className;
    String classCode = classCodeGenerator.generateCode(
        packageName, className, codeSnippet);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating " + fullClassName + " with \n" + classCode);
    }
    Class<T> generatedClass = generateClass(fullClassName, classCode);
    knownClassCodeGenerators.put(codeSnippet, generatedClass);
    return generatedClass;
  }

  /**
   * Create class with a name, in the package and with source code.
   */
  public static synchronized <T> Class<T> createClassOnce(
      String className, String packageName, String classCode) {
    return generateClass(packageName + "." + className, classCode);
  }

  private static <T> Class<T> generateClass(
      String fullClassName, String classCode) {
    try {
      Class<T> generatedClass = new CachedCompiler(null, null).loadFromJava(
          fullClassName, classCode);
      // Confirm that class was loaded into default class loader
      Preconditions.checkState(generatedClass == Class.forName(fullClassName));
      return generatedClass;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Couldn't be compile class " + fullClassName + " , with code: " +
          classCode,
          e);
    }
  }

  /**
   * For test
   */
  static int getCreatedClassesCount() {
    return CREATED_CLASSES_COUNTER.get();
  }
}
