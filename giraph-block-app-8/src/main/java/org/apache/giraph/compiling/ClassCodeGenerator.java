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

/**
 * Interface which can generate code from code snippet
 */
public interface ClassCodeGenerator {
  /**
   * Generate code for creating a class inside of specified package with given
   * class name, using provided code snippet
   *
   * @param packageName Package to create class in
   * @param className Name of the class
   * @param codeSnippet Cod snippet for this class
   * @return Code used to generate the class
   */
  String generateCode(
      String packageName, String className, String codeSnippet);
}
