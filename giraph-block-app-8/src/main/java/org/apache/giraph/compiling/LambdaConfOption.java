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

import static org.apache.giraph.compiling.FactoryCodeGenerator.getSimpleName;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

/**
 * Conf option which is configured by specifying lambda body, and it returns
 * compiled instance of wanted functional interface with given body.
 * See TestLambdaConfOption for example.
 *
 * @param <T> Functional interface type
 */
public class LambdaConfOption<T> extends ObjectFactoryConfOption<T> {
  private final String[] argumentNames;

  public LambdaConfOption(String key, Class<T> classType, String codeSnippet,
      String description, String... argumentNames) {
    super(key, classType, codeSnippet, description);
    this.argumentNames = argumentNames;
  }

  public LambdaConfOption(
      String key, TypeToken<T> interfaceTypeToken, String codeSnippet,
      String description, String... arguments) {
    super(key, interfaceTypeToken, codeSnippet, description);
    this.argumentNames = arguments;
  }

  @Override
  protected String createFullCodeSnippet(String codeSnippet) {
    if (codeSnippet == null || codeSnippet.isEmpty()) {
      return null;
    }

    return createLambdaCode(codeSnippet, getInterfaceType(), argumentNames);
  }

  public static String createLambdaCode(
      String body, Type type, String[] argumentNames) {
    TypeToken<?> typeToken = TypeToken.of(type);
    Class<?> rawType = typeToken.getRawType();

    if (rawType.isInterface()) {
      return  "return (" +
          Stream.of(argumentNames).collect(Collectors.joining(", ")) +
          ") -> " + body + ";";
    } else {
      // Long workaround for abstract classes
      Method sam = null;

      for (Method m : rawType.getMethods()) {
        if (Modifier.isAbstract(m.getModifiers())) {
          Preconditions.checkState(sam == null);
          sam = m;
        }
      }
      Preconditions.checkState(sam != null);

      Type[] params = sam.getGenericParameterTypes();
      Preconditions.checkState(params.length == argumentNames.length);

      String argList = IntStream.range(0, params.length).mapToObj(
          (i) -> getSimpleName(typeToken.resolveType(params[i])) + " " +
              argumentNames[i]
      ).collect(Collectors.joining(", "));

      return "  return new " + getSimpleName(type) + "() {\n" +
        "    @Override\n" +
        "    public " +
        getSimpleName(typeToken.resolveType(sam.getGenericReturnType())) +
        " " + sam.getName() + "(" + argList + ") {\n" +
        "      return " + body + ";\n" +
        "    }\n" +
        "  };";
    }
  }
}
