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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.giraph.utils.Factory;

import com.google.common.reflect.TypeToken;

/**
 * Class which creates object factories from code snippet to create and return
 * object
 */
public class FactoryCodeGenerator implements ClassCodeGenerator {
  private final Type objectType;

  public FactoryCodeGenerator(Type objectType) {
    this.objectType = objectType;
  }

  @Override
  public String generateCode(
      String packageName, String className, String creationCode) {
    StringBuilder src = new StringBuilder();
    src.append("package ").append(packageName).append(";\n");
    // Import Factory
    src.append("import ").append(Factory.class.getName()).append(";\n");

    appendTypeImport(src, objectType);
    String simpleName = getSimpleName(objectType);

    // Allow classes from java.util to be used
    src.append("import java.util.*;\n");
    // Object compiled in one VM cannot be deserialized in other,
    // so mark generated class as NonKryoWritable
    src.append(
        "import org.apache.giraph.writable.kryo.markers.NonKryoWritable;\n");
    src.append("public class ").append(className).append(
        " implements ").append(Factory.class.getSimpleName()).append(
            "<").append(simpleName).append(">, NonKryoWritable {\n");
    src.append("public ").append(simpleName).append(" create() {\n");
    src.append(creationCode).append("\n").append("}\n").append("}\n");
    return src.toString();
  }

  public static String getSimpleName(TypeToken<?> typeToken) {
    return getSimpleName(typeToken.getType());
  }

  public static String getSimpleName(Type type) {
    return (type instanceof Class) ?
        ((Class<?>) type).getCanonicalName() :
        type.toString();
  }

  private static void appendTypeImport(StringBuilder src, Type objectType) {
    if (objectType == null) {
      return;
    }

    if (objectType instanceof Class) {
      appendClassImport(src, (Class<?>) objectType);
    } else if (objectType instanceof ParameterizedType) {
      appendTypeImport(src, ((ParameterizedType) objectType).getRawType());
      appendTypeImport(src, ((ParameterizedType) objectType).getOwnerType());

      for (Type innerType :
            ((ParameterizedType) objectType).getActualTypeArguments()) {
        appendTypeImport(src, innerType);
      }
    } else {
      throw new IllegalArgumentException(
          "Unsupported " + objectType + " of type " + objectType.getClass());
    }
  }

  private static void appendClassImport(
      StringBuilder src, Class<?> objectClass) {
    // Allow classes from the same package to be used
    src.append("import ").append(
        objectClass.getPackage().getName()).append(".*;\n");
    // If objectClass is inner class then above import is not enough
    src.append("import ").append(objectClass.getCanonicalName()).append(";\n");
  }

  @Override
  public int hashCode() {
    return objectType.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof FactoryCodeGenerator)) {
      return false;
    }
    return objectType.equals(((FactoryCodeGenerator) obj).objectType);
  }
}
