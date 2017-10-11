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
package org.apache.giraph.writable.kryo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.giraph.utils.UnsafeReusableByteArrayInput;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Generic wrapper object, making any object writable.
 *
 * Uses Kryo inside for serialization.
 * Current configuration is not optimized for performance,
 * but Writable interface doesn't allow much room for it.
 *
 * Note - Java8 lambdas need to implement Serializable to work.
 *
 * @param <T> Object type
 */
public class KryoWritableWrapper<T> implements Writable {
  /** Class logger */
  private static final Logger LOG =
          Logger.getLogger(KryoWritableWrapper.class);
  /** Wrapped object */
  private T object;

  /**
   * Create wrapper given an object.
   * @param object Object instance
   */
  public KryoWritableWrapper(T object) {
    this.object = object;
  }

  /**
   * Creates wrapper initialized with null.
   */
  public KryoWritableWrapper() {
  }

  /**
   * Unwrap the object value
   * @return Object value
   */
  public T get() {
    return object;
  }

  /**
   * Set wrapped object value
   * @param object New object value
   */
  public void set(T object) {
    this.object = object;
  }

  @Override
  public void readFields(DataInput in) throws java.io.IOException {
    object = HadoopKryo.readClassAndObject(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    HadoopKryo.writeClassAndObject(out, object);
  }

  /**
   * Returns Writable instance, wrapping given object only
   * if it is not already writable.
   *
   * @param object Object to potentially wrap
   * @return Writable object holding argument
   */
  public static Writable wrapIfNeeded(Object object) {
    if (object instanceof Writable) {
      return (Writable) object;
    } else {
      return new KryoWritableWrapper<>(object);
    }
  }

  /**
   * Unwrap Writable object if it was wrapped initially,
   * inverse of wrapIfNeeded function.
   * @param value Potentially wrapped value
   * @return Original unwrapped value
   * @param <T> Type of returned object.
   */
  public static <T> T unwrapIfNeeded(Writable value) {
    if (value instanceof KryoWritableWrapper) {
      return ((KryoWritableWrapper<T>) value).get();
    } else {
      return (T) value;
    }
  }

  /**
   * Wrap object with KryoWritableWrapper, create a writable copy of it,
   * and then unwrap it, allowing any object to be copied.
   *
   * @param object Object to copy
   * @return copy of the object
   * @param <T> Type of the object
   */
  public static <T> T wrapAndCopy(T object) {
    return WritableUtils.createCopy(new KryoWritableWrapper<>(object)).get();
  }

  /**
   * Try converting the object to byte array.
   * @param object Object
   * @param <T> Type
   * @return byte array
   */
  public static <T> byte [] tryConvertToByteArray(T object) {
    byte [] arr = null;
    try {
      KryoWritableWrapper<T> wrapper =
              new KryoWritableWrapper<>(object);
      arr = WritableUtils.toByteArrayUnsafe(wrapper);
      // Checkstyle exception due to unsafe conversion
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.error("Failed to convert to byte array: " +
              ExceptionUtils.getStackTrace(e));
    }
    return arr;
  }

  /**
   * Try converting from byte array
   * @param arr byte array
   * @param <T> type
   * @return original object
   */
  public static <T> T tryConvertFromByteArray(byte [] arr) {
    T result = null;
    try {
      KryoWritableWrapper<T> wrapper =
              new KryoWritableWrapper<>();
      WritableUtils.fromByteArrayUnsafe(
              arr, wrapper, new UnsafeReusableByteArrayInput());
      result = wrapper.get();
      // Checkstyle exception due to unsafe conversion
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      LOG.error("Failed to convert from byte array: " +
              ExceptionUtils.getStackTrace(e));
    }
    return result;
  }
}
